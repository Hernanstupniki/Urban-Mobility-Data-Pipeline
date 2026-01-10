"""
OLTP Data Generator – Urban Mobility
-----------------------------------
Purpose:
- Simulate a living OLTP system (real app behavior)
- Generate incremental operational data
- Feed downstream ETL pipelines

IMPORTANT:
- This is NOT ETL
- OLTP constraints must be respected
"""

import os
import random
import logging
from datetime import datetime, timedelta

import psycopg2
from faker import Faker


# ============================================================
# Configuration (env-driven)
# ============================================================

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "dbname": os.getenv("DB_NAME", "mobility_oltp"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
}

# Per-execution volume
N_TRIPS = int(os.getenv("N_TRIPS", 10_000))

# Seed sizes (only if empty)
N_PASSENGERS = int(os.getenv("N_PASSENGERS", 2_000))
N_DRIVERS = int(os.getenv("N_DRIVERS", 500))

BROKEN_RATE = 0.20
LOG_EVERY = 5_000


# ============================================================
# Setup
# ============================================================

fake = Faker()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


def maybe_null(value):
    return None if random.random() < BROKEN_RATE else value


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


# ============================================================
# Helpers
# ============================================================

def fetch_ids(cur, table, id_col):
    cur.execute(f"SELECT {id_col} FROM mobility.{table}")
    return [r[0] for r in cur.fetchall()]


# ============================================================
# Seed functions (run once)
# ============================================================

def seed_passengers(cur):
    logging.info("Seeding passengers...")
    ids = []

    attempts = 0
    inserted = 0

    while inserted < N_PASSENGERS:
        attempts += 1

        cur.execute(
            """
            INSERT INTO mobility.passengers (full_name, email, phone, city)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (email) DO NOTHING
            RETURNING passenger_id
            """,
            (
                fake.name(),
                maybe_null(fake.email()),
                maybe_null(fake.phone_number()),
                fake.city(),
            )
        )

        row = cur.fetchone()
        if row:
            ids.append(row[0])
            inserted += 1

        if attempts % 500 == 0:
            logging.info(f"Passengers inserted: {inserted}/{N_PASSENGERS}")

    return ids


def seed_drivers_and_vehicles(cur):
    logging.info("Seeding drivers and vehicles...")
    driver_ids, vehicle_ids = [], []

    for _ in range(N_DRIVERS):
        cur.execute(
            """
            INSERT INTO mobility.drivers (full_name, license_number)
            VALUES (%s, %s)
            RETURNING driver_id
            """,
            (fake.name(), fake.unique.bothify("LIC-#####")),
        )
        driver_id = cur.fetchone()[0]
        driver_ids.append(driver_id)

        cur.execute(
            """
            INSERT INTO mobility.vehicles (driver_id, plate_number, vehicle_type)
            VALUES (%s, %s, %s)
            RETURNING vehicle_id
            """,
            (
                driver_id,
                fake.unique.license_plate(),
                random.choice(["sedan", "hatchback", "motorbike"]),
            ),
        )
        vehicle_ids.append(cur.fetchone()[0])

    return driver_ids, vehicle_ids


# ============================================================
# Incremental inserts
# ============================================================

def insert_trips(cur, passenger_ids, driver_ids, vehicle_ids, zone_ids):
    logging.info(f"Inserting {N_TRIPS} trips...")
    trip_ids = []

    for i in range(1, N_TRIPS + 1):
        requested_at = datetime.now()
        accepted_at = requested_at + timedelta(minutes=random.randint(1, 10))
        started_at = accepted_at + timedelta(minutes=random.randint(1, 5))
        ended_at = started_at + timedelta(minutes=random.randint(5, 40))

        status = random.choice(
            ["requested", "accepted", "started", "completed", "canceled"]
        )

        if status == "requested":
            accepted_at = started_at = ended_at = None
        elif status == "accepted":
            started_at = ended_at = None
        elif status == "started":
            ended_at = None
        elif status == "canceled":
            ended_at = None

        pickup_zone_id = random.choice(zone_ids)
        dropoff_zone_id = random.choice(zone_ids)

        estimated_distance = round(random.uniform(1, 30), 2)
        raw_actual_distance = round(estimated_distance + random.uniform(-2, 5),2)
        actual_distance = (
            None if raw_actual_distance < 0
            else maybe_null(raw_actual_distance)
        )
        fare_amount = maybe_null(round(random.uniform(5, 80), 2))

        cur.execute(
            """
            INSERT INTO mobility.trips (
                passenger_id,
                driver_id,
                vehicle_id,
                pickup_zone_id,
                dropoff_zone_id,
                status,
                requested_at,
                accepted_at,
                started_at,
                ended_at,
                estimated_distance_km,
                actual_distance_km,
                fare_amount
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING trip_id
            """,
            (
                random.choice(passenger_ids),
                random.choice(driver_ids),
                random.choice(vehicle_ids),
                pickup_zone_id,
                dropoff_zone_id,
                status,
                requested_at,
                accepted_at,
                started_at,
                maybe_null(ended_at),
                estimated_distance,
                actual_distance,
                fare_amount,
            ),
        )

        trip_ids.append(cur.fetchone()[0])

        if i % LOG_EVERY == 0:
            logging.info(f"{i} trips inserted...")

    return trip_ids


def insert_payments(cur, trip_ids):
    logging.info("Inserting payments...")
    for trip_id in trip_ids:
        if random.random() < 0.8:
            cur.execute(
                """
                INSERT INTO mobility.payments (trip_id, method, status, amount)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    trip_id,
                    random.choice(["cash", "card", "wallet"]),
                    random.choice(["paid", "failed", "pending"]),
                    round(random.uniform(5, 80), 2),
                ),
            )


def insert_ratings(cur, trip_ids):
    logging.info("Inserting ratings...")
    rated = random.sample(trip_ids, k=int(len(trip_ids) * 0.6))

    for trip_id in rated:
        cur.execute(
            """
            INSERT INTO mobility.ratings (trip_id, passenger_id, driver_id, score)
            SELECT t.trip_id, t.passenger_id, t.driver_id, %s
            FROM mobility.trips t
            WHERE t.trip_id = %s
            """,
            (random.randint(1, 5), trip_id),
        )

def update_trip_statuses(cur, max_updates=3000):
    logging.info("Updating existing trips statuses...")

    cur.execute("""
        SELECT trip_id, started_at, estimated_distance_km
        FROM mobility.trips
        WHERE status IN ('requested','accepted','started')
        ORDER BY random()
        LIMIT %s
    """, (max_updates,))

    trips = cur.fetchall()

    for trip_id, started_at, estimated_distance in trips:
        new_status = random.choice(["completed", "canceled"])

        if new_status == "completed":
            ended_at = started_at + timedelta(minutes=random.randint(5, 40)) if started_at else datetime.now()
            raw_actual = float(estimated_distance) + random.uniform(-2, 5) if estimated_distance else None
            actual_distance = None if raw_actual and raw_actual < 0 else raw_actual

            cur.execute("""
                UPDATE mobility.trips
                SET status = %s,
                    ended_at = %s,
                    actual_distance_km = %s
                WHERE trip_id = %s
            """, (
                new_status,
                ended_at,
                actual_distance,
                trip_id
            ))

        else:  # canceled
            cur.execute("""
                UPDATE mobility.trips
                SET status = %s,
                    canceled_at = now(),
                    cancel_reason = %s
                WHERE trip_id = %s
            """, (
                new_status,
                random.choice(["passenger","driver","system"]),
                trip_id
            ))

    logging.info(f"Updated {len(trips)} trips")



# ============================================================
# Main
# ============================================================

def main():
    logging.info("Starting OLTP data generation")

    conn = get_connection()
    cur = conn.cursor()

    try:
        # Load zones (must exist)
        zone_ids = fetch_ids(cur, "zones", "zone_id")
        if not zone_ids:
            raise RuntimeError("No zones found. Seed zones before running generator.")

        # Seed entities only if empty
        passenger_ids = fetch_ids(cur, "passengers", "passenger_id")
        if not passenger_ids:
            passenger_ids = seed_passengers(cur)

        driver_ids = fetch_ids(cur, "drivers", "driver_id")
        vehicle_ids = fetch_ids(cur, "vehicles", "vehicle_id")
        if not driver_ids or not vehicle_ids:
            driver_ids, vehicle_ids = seed_drivers_and_vehicles(cur)

        trip_ids = insert_trips(
            cur,
            passenger_ids,
            driver_ids,
            vehicle_ids,
            zone_ids,
        )
        insert_payments(cur, trip_ids)
        logging.info("Payments inserted")
        insert_ratings(cur, trip_ids)
        logging.info("Ratings inserted")

        update_trip_statuses(cur, max_updates=3000)

        conn.commit()
        logging.info("OLTP data generation completed successfully")

    except Exception:
        conn.rollback()
        logging.exception("Error during OLTP data generation")
        raise

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
