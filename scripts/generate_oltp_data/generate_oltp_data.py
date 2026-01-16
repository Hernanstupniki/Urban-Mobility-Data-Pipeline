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
N_TRIPS = int(os.getenv("N_TRIPS", "10000"))

# Seed sizes (only if empty)
N_PASSENGERS = int(os.getenv("N_PASSENGERS", "2000"))
N_DRIVERS = int(os.getenv("N_DRIVERS", "500"))

# Generic "broken data" rate for nullable fields
BROKEN_RATE = float(os.getenv("BROKEN_RATE", "0.20"))
LOG_EVERY = int(os.getenv("LOG_EVERY", "5000"))

# --- Rates to control your Silver flag has_distance_in_invalid_status ---
INVALID_DISTANCE_IN_WRONG_STATUS_RATE = float(
    os.getenv("INVALID_DISTANCE_IN_WRONG_STATUS_RATE", "0.01")
)  # A: accepted/canceled with distance
MISSING_DISTANCE_ON_COMPLETED_RATE = float(
    os.getenv("MISSING_DISTANCE_ON_COMPLETED_RATE", "0.02")
)  # B: completed but distance is NULL
MISSING_DISTANCE_ON_STARTED_RATE = float(
    os.getenv("MISSING_DISTANCE_ON_STARTED_RATE", "0.01")
)

# --- New "noise" knobs ---
CANCEL_NOTE_GARBAGE_RATE = float(os.getenv("CANCEL_NOTE_GARBAGE_RATE", "0.20"))
CANCEL_NOTE_NULLLIKE_RATE = float(os.getenv("CANCEL_NOTE_NULLLIKE_RATE", "0.10"))
CANCEL_NOTE_EMPTY_STRING_RATE = float(os.getenv("CANCEL_NOTE_EMPTY_STRING_RATE", "0.05"))

TIME_WEIRDNESS_RATE = float(os.getenv("TIME_WEIRDNESS_RATE", "0.03"))  # started_at < accepted_at, huge lags, etc.
COORDS_MISSING_RATE = float(os.getenv("COORDS_MISSING_RATE", "0.10"))
COORDS_OUT_OF_RANGE_RATE = float(os.getenv("COORDS_OUT_OF_RANGE_RATE", "0.01"))

VEHICLE_DRIVER_MISMATCH_RATE = float(os.getenv("VEHICLE_DRIVER_MISMATCH_RATE", "0.02"))
HIGH_PRECISION_NUMERIC_RATE = float(os.getenv("HIGH_PRECISION_NUMERIC_RATE", "0.20"))

# If you want more "completed missing ended_at" noise (independent of BROKEN_RATE)
MISSING_ENDED_AT_ON_COMPLETED_RATE = float(os.getenv("MISSING_ENDED_AT_ON_COMPLETED_RATE", "0.02"))

# ============================================================
# Setup
# ============================================================

fake = Faker()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


def maybe_null(value, rate=BROKEN_RATE):
    return None if random.random() < rate else value


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def fetch_ids(cur, table, id_col):
    cur.execute(f"SELECT {id_col} FROM mobility.{table}")
    return [r[0] for r in cur.fetchall()]


def fetch_driver_vehicle_pairs(cur):
    """
    Returns list of tuples: (driver_id, vehicle_id) for existing vehicles.
    Useful to generate consistent driver<->vehicle combos, and mismatch on purpose.
    """
    cur.execute("SELECT driver_id, vehicle_id FROM mobility.vehicles")
    return cur.fetchall()


def maybe_high_precision(value, max_extra_decimals=6):
    """
    Postgres NUMERIC(10,3) will round/trim anyway, but this simulates upstream noise.
    """
    if value is None:
        return None
    if random.random() >= HIGH_PRECISION_NUMERIC_RATE:
        return value
    # add extra fractional precision
    noise = random.random() / (10 ** max_extra_decimals)
    return float(value) + noise


def noisy_cancel_note():
    """
    Generates cancel_note with realistic mess:
    - NULL-like strings
    - empty strings
    - leading/trailing whitespace
    - newlines, emojis, random text
    """
    if random.random() > CANCEL_NOTE_GARBAGE_RATE:
        return None

    # null-like
    if random.random() < CANCEL_NOTE_NULLLIKE_RATE:
        return random.choice(["NULL", "null", "N/A", "-", "None", "  NULL  "])

    # empty string
    if random.random() < CANCEL_NOTE_EMPTY_STRING_RATE:
        return ""

    base = random.choice([
        "No me contestó",
        "Cancelé porque me equivoqué",
        "El chofer no llegó",
        "Sistema se cayó",
        "Demasiada demora",
        fake.sentence(nb_words=6),
    ])

    # add whitespace and line breaks
    if random.random() < 0.5:
        base = " " * random.randint(1, 4) + base + " " * random.randint(1, 4)
    if random.random() < 0.2:
        base = base + "\n" + fake.sentence(nb_words=4)
    if random.random() < 0.2:
        base = base + " " + random.choice(["😅", "🚗", "❌", "🕒"])

    return base


def generate_coords():
    """
    Generates (start_lat, start_lng, end_lat, end_lng)
    with missing and out-of-range noise.
    """
    # missing coords
    if random.random() < COORDS_MISSING_RATE:
        return (None, None, None, None)

    # base: plausible coords using faker
    # faker returns strings often -> cast to float
    start_lat = float(fake.latitude())
    start_lng = float(fake.longitude())
    end_lat = float(fake.latitude())
    end_lng = float(fake.longitude())

    # out of range noise
    if random.random() < COORDS_OUT_OF_RANGE_RATE:
        start_lat = random.choice([95.0, -95.0, 123.456])   # invalid lat
        start_lng = random.choice([190.0, -190.0, 222.222]) # invalid lng

    if random.random() < COORDS_OUT_OF_RANGE_RATE:
        end_lat = random.choice([95.0, -95.0, 123.456])
        end_lng = random.choice([190.0, -190.0, 222.222])

    return (start_lat, start_lng, end_lat, end_lng)


def compute_times_for_status(status: str):
    """
    Returns (requested_at, accepted_at, started_at, ended_at, canceled_at)
    respecting your trips_time_order_chk:
      accepted_at >= requested_at (if not null)
      started_at  >= requested_at (if not null)
      ended_at    >= requested_at (if not null)

    Note: We intentionally allow weird ordering between accepted_at and started_at
    because the constraint doesn't forbid it (real systems can be messy).
    """
    requested_at = datetime.now()

    # base lags
    accept_lag = timedelta(minutes=random.randint(1, 10))
    start_lag = accept_lag + timedelta(minutes=random.randint(0, 10))
    end_lag = start_lag + timedelta(minutes=random.randint(5, 40))

    accepted_at = requested_at + accept_lag
    started_at = requested_at + start_lag
    ended_at = requested_at + end_lag
    canceled_at = None

    # introduce weirdness but keep >= requested_at
    if random.random() < TIME_WEIRDNESS_RATE:
        # huge delays
        if random.random() < 0.5:
            accepted_at = requested_at + timedelta(hours=random.randint(1, 72))
            started_at = requested_at + timedelta(hours=random.randint(1, 72))
            ended_at = requested_at + timedelta(hours=random.randint(1, 72))
        else:
            # started_at earlier than accepted_at (still >= requested_at)
            started_at = requested_at + timedelta(minutes=random.randint(1, 5))
            accepted_at = requested_at + timedelta(minutes=random.randint(6, 15))

            # ended_at may exist even if started is early
            ended_at = requested_at + timedelta(minutes=random.randint(10, 60))

    if status == "requested":
        return requested_at, None, None, None, None

    if status == "accepted":
        return requested_at, accepted_at, None, None, None

    if status == "started":
        return requested_at, accepted_at, started_at, None, None

    if status == "completed":
        # allow missing ended_at anomaly (plus maybe_null below)
        if random.random() < MISSING_ENDED_AT_ON_COMPLETED_RATE:
            ended_at = None
        return requested_at, accepted_at, started_at, ended_at, None

    if status == "canceled":
        canceled_at = requested_at + timedelta(minutes=random.randint(1, 20))
        # In canceled, you can still have accepted_at/started_at sometimes (messy state machine)
        # But keep >= requested_at always.
        # We'll sometimes null them for realism.
        if random.random() < 0.5:
            accepted_at = None
        if random.random() < 0.8:
            started_at = None
        ended_at = None
        return requested_at, accepted_at, started_at, ended_at, canceled_at

    # fallback
    return requested_at, accepted_at, started_at, ended_at, canceled_at


def compute_distances_for_status(status: str):
    """
    Returns (estimated_distance, actual_distance) with controlled anomalies
    according to your has_distance_in_invalid_status logic.
    """
    estimated_distance = round(random.uniform(1, 30), 2)
    raw_actual = round(estimated_distance + random.uniform(-2, 5), 2)
    raw_actual = None if raw_actual < 0 else raw_actual

    actual_distance = None

    if status == "completed":
        if raw_actual is None or random.random() < MISSING_DISTANCE_ON_COMPLETED_RATE:
            actual_distance = None
        else:
            actual_distance = raw_actual

    elif status == "started":
        if raw_actual is None or random.random() < MISSING_DISTANCE_ON_STARTED_RATE:
            actual_distance = None
        else:
            actual_distance = raw_actual

    else:
        if raw_actual is not None and random.random() < INVALID_DISTANCE_IN_WRONG_STATUS_RATE:
            actual_distance = raw_actual
        else:
            actual_distance = None

    # add numeric precision noise
    estimated_distance = maybe_high_precision(estimated_distance)
    actual_distance = maybe_high_precision(actual_distance)

    return estimated_distance, actual_distance


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
            ),
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

    # driver<->vehicle pairs for consistency (and mismatch noise)
    driver_vehicle_pairs = fetch_driver_vehicle_pairs(cur)
    if not driver_vehicle_pairs:
        raise RuntimeError("No vehicles found. Seed drivers/vehicles first.")

    cancel_enum_values = ["passenger", "driver", "system"]  # keep safe with your enum usage

    for i in range(1, N_TRIPS + 1):
        status = random.choice(["requested", "accepted", "started", "completed", "canceled"])

        requested_at, accepted_at, started_at, ended_at, canceled_at = compute_times_for_status(status)

        pickup_zone_id = random.choice(zone_ids)
        dropoff_zone_id = random.choice(zone_ids)

        # Choose driver & vehicle: mostly consistent pair, sometimes mismatch
        if random.random() < VEHICLE_DRIVER_MISMATCH_RATE:
            # mismatch: pick random driver and random vehicle independently
            driver_id = random.choice(driver_ids)
            vehicle_id = random.choice(vehicle_ids)
        else:
            driver_id, vehicle_id = random.choice(driver_vehicle_pairs)

        # requested trips could be without driver/vehicle sometimes (realistic),
        # but your schema allows null driver/vehicle.
        if status == "requested":
            if random.random() < 0.9:
                driver_id = None
                vehicle_id = None
        elif status == "accepted":
            if random.random() < 0.2:
                vehicle_id = None  # e.g. assigned driver but vehicle missing

        start_lat, start_lng, end_lat, end_lng = generate_coords()

        estimated_distance, actual_distance = compute_distances_for_status(status)
        fare_amount = maybe_null(maybe_high_precision(round(random.uniform(5, 80), 2)))

        # cancel fields
        cancel_reason = None
        cancel_by = None
        cancel_note = None

        if status == "canceled":
            cancel_reason = random.choice(cancel_enum_values)
            cancel_by = random.choice(cancel_enum_values)
            cancel_note = noisy_cancel_note()
        else:
            # sometimes you still get garbage cancel_note even when not canceled (integration bug)
            if random.random() < (CANCEL_NOTE_GARBAGE_RATE * 0.1):
                cancel_note = noisy_cancel_note()

        # ended_at "broken" behavior (keep your original idea)
        ended_at = maybe_null(ended_at) if status in ("completed",) else ended_at

        cur.execute(
            """
            INSERT INTO mobility.trips (
                passenger_id,
                driver_id,
                vehicle_id,
                pickup_zone_id,
                dropoff_zone_id,

                start_lat,
                start_lng,
                end_lat,
                end_lng,

                status,

                requested_at,
                accepted_at,
                started_at,
                ended_at,

                canceled_at,
                cancel_reason,
                cancel_by,
                cancel_note,

                estimated_distance_km,
                actual_distance_km,
                fare_amount
            )
            VALUES (
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,
                %s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s
            )
            RETURNING trip_id
            """,
            (
                random.choice(passenger_ids),
                driver_id,
                vehicle_id,
                pickup_zone_id,
                dropoff_zone_id,

                start_lat,
                start_lng,
                end_lat,
                end_lng,

                status,

                requested_at,
                accepted_at,
                started_at,
                ended_at,

                canceled_at,
                cancel_reason,
                cancel_by,
                cancel_note,

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

    # Solo viajes que tienen driver asignado (y opcionalmente completed)
    cur.execute(
        """
        SELECT trip_id
        FROM mobility.trips
        WHERE trip_id = ANY(%s)
          AND driver_id IS NOT NULL
          AND passenger_id IS NOT NULL
          AND status = 'completed'
        """,
        (trip_ids,)
    )
    eligible_trip_ids = [r[0] for r in cur.fetchall()]

    if not eligible_trip_ids:
        logging.info("No eligible trips for ratings (no completed trips with driver).")
        return

    rated = random.sample(eligible_trip_ids, k=int(len(eligible_trip_ids) * 0.6))

    for trip_id in rated:
        cur.execute(
            """
            INSERT INTO mobility.ratings (trip_id, passenger_id, driver_id, score)
            SELECT t.trip_id, t.passenger_id, t.driver_id, %s
            FROM mobility.trips t
            WHERE t.trip_id = %s
              AND t.driver_id IS NOT NULL
              AND t.passenger_id IS NOT NULL
            """,
            (random.randint(1, 5), trip_id),
        )


def update_trip_statuses(cur, max_updates=3000):
    logging.info("Updating existing trips statuses...")

    cur.execute(
        """
        SELECT trip_id, started_at, estimated_distance_km, requested_at
        FROM mobility.trips
        WHERE status IN ('requested','accepted','started')
        ORDER BY random()
        LIMIT %s
        """,
        (max_updates,),
    )

    trips = cur.fetchall()

    cancel_enum_values = ["passenger", "driver", "system"]

    for trip_id, started_at, estimated_distance, requested_at in trips:
        new_status = random.choice(["completed", "canceled"])

        if new_status == "completed":
            # ended_at should be >= requested_at
            base = started_at if started_at else requested_at if requested_at else datetime.now()
            ended_at = base + timedelta(minutes=random.randint(5, 40))

            raw_actual = float(estimated_distance) + random.uniform(-2, 5) if estimated_distance else None
            raw_actual = None if (raw_actual is not None and raw_actual < 0) else raw_actual

            if raw_actual is None or random.random() < MISSING_DISTANCE_ON_COMPLETED_RATE:
                actual_distance = None
            else:
                actual_distance = maybe_high_precision(raw_actual)

            # sometimes missing ended_at too
            if random.random() < MISSING_ENDED_AT_ON_COMPLETED_RATE:
                ended_at = None

            cur.execute(
                """
                UPDATE mobility.trips
                SET status = %s,
                    ended_at = %s,
                    actual_distance_km = %s,
                    updated_at = now()
                WHERE trip_id = %s
                """,
                (new_status, ended_at, actual_distance, trip_id),
            )

        else:  # canceled
            # canceled_at should be >= requested_at (no constraint, but keep consistent)
            canceled_at = (requested_at or datetime.now()) + timedelta(minutes=random.randint(1, 20))

            # Normally canceled should NOT have distance; keep anomaly A
            raw_candidate = float(estimated_distance) + random.uniform(-2, 5) if estimated_distance else None
            raw_candidate = None if (raw_candidate is not None and raw_candidate < 0) else raw_candidate

            if raw_candidate is not None and random.random() < INVALID_DISTANCE_IN_WRONG_STATUS_RATE:
                actual_distance = maybe_high_precision(raw_candidate)
            else:
                actual_distance = None

            cur.execute(
                """
                UPDATE mobility.trips
                SET status = %s,
                    canceled_at = %s,
                    cancel_reason = %s,
                    cancel_by = %s,
                    cancel_note = %s,
                    actual_distance_km = %s,
                    ended_at = NULL,
                    updated_at = now()
                WHERE trip_id = %s
                """,
                (
                    new_status,
                    canceled_at,
                    random.choice(cancel_enum_values),
                    random.choice(cancel_enum_values),
                    noisy_cancel_note(),
                    actual_distance,
                    trip_id,
                ),
            )

    logging.info(f"Updated {len(trips)} trips")


# ============================================================
# Main
# ============================================================

def main():
    logging.info("Starting OLTP data generation")

    conn = get_connection()
    cur = conn.cursor()

    try:
        zone_ids = fetch_ids(cur, "zones", "zone_id")
        if not zone_ids:
            raise RuntimeError("No zones found. Seed zones before running generator.")

        passenger_ids = fetch_ids(cur, "passengers", "passenger_id")
        if not passenger_ids:
            passenger_ids = seed_passengers(cur)

        driver_ids = fetch_ids(cur, "drivers", "driver_id")
        vehicle_ids = fetch_ids(cur, "vehicles", "vehicle_id")
        if not driver_ids or not vehicle_ids:
            driver_ids, vehicle_ids = seed_drivers_and_vehicles(cur)

        trip_ids = insert_trips(cur, passenger_ids, driver_ids, vehicle_ids, zone_ids)

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
