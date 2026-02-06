"""
OLTP Data Generator – Urban Mobility (PLUS: GDPR extended)
---------------------------------------------------------
Purpose:
- Simulate a living OLTP system (real app behavior)
- Generate incremental operational data
- Feed downstream ETL pipelines

IMPORTANT:
- This is NOT ETL
- OLTP constraints must be respected

PLUS additions:
- GDPR erasure simulation for:
  - passengers (full_name/email/phone + accidental PII scrub)
  - drivers (full_name/license_number + vehicle plate anonymization + accidental PII scrub)
  - vehicles (plate anonymization + accidental PII scrub)
- Generate "accidental PII" fields so GDPR actually does something:
  - ratings.comment
  - payments.provider_ref
- Avoid using/updating soft-deleted entities for new activity
"""

import os
import random
import logging
import uuid
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
)  # accepted/canceled with distance
MISSING_DISTANCE_ON_COMPLETED_RATE = float(
    os.getenv("MISSING_DISTANCE_ON_COMPLETED_RATE", "0.02")
)  # completed but actual_distance is NULL
MISSING_DISTANCE_ON_STARTED_RATE = float(
    os.getenv("MISSING_DISTANCE_ON_STARTED_RATE", "0.01")
)

# --- cancel_note noise knobs ---
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

# --- Driver growth / changes per run ---
N_NEW_DRIVERS_PER_RUN = int(os.getenv("N_NEW_DRIVERS_PER_RUN", "25"))
N_DRIVER_UPDATES_PER_RUN = int(os.getenv("N_DRIVER_UPDATES_PER_RUN", "60"))
DRIVER_STATUS_CHANGE_RATE = float(os.getenv("DRIVER_STATUS_CHANGE_RATE", "0.30"))

# --- Passenger growth / changes per run ---
N_NEW_PASSENGERS_PER_RUN = int(os.getenv("N_NEW_PASSENGERS_PER_RUN", "80"))
N_PASSENGER_UPDATES_PER_RUN = int(os.getenv("N_PASSENGER_UPDATES_PER_RUN", "200"))

# --- GDPR / RTBF simulation (OLTP-only) ---
GDPR_ERASURE_RATE = float(os.getenv("GDPR_ERASURE_RATE", "0.10"))  # % of runs that trigger GDPR

N_GDPR_PASSENGER_ERASURES_PER_RUN = int(os.getenv("N_GDPR_PASSENGER_ERASURES_PER_RUN", "2"))
N_GDPR_DRIVER_ERASURES_PER_RUN = int(os.getenv("N_GDPR_DRIVER_ERASURES_PER_RUN", "1"))
N_GDPR_VEHICLE_ERASURES_PER_RUN = int(os.getenv("N_GDPR_VEHICLE_ERASURES_PER_RUN", "1"))

# --- Accidental PII simulation ---
RATINGS_COMMENT_RATE = float(os.getenv("RATINGS_COMMENT_RATE", "0.25"))           # % ratings with comment
RATINGS_COMMENT_PII_RATE = float(os.getenv("RATINGS_COMMENT_PII_RATE", "0.05"))   # % comments with accidental PII
PAYMENT_PROVIDER_REF_RATE = float(os.getenv("PAYMENT_PROVIDER_REF_RATE", "0.30")) # % payments with provider_ref


# ============================================================
# Setup
# ============================================================

fake = Faker()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


# ============================================================
# Basic helpers
# ============================================================

def maybe_null(value, rate=BROKEN_RATE):
    return None if random.random() < rate else value


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_table_columns(cur, table_name: str):
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'mobility'
          AND table_name = %s
        """,
        (table_name,),
    )
    return {r[0] for r in cur.fetchall()}


def fetch_ids(cur, table, id_col):
    cur.execute(f"SELECT {id_col} FROM mobility.{table}")
    return [r[0] for r in cur.fetchall()]


# ============================================================
# Active/soft-delete aware fetch helpers
# ============================================================

def fetch_active_passenger_ids(cur):
    passengers_cols = get_table_columns(cur, "passengers")
    if "is_deleted" in passengers_cols:
        cur.execute("""
            SELECT passenger_id
            FROM mobility.passengers
            WHERE is_deleted = FALSE
        """)
    else:
        cur.execute("SELECT passenger_id FROM mobility.passengers")
    return [r[0] for r in cur.fetchall()]


def fetch_active_driver_ids(cur):
    drivers_cols = get_table_columns(cur, "drivers")
    if "is_deleted" in drivers_cols:
        cur.execute("""
            SELECT driver_id
            FROM mobility.drivers
            WHERE COALESCE(is_deleted, FALSE) = FALSE
        """)
    else:
        cur.execute("SELECT driver_id FROM mobility.drivers")
    return [r[0] for r in cur.fetchall()]


def fetch_active_vehicle_ids(cur):
    vehicles_cols = get_table_columns(cur, "vehicles")
    if "is_deleted" in vehicles_cols:
        cur.execute("""
            SELECT vehicle_id
            FROM mobility.vehicles
            WHERE COALESCE(is_deleted, FALSE) = FALSE
        """)
    else:
        cur.execute("SELECT vehicle_id FROM mobility.vehicles")
    return [r[0] for r in cur.fetchall()]


def fetch_driver_vehicle_pairs(cur):
    """
    Returns list of tuples: (driver_id, vehicle_id) for existing vehicles.
    Prefer active (non-deleted) pairs when soft-delete columns exist.
    """
    drivers_cols = get_table_columns(cur, "drivers")
    vehicles_cols = get_table_columns(cur, "vehicles")

    has_driver_deleted = "is_deleted" in drivers_cols
    has_vehicle_deleted = "is_deleted" in vehicles_cols

    if has_driver_deleted and has_vehicle_deleted:
        cur.execute("""
            SELECT v.driver_id, v.vehicle_id
            FROM mobility.vehicles v
            JOIN mobility.drivers d ON d.driver_id = v.driver_id
            WHERE COALESCE(v.is_deleted, FALSE) = FALSE
              AND COALESCE(d.is_deleted, FALSE) = FALSE
        """)
    elif has_driver_deleted:
        cur.execute("""
            SELECT v.driver_id, v.vehicle_id
            FROM mobility.vehicles v
            JOIN mobility.drivers d ON d.driver_id = v.driver_id
            WHERE COALESCE(d.is_deleted, FALSE) = FALSE
        """)
    elif has_vehicle_deleted:
        cur.execute("""
            SELECT driver_id, vehicle_id
            FROM mobility.vehicles
            WHERE COALESCE(is_deleted, FALSE) = FALSE
        """)
    else:
        cur.execute("SELECT driver_id, vehicle_id FROM mobility.vehicles")

    return cur.fetchall()


# ============================================================
# Noise helpers
# ============================================================

def maybe_high_precision(value, max_extra_decimals=6):
    """
    Postgres NUMERIC(10,3) will round/trim anyway, but this simulates upstream noise.
    """
    if value is None:
        return None
    if random.random() >= HIGH_PRECISION_NUMERIC_RATE:
        return value
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

    if random.random() < CANCEL_NOTE_NULLLIKE_RATE:
        return random.choice(["NULL", "null", "N/A", "-", "None", "  NULL  "])

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
    if random.random() < COORDS_MISSING_RATE:
        return (None, None, None, None)

    start_lat = float(fake.latitude())
    start_lng = float(fake.longitude())
    end_lat = float(fake.latitude())
    end_lng = float(fake.longitude())

    if random.random() < COORDS_OUT_OF_RANGE_RATE:
        start_lat = random.choice([95.0, -95.0, 123.456])
        start_lng = random.choice([190.0, -190.0, 222.222])

    if random.random() < COORDS_OUT_OF_RANGE_RATE:
        end_lat = random.choice([95.0, -95.0, 123.456])
        end_lng = random.choice([190.0, -190.0, 222.222])

    return (start_lat, start_lng, end_lat, end_lng)


def compute_times_for_status(status: str):
    """
    Returns (requested_at, accepted_at, started_at, ended_at, canceled_at)
    respecting trips_time_order_chk:
      accepted_at >= requested_at (if not null)
      started_at  >= requested_at (if not null)
      ended_at    >= requested_at (if not null)
    """
    requested_at = datetime.now()

    accept_lag = timedelta(minutes=random.randint(1, 10))
    start_lag = accept_lag + timedelta(minutes=random.randint(0, 10))
    end_lag = start_lag + timedelta(minutes=random.randint(5, 40))

    accepted_at = requested_at + accept_lag
    started_at = requested_at + start_lag
    ended_at = requested_at + end_lag
    canceled_at = None

    if random.random() < TIME_WEIRDNESS_RATE:
        if random.random() < 0.5:
            accepted_at = requested_at + timedelta(hours=random.randint(1, 72))
            started_at = requested_at + timedelta(hours=random.randint(1, 72))
            ended_at = requested_at + timedelta(hours=random.randint(1, 72))
        else:
            started_at = requested_at + timedelta(minutes=random.randint(1, 5))
            accepted_at = requested_at + timedelta(minutes=random.randint(6, 15))
            ended_at = requested_at + timedelta(minutes=random.randint(10, 60))

    if status == "requested":
        return requested_at, None, None, None, None

    if status == "accepted":
        return requested_at, accepted_at, None, None, None

    if status == "started":
        return requested_at, accepted_at, started_at, None, None

    if status == "completed":
        if random.random() < MISSING_ENDED_AT_ON_COMPLETED_RATE:
            ended_at = None
        return requested_at, accepted_at, started_at, ended_at, None

    if status == "canceled":
        canceled_at = requested_at + timedelta(minutes=random.randint(1, 20))
        if random.random() < 0.5:
            accepted_at = None
        if random.random() < 0.8:
            started_at = None
        ended_at = None
        return requested_at, accepted_at, started_at, ended_at, canceled_at

    return requested_at, accepted_at, started_at, ended_at, canceled_at


def compute_distances_for_status(status: str):
    """
    Returns (estimated_distance, actual_distance) with controlled anomalies.
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

    estimated_distance = maybe_high_precision(estimated_distance)
    actual_distance = maybe_high_precision(actual_distance)

    return estimated_distance, actual_distance


# ============================================================
# Accidental PII simulation helpers
# ============================================================

def fake_provider_ref():
    return f"gw_{uuid.uuid4().hex[:16]}"


def noisy_rating_comment():
    if random.random() > RATINGS_COMMENT_RATE:
        return None

    base = random.choice([
        "Great driver!",
        "Car was clean.",
        "Too much delay.",
        fake.sentence(nb_words=10),
    ])

    # accidental PII simulation
    if random.random() < RATINGS_COMMENT_PII_RATE:
        base += " | contact: " + random.choice([fake.email(), fake.phone_number(), fake.name()])

    return base


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
    """
    Seed drivers and 1 vehicle each, only if empty.
    Safe with UNIQUE constraints via ON CONFLICT + retries.
    """
    logging.info("Seeding drivers and vehicles...")
    driver_ids, vehicle_ids = [], []

    status_choices = ["active", "inactive", "suspended"]
    vehicle_status_choices = ["active", "inactive"]
    vehicle_type_choices = ["sedan", "hatchback", "motorbike"]

    inserted = 0
    attempts = 0
    max_attempts = max(N_DRIVERS * 50, 2000)

    while inserted < N_DRIVERS:
        attempts += 1
        if attempts > max_attempts:
            raise RuntimeError(f"seed_drivers_and_vehicles: max_attempts reached. Inserted {inserted}/{N_DRIVERS}")

        # insert driver
        cur.execute(
            """
            INSERT INTO mobility.drivers (full_name, license_number, status)
            VALUES (%s, %s, %s)
            ON CONFLICT (license_number) DO NOTHING
            RETURNING driver_id
            """,
            (fake.name(), fake.bothify("LIC-#####"), random.choice(status_choices)),
        )
        row = cur.fetchone()
        if not row:
            continue  # collision, retry

        driver_id = row[0]
        driver_ids.append(driver_id)

        # insert vehicle for that driver (retry on plate collision)
        v_attempts = 0
        while True:
            v_attempts += 1
            if v_attempts > 50:
                raise RuntimeError("seed_drivers_and_vehicles: too many plate collisions")

            cur.execute(
                """
                INSERT INTO mobility.vehicles (driver_id, plate_number, vehicle_type, status)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (plate_number) DO NOTHING
                RETURNING vehicle_id
                """,
                (
                    driver_id,
                    fake.license_plate(),
                    random.choice(vehicle_type_choices),
                    random.choice(vehicle_status_choices),
                ),
            )
            vrow = cur.fetchone()
            if vrow:
                vehicle_ids.append(vrow[0])
                break  # vehicle inserted

        inserted += 1
        if inserted % 100 == 0:
            logging.info(f"Drivers/Vehicles inserted: {inserted}/{N_DRIVERS}")

    return driver_ids, vehicle_ids


# ============================================================
# Incremental drivers/vehicles per run
# ============================================================

def insert_new_drivers_and_vehicles(cur, n_new: int):
    """
    Insert n_new drivers and 1 vehicle for each driver.
    Uses only columns that exist in your schema.
    Returns (new_driver_ids, new_vehicle_ids)
    """
    if n_new <= 0:
        return [], []

    drivers_cols = get_table_columns(cur, "drivers")
    vehicles_cols = get_table_columns(cur, "vehicles")

    new_driver_ids, new_vehicle_ids = [], []

    status_choices = ["active", "inactive", "suspended"]
    vehicle_status_choices = ["active", "inactive"]
    vehicle_type_choices = ["sedan", "hatchback", "motorbike"]

    inserted = 0
    attempts = 0
    max_attempts = max(n_new * 80, 500)

    while inserted < n_new:
        attempts += 1
        if attempts > max_attempts:
            logging.warning(f"insert_new_drivers_and_vehicles: max_attempts reached. Inserted {inserted}/{n_new}")
            break

        # --- build dynamic INSERT for drivers ---
        d_cols = []
        d_vals = []

        if "full_name" in drivers_cols:
            d_cols.append("full_name")
            d_vals.append(fake.name())

        if "license_number" in drivers_cols:
            d_cols.append("license_number")
            d_vals.append(fake.bothify("LIC-#####"))

        if "status" in drivers_cols:
            d_cols.append("status")
            d_vals.append(random.choice(status_choices))

        if "created_at" in drivers_cols:
            d_cols.append("created_at")
            d_vals.append(datetime.now())

        if "updated_at" in drivers_cols:
            d_cols.append("updated_at")
            d_vals.append(datetime.now())

        if not d_cols:
            raise RuntimeError("drivers table has no usable columns (unexpected schema).")

        placeholders = ",".join(["%s"] * len(d_cols))
        conflict_clause = ""
        if "license_number" in d_cols:
            conflict_clause = "ON CONFLICT (license_number) DO NOTHING"

        sql = f"""
            INSERT INTO mobility.drivers ({",".join(d_cols)})
            VALUES ({placeholders})
            {conflict_clause}
            RETURNING driver_id
        """
        cur.execute(sql, tuple(d_vals))
        row = cur.fetchone()
        if not row:
            continue  # collision, retry

        driver_id = row[0]
        new_driver_ids.append(driver_id)

        # --- build dynamic INSERT for vehicles ---
        v_cols = []
        v_vals = []

        if "driver_id" in vehicles_cols:
            v_cols.append("driver_id")
            v_vals.append(driver_id)

        if "plate_number" in vehicles_cols:
            v_cols.append("plate_number")
            v_vals.append(fake.license_plate())

        if "vehicle_type" in vehicles_cols:
            v_cols.append("vehicle_type")
            v_vals.append(random.choice(vehicle_type_choices))

        if "status" in vehicles_cols:
            v_cols.append("status")
            v_vals.append(random.choice(vehicle_status_choices))

        if "created_at" in vehicles_cols:
            v_cols.append("created_at")
            v_vals.append(datetime.now())

        if "updated_at" in vehicles_cols:
            v_cols.append("updated_at")
            v_vals.append(datetime.now())

        # plate unique -> retry on conflict
        if v_cols:
            v_inserted = False
            for _ in range(50):
                placeholders = ",".join(["%s"] * len(v_cols))
                conflict_clause = ""
                if "plate_number" in v_cols:
                    conflict_clause = "ON CONFLICT (plate_number) DO NOTHING"

                vsql = f"""
                    INSERT INTO mobility.vehicles ({",".join(v_cols)})
                    VALUES ({placeholders})
                    {conflict_clause}
                    RETURNING vehicle_id
                """
                cur.execute(vsql, tuple(v_vals))
                vrow = cur.fetchone()
                if vrow:
                    new_vehicle_ids.append(vrow[0])
                    v_inserted = True
                    break

                # collision -> generate a new plate
                if "plate_number" in v_cols:
                    idx = v_cols.index("plate_number")
                    v_vals[idx] = fake.license_plate()

            if not v_inserted:
                raise RuntimeError("insert_new_drivers_and_vehicles: too many plate collisions")

        inserted += 1

    return new_driver_ids, new_vehicle_ids


def update_existing_drivers(cur, max_updates: int):
    """
    Updates random existing active drivers so updated_at moves.
    Tries to update columns that exist: status, full_name, etc.
    """
    if max_updates <= 0:
        return 0

    drivers_cols = get_table_columns(cur, "drivers")
    has_deleted = "is_deleted" in drivers_cols

    if has_deleted:
        cur.execute(
            """
            SELECT driver_id
            FROM mobility.drivers
            WHERE COALESCE(is_deleted, FALSE) = FALSE
            ORDER BY random()
            LIMIT %s
            """,
            (max_updates,),
        )
    else:
        cur.execute(
            "SELECT driver_id FROM mobility.drivers ORDER BY random() LIMIT %s",
            (max_updates,),
        )

    driver_ids = [r[0] for r in cur.fetchall()]
    if not driver_ids:
        return 0

    status_choices = ["active", "inactive", "suspended"]

    updated = 0
    for driver_id in driver_ids:
        sets = []
        vals = []

        if "status" in drivers_cols and random.random() < DRIVER_STATUS_CHANGE_RATE:
            sets.append("status = %s")
            vals.append(random.choice(status_choices))

        if "full_name" in drivers_cols and random.random() < 0.05:
            sets.append("full_name = %s")
            vals.append(fake.name())

        # Always bump updated_at if exists (trigger also does it; this is explicit)
        if "updated_at" in drivers_cols:
            sets.append("updated_at = now()")

        if not sets:
            continue

        sql = f"UPDATE mobility.drivers SET {', '.join(sets)} WHERE driver_id = %s"
        vals.append(driver_id)
        cur.execute(sql, tuple(vals))
        updated += 1

    return updated


# ============================================================
# Incremental passengers per run
# ============================================================

def insert_new_passengers(cur, n_new: int):
    """
    Insert n_new passengers per run.
    Uses only columns that exist in your schema.
    Returns list of new passenger_ids.
    """
    if n_new <= 0:
        return []

    passengers_cols = get_table_columns(cur, "passengers")
    new_ids = []

    has_email = "email" in passengers_cols

    attempts = 0
    max_attempts = max(n_new * 25, 200)

    while len(new_ids) < n_new:
        attempts += 1
        if attempts > max_attempts:
            logging.warning(
                f"insert_new_passengers: reached max_attempts={max_attempts}. "
                f"Inserted {len(new_ids)}/{n_new} passengers."
            )
            break

        cols = []
        vals = []

        if "full_name" in passengers_cols:
            cols.append("full_name")
            vals.append(fake.name())

        if has_email:
            email_val = None if random.random() < BROKEN_RATE else fake.email()
            cols.append("email")
            vals.append(email_val)

        if "phone" in passengers_cols:
            cols.append("phone")
            vals.append(maybe_null(fake.phone_number()))

        if "city" in passengers_cols:
            cols.append("city")
            vals.append(fake.city())

        if "created_at" in passengers_cols:
            cols.append("created_at")
            vals.append(datetime.now())

        if "updated_at" in passengers_cols:
            cols.append("updated_at")
            vals.append(datetime.now())

        placeholders = ",".join(["%s"] * len(cols))

        if has_email:
            sql = f"""
                INSERT INTO mobility.passengers ({",".join(cols)})
                VALUES ({placeholders})
                ON CONFLICT (email) DO NOTHING
                RETURNING passenger_id
            """
        else:
            sql = f"""
                INSERT INTO mobility.passengers ({",".join(cols)})
                VALUES ({placeholders})
                RETURNING passenger_id
            """

        cur.execute(sql, tuple(vals))
        row = cur.fetchone()
        if row:
            new_ids.append(row[0])

    return new_ids


def update_existing_passengers(cur, max_updates: int):
    """
    Updates random active passengers so updated_at moves.
    SAFE: does NOT update email (UNIQUE) to avoid collisions.
    """
    if max_updates <= 0:
        return 0

    passengers_cols = get_table_columns(cur, "passengers")
    has_deleted = "is_deleted" in passengers_cols

    if has_deleted:
        cur.execute(
            """
            SELECT passenger_id
            FROM mobility.passengers
            WHERE is_deleted = FALSE
            ORDER BY random()
            LIMIT %s
            """,
            (max_updates,),
        )
    else:
        cur.execute(
            "SELECT passenger_id FROM mobility.passengers ORDER BY random() LIMIT %s",
            (max_updates,),
        )

    passenger_ids = [r[0] for r in cur.fetchall()]
    if not passenger_ids:
        return 0

    updated = 0
    for pid in passenger_ids:
        sets = []
        vals = []

        if "full_name" in passengers_cols and random.random() < 0.05:
            sets.append("full_name = %s")
            vals.append(fake.name())

        if "phone" in passengers_cols and random.random() < 0.10:
            sets.append("phone = %s")
            vals.append(maybe_null(fake.phone_number(), rate=0.15))

        if "city" in passengers_cols and random.random() < 0.10:
            sets.append("city = %s")
            vals.append(maybe_null(fake.city(), rate=0.15))

        if "updated_at" in passengers_cols:
            sets.append("updated_at = now()")

        if not sets:
            continue

        sql = f"UPDATE mobility.passengers SET {', '.join(sets)} WHERE passenger_id = %s"
        vals.append(pid)
        cur.execute(sql, tuple(vals))
        updated += 1

    return updated


# ============================================================
# GDPR (OLTP-only) – call your OLTP functions
# ============================================================

def apply_gdpr_passenger_erasure_requests(cur, passenger_ids, n_requests: int):
    """
    Calls mobility.gdpr_erasure_passenger(passenger_id, note)
    - logs GDPR request
    - anonymizes passenger PII
    - scrubs accidental PII in related tables (as per your function)
    """
    if n_requests <= 0 or not passenger_ids:
        return 0

    chosen = random.sample(passenger_ids, k=min(n_requests, len(passenger_ids)))
    processed = 0

    for pid in chosen:
        note = f"generator_erasure passenger_id={pid}"
        cur.execute("SELECT mobility.gdpr_erasure_passenger(%s, %s);", (pid, note))
        _request_id = cur.fetchone()[0]
        processed += 1

    return processed


def apply_gdpr_driver_erasure_requests(cur, driver_ids, n_requests: int):
    """
    Calls mobility.gdpr_erasure_driver(driver_id, note)
    - anonymizes driver PII + vehicle plates for that driver
    - scrubs accidental PII (ratings.comment, trips.cancel_note, payments.provider_ref)
    """
    if n_requests <= 0 or not driver_ids:
        return 0

    chosen = random.sample(driver_ids, k=min(n_requests, len(driver_ids)))
    processed = 0

    for did in chosen:
        note = f"generator_erasure driver_id={did}"
        cur.execute("SELECT mobility.gdpr_erasure_driver(%s, %s);", (did, note))
        _request_id = cur.fetchone()[0]
        processed += 1

    return processed


def apply_gdpr_vehicle_erasure_requests(cur, vehicle_ids, n_requests: int):
    """
    Calls mobility.gdpr_erasure_vehicle(vehicle_id, note)
    - anonymizes plate_number
    - scrubs accidental PII in trips/payments for that vehicle
    """
    if n_requests <= 0 or not vehicle_ids:
        return 0

    chosen = random.sample(vehicle_ids, k=min(n_requests, len(vehicle_ids)))
    processed = 0

    for vid in chosen:
        note = f"generator_erasure vehicle_id={vid}"
        cur.execute("SELECT mobility.gdpr_erasure_vehicle(%s, %s);", (vid, note))
        _request_id = cur.fetchone()[0]
        processed += 1

    return processed


# ============================================================
# Incremental inserts
# ============================================================

def insert_trips(cur, passenger_ids, driver_ids, vehicle_ids, zone_ids):
    logging.info(f"Inserting {N_TRIPS} trips...")
    trip_ids = []

    driver_vehicle_pairs = fetch_driver_vehicle_pairs(cur)
    if not driver_vehicle_pairs:
        raise RuntimeError("No active driver/vehicle pairs found. Seed drivers/vehicles first.")

    cancel_enum_values = ["passenger", "driver", "system"]

    for i in range(1, N_TRIPS + 1):
        status = random.choice(["requested", "accepted", "started", "completed", "canceled"])

        requested_at, accepted_at, started_at, ended_at, canceled_at = compute_times_for_status(status)

        pickup_zone_id = random.choice(zone_ids)
        dropoff_zone_id = random.choice(zone_ids)

        # Choose driver & vehicle: mostly consistent pair, sometimes mismatch
        if random.random() < VEHICLE_DRIVER_MISMATCH_RATE and driver_ids and vehicle_ids:
            driver_id = random.choice(driver_ids)
            vehicle_id = random.choice(vehicle_ids)
        else:
            driver_id, vehicle_id = random.choice(driver_vehicle_pairs)

        # requested trips often have no driver/vehicle
        if status == "requested":
            if random.random() < 0.9:
                driver_id = None
                vehicle_id = None
        elif status == "accepted":
            if random.random() < 0.2:
                vehicle_id = None

        start_lat, start_lng, end_lat, end_lng = generate_coords()

        estimated_distance, actual_distance = compute_distances_for_status(status)
        fare_amount = maybe_null(maybe_high_precision(round(random.uniform(5, 80), 2)))

        cancel_reason = None
        cancel_by = None
        cancel_note = None

        if status == "canceled":
            cancel_reason = random.choice(cancel_enum_values)
            cancel_by = random.choice(cancel_enum_values)
            cancel_note = noisy_cancel_note()
        else:
            if random.random() < (CANCEL_NOTE_GARBAGE_RATE * 0.1):
                cancel_note = noisy_cancel_note()

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

    payments_cols = get_table_columns(cur, "payments")
    has_provider_ref = "provider_ref" in payments_cols

    for trip_id in trip_ids:
        if random.random() < 0.8:
            provider_ref = None
            if has_provider_ref and random.random() < PAYMENT_PROVIDER_REF_RATE:
                provider_ref = fake_provider_ref()

            if has_provider_ref:
                cur.execute(
                    """
                    INSERT INTO mobility.payments (trip_id, method, status, amount, provider_ref)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        trip_id,
                        random.choice(["cash", "card", "wallet"]),
                        random.choice(["paid", "failed", "pending"]),
                        round(random.uniform(5, 80), 2),
                        provider_ref,
                    ),
                )
            else:
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

    ratings_cols = get_table_columns(cur, "ratings")
    has_comment = "comment" in ratings_cols

    cur.execute(
        """
        SELECT trip_id
        FROM mobility.trips
        WHERE trip_id = ANY(%s)
          AND driver_id IS NOT NULL
          AND passenger_id IS NOT NULL
          AND status = 'completed'
        """,
        (trip_ids,),
    )
    eligible_trip_ids = [r[0] for r in cur.fetchall()]

    if not eligible_trip_ids:
        logging.info("No eligible trips for ratings (no completed trips with driver).")
        return

    rated = random.sample(eligible_trip_ids, k=int(len(eligible_trip_ids) * 0.6))

    for trip_id in rated:
        if has_comment:
            cur.execute(
                """
                INSERT INTO mobility.ratings (trip_id, passenger_id, driver_id, score, comment)
                SELECT t.trip_id, t.passenger_id, t.driver_id, %s, %s
                FROM mobility.trips t
                WHERE t.trip_id = %s
                  AND t.driver_id IS NOT NULL
                  AND t.passenger_id IS NOT NULL
                """,
                (random.randint(1, 5), noisy_rating_comment(), trip_id),
            )
        else:
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
            base = started_at if started_at else requested_at if requested_at else datetime.now()
            ended_at = base + timedelta(minutes=random.randint(5, 40))

            raw_actual = float(estimated_distance) + random.uniform(-2, 5) if estimated_distance else None
            raw_actual = None if (raw_actual is not None and raw_actual < 0) else raw_actual

            if raw_actual is None or random.random() < MISSING_DISTANCE_ON_COMPLETED_RATE:
                actual_distance = None
            else:
                actual_distance = maybe_high_precision(raw_actual)

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

        else:
            canceled_at = (requested_at or datetime.now()) + timedelta(minutes=random.randint(1, 20))

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
    logging.info("Starting OLTP data generation (PLUS GDPR)")

    conn = get_connection()
    cur = conn.cursor()

    try:
        zone_ids = fetch_ids(cur, "zones", "zone_id")
        if not zone_ids:
            raise RuntimeError("No zones found. Seed zones before running generator.")

        # ----------------------------
        # PASSENGERS (seed/grow/update)
        # ----------------------------
        passenger_ids = fetch_active_passenger_ids(cur)
        if not passenger_ids:
            _all_passenger_ids = fetch_ids(cur, "passengers", "passenger_id")
            if not _all_passenger_ids:
                passenger_ids = seed_passengers(cur)
            else:
                passenger_ids = fetch_active_passenger_ids(cur)

        new_passenger_ids = insert_new_passengers(cur, N_NEW_PASSENGERS_PER_RUN)
        if new_passenger_ids:
            logging.info(f"New passengers inserted this run: {len(new_passenger_ids)}")
            passenger_ids.extend(new_passenger_ids)

        n_p_updated = update_existing_passengers(cur, N_PASSENGER_UPDATES_PER_RUN)
        logging.info(f"Passengers updated this run: {n_p_updated}")

        # ----------------------------
        # DRIVERS + VEHICLES (seed/grow/update)
        # ----------------------------
        driver_ids = fetch_active_driver_ids(cur)
        vehicle_ids = fetch_active_vehicle_ids(cur)
        if not driver_ids or not vehicle_ids:
            driver_ids, vehicle_ids = seed_drivers_and_vehicles(cur)
            # ensure we only keep active after seed
            driver_ids = fetch_active_driver_ids(cur)
            vehicle_ids = fetch_active_vehicle_ids(cur)

        new_driver_ids, new_vehicle_ids = insert_new_drivers_and_vehicles(cur, N_NEW_DRIVERS_PER_RUN)
        if new_driver_ids:
            logging.info(f"New drivers inserted this run: {len(new_driver_ids)}")
            driver_ids.extend(new_driver_ids)
        if new_vehicle_ids:
            logging.info(f"New vehicles inserted this run: {len(new_vehicle_ids)}")
            vehicle_ids.extend(new_vehicle_ids)

        n_d_updated = update_existing_drivers(cur, N_DRIVER_UPDATES_PER_RUN)
        logging.info(f"Drivers updated this run: {n_d_updated}")

        # Keep lists clean (avoid deleted ones if GDPR ran previously)
        passenger_ids = fetch_active_passenger_ids(cur)
        driver_ids = fetch_active_driver_ids(cur)
        vehicle_ids = fetch_active_vehicle_ids(cur)

        # ----------------------------
        # CORE ACTIVITY
        # ----------------------------
        if not passenger_ids:
            raise RuntimeError("No active passengers available for trips.")
        trip_ids = insert_trips(cur, passenger_ids, driver_ids, vehicle_ids, zone_ids)

        insert_payments(cur, trip_ids)
        logging.info("Payments inserted")

        insert_ratings(cur, trip_ids)
        logging.info("Ratings inserted")

        update_trip_statuses(cur, max_updates=3000)

        # ----------------------------
        # GDPR erasure simulation sometimes
        # ----------------------------
        if random.random() < GDPR_ERASURE_RATE:
            active_passengers = fetch_active_passenger_ids(cur)
            active_drivers = fetch_active_driver_ids(cur)
            active_vehicles = fetch_active_vehicle_ids(cur)

            n_gp = apply_gdpr_passenger_erasure_requests(cur, active_passengers, N_GDPR_PASSENGER_ERASURES_PER_RUN)
            n_gd = apply_gdpr_driver_erasure_requests(cur, active_drivers, N_GDPR_DRIVER_ERASURES_PER_RUN)
            n_gv = apply_gdpr_vehicle_erasure_requests(cur, active_vehicles, N_GDPR_VEHICLE_ERASURES_PER_RUN)

            logging.info(f"GDPR erasures processed this run: passengers={n_gp}, drivers={n_gd}, vehicles={n_gv}")
        else:
            logging.info("GDPR erasures processed this run: 0")

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
