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
import sys
import logging
import uuid
from datetime import datetime, timedelta

import psycopg2
from faker import Faker

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mobility_model import (  # noqa: E402
    DQ_RATES,
    MobilityModel,
    apply_driver_dq,
    apply_note_dq,
    apply_passenger_dq,
    apply_vehicle_dq,
    in_incident_window,
    requested_at_source_text,
)
from mobility_model import _clamp  # noqa: E402


# Configuration (env-driven)

# Dual naming (OLTP_DB_* preferred, DB_* fallback) matches src/common/config.py:
# containers receive OLTP_DB_* from docker-compose, host CLI exports DB_*.
DB_CONFIG = {
    "host": os.getenv("OLTP_DB_HOST", os.getenv("DB_HOST", "localhost")),
    "port": int(os.getenv("OLTP_DB_PORT", os.getenv("DB_PORT", "5432"))),
    "dbname": os.getenv("OLTP_DB_NAME", os.getenv("DB_NAME", "mobility_oltp")),
    "user": os.getenv("OLTP_DB_USER", os.getenv("DB_USER", "postgres")),
    "password": os.getenv("OLTP_DB_PASSWORD", os.getenv("DB_PASSWORD")),
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

# --- Broader, domain-aware quality noise ---
# --- formatting noise: now split into INDEPENDENT case / pad channels
# (TEXT_FORMAT_NOISE_RATE is kept as the historical aggregate knob and is
# intentionally unused by the new per-anomaly injectors; kept for contract
# compatibility.)
TEXT_FORMAT_NOISE_RATE = float(os.getenv("TEXT_FORMAT_NOISE_RATE", "0.15"))
TEXT_FORMAT_CASE_RATE = float(os.getenv("TEXT_FORMAT_CASE_RATE", "0.09"))
TEXT_FORMAT_PAD_RATE = float(os.getenv("TEXT_FORMAT_PAD_RATE", "0.07"))
CATEGORY_VARIANT_RATE = float(os.getenv("CATEGORY_VARIANT_RATE", "0.12"))
INVALID_CONTACT_RATE = float(os.getenv("INVALID_CONTACT_RATE", "0.04"))
DUPLICATE_PASSENGER_RATE = float(os.getenv("DUPLICATE_PASSENGER_RATE", "0.03"))
DUPLICATE_PAYMENT_RATE = float(os.getenv("DUPLICATE_PAYMENT_RATE", "0.03"))
PAYMENT_PROVIDER_REF_PII_RATE = float(os.getenv("PAYMENT_PROVIDER_REF_PII_RATE", "0.02"))
CANCEL_NOTE_PII_RATE = float(os.getenv("CANCEL_NOTE_PII_RATE", "0.03"))
LONG_ACCEPTANCE_DELAY_RATE = float(os.getenv("LONG_ACCEPTANCE_DELAY_RATE", "0.02"))
LONG_TRIP_DURATION_RATE = float(os.getenv("LONG_TRIP_DURATION_RATE", "0.02"))
DISTANCE_OUTLIER_RATE = float(os.getenv("DISTANCE_OUTLIER_RATE", "0.03"))
PAYMENT_TIMESTAMP_INCONSISTENCY_RATE = float(
    os.getenv("PAYMENT_TIMESTAMP_INCONSISTENCY_RATE", "0.03")
)

# Reproducibility now has an explicit default so every bootstrap is
# reproducible; override via env for experiments.
RANDOM_SEED = os.getenv("RANDOM_SEED", "20260921")

DIRTY_DATA_RATES = {
    "TEXT_FORMAT_CASE_RATE": TEXT_FORMAT_CASE_RATE,
    "TEXT_FORMAT_PAD_RATE": TEXT_FORMAT_PAD_RATE,
    **DQ_RATES,  # granular, INDEPENDENT per-anomaly-type rates
    "BROKEN_RATE": BROKEN_RATE,
    "INVALID_DISTANCE_IN_WRONG_STATUS_RATE": INVALID_DISTANCE_IN_WRONG_STATUS_RATE,
    "MISSING_DISTANCE_ON_COMPLETED_RATE": MISSING_DISTANCE_ON_COMPLETED_RATE,
    "MISSING_DISTANCE_ON_STARTED_RATE": MISSING_DISTANCE_ON_STARTED_RATE,
    "CANCEL_NOTE_GARBAGE_RATE": CANCEL_NOTE_GARBAGE_RATE,
    "CANCEL_NOTE_NULLLIKE_RATE": CANCEL_NOTE_NULLLIKE_RATE,
    "CANCEL_NOTE_EMPTY_STRING_RATE": CANCEL_NOTE_EMPTY_STRING_RATE,
    "TIME_WEIRDNESS_RATE": TIME_WEIRDNESS_RATE,
    "COORDS_MISSING_RATE": COORDS_MISSING_RATE,
    "COORDS_OUT_OF_RANGE_RATE": COORDS_OUT_OF_RANGE_RATE,
    "VEHICLE_DRIVER_MISMATCH_RATE": VEHICLE_DRIVER_MISMATCH_RATE,
    "HIGH_PRECISION_NUMERIC_RATE": HIGH_PRECISION_NUMERIC_RATE,
    "MISSING_ENDED_AT_ON_COMPLETED_RATE": MISSING_ENDED_AT_ON_COMPLETED_RATE,
    "DRIVER_STATUS_CHANGE_RATE": DRIVER_STATUS_CHANGE_RATE,
    "GDPR_ERASURE_RATE": GDPR_ERASURE_RATE,
    "RATINGS_COMMENT_RATE": RATINGS_COMMENT_RATE,
    "RATINGS_COMMENT_PII_RATE": RATINGS_COMMENT_PII_RATE,
    "PAYMENT_PROVIDER_REF_RATE": PAYMENT_PROVIDER_REF_RATE,
    "TEXT_FORMAT_NOISE_RATE": TEXT_FORMAT_NOISE_RATE,
    "CATEGORY_VARIANT_RATE": CATEGORY_VARIANT_RATE,
    "INVALID_CONTACT_RATE": INVALID_CONTACT_RATE,
    "DUPLICATE_PASSENGER_RATE": DUPLICATE_PASSENGER_RATE,
    "DUPLICATE_PAYMENT_RATE": DUPLICATE_PAYMENT_RATE,
    "PAYMENT_PROVIDER_REF_PII_RATE": PAYMENT_PROVIDER_REF_PII_RATE,
    "CANCEL_NOTE_PII_RATE": CANCEL_NOTE_PII_RATE,
    "LONG_ACCEPTANCE_DELAY_RATE": LONG_ACCEPTANCE_DELAY_RATE,
    "LONG_TRIP_DURATION_RATE": LONG_TRIP_DURATION_RATE,
    "DISTANCE_OUTLIER_RATE": DISTANCE_OUTLIER_RATE,
    "PAYMENT_TIMESTAMP_INCONSISTENCY_RATE": PAYMENT_TIMESTAMP_INCONSISTENCY_RATE,
}


def validate_configuration():
    """Fail early instead of producing a silently unexpected distribution."""
    invalid_rates = {name: value for name, value in DIRTY_DATA_RATES.items() if not 0.0 <= value <= 1.0}
    if invalid_rates:
        raise ValueError(f"Dirty-data rates must be between 0 and 1: {invalid_rates}")
    counts = {
        "N_TRIPS": N_TRIPS,
        "N_PASSENGERS": N_PASSENGERS,
        "N_DRIVERS": N_DRIVERS,
        "N_NEW_DRIVERS_PER_RUN": N_NEW_DRIVERS_PER_RUN,
        "N_DRIVER_UPDATES_PER_RUN": N_DRIVER_UPDATES_PER_RUN,
        "N_NEW_PASSENGERS_PER_RUN": N_NEW_PASSENGERS_PER_RUN,
        "N_PASSENGER_UPDATES_PER_RUN": N_PASSENGER_UPDATES_PER_RUN,
        "N_GDPR_PASSENGER_ERASURES_PER_RUN": N_GDPR_PASSENGER_ERASURES_PER_RUN,
        "N_GDPR_DRIVER_ERASURES_PER_RUN": N_GDPR_DRIVER_ERASURES_PER_RUN,
        "N_GDPR_VEHICLE_ERASURES_PER_RUN": N_GDPR_VEHICLE_ERASURES_PER_RUN,
    }
    invalid_counts = {name: value for name, value in counts.items() if value < 0}
    if invalid_counts:
        raise ValueError(f"Generator counts cannot be negative: {invalid_counts}")
    if not DB_CONFIG["password"]:
        raise ValueError("DB_PASSWORD is required")
    if RANDOM_SEED is not None:
        seed = int(RANDOM_SEED)
        random.seed(seed)
        Faker.seed(seed)


# Setup

fake = Faker()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


# Basic helpers

def maybe_null(value, rate=BROKEN_RATE):
    return None if random.random() < rate else value


def noisy_text(value, *, allow_case=True):
    """Formatting noise as two INDEPENDENT Bernoulli channels: casing and
    surrounding whitespace. A row may coincide in both only by probability."""
    if value is None:
        return None
    result = str(value)
    if allow_case and random.random() < TEXT_FORMAT_CASE_RATE:
        result = random.choice([result.upper(), result.lower()])
    if random.random() < TEXT_FORMAT_PAD_RATE:
        result = f"{' ' * random.randint(1, 3)}{result}{' ' * random.randint(1, 3)}"
    return result


def noisy_email(value):
    """Independent channels: invalid-format, uppercase, whitespace padding."""
    if value is None:
        return None
    result = str(value)
    if random.random() < INVALID_CONTACT_RATE:
        result = random.choice([
            result.replace("@", " at "),
            f"invalid-{uuid.uuid4().hex[:10]}",
            result.split("@")[0] + "@",
        ])
    if random.random() < DQ_RATES["dq_passenger_email_upper"]:
        result = result.upper()
    if random.random() < DQ_RATES["dq_passenger_email_pad"]:
        result = f"{' ' * random.randint(1, 3)}{result}{' ' * random.randint(1, 3)}"
    return result


def noisy_phone(value):
    """Independent channels: invalid-value, whitespace padding."""
    if value is None:
        return None
    result = str(value)
    if random.random() < INVALID_CONTACT_RATE:
        result = random.choice(["N/A", "000", "sin telefono", f"ext {random.randint(1, 99)}"])
    if random.random() < DQ_RATES["dq_passenger_phone_pad"]:
        result = f"{' ' * random.randint(1, 3)}{result}{' ' * random.randint(1, 3)}"
    return result


def noisy_vehicle_type(value):
    variants = {
        "sedan": ["Sedan", " SEDAN ", "saloon"],
        "hatchback": ["Hatchback", "HATCH BACK", " hatch back "],
        "motorbike": ["MotorBike", "motorcycle", "moto", " bike "],
    }
    if random.random() < CATEGORY_VARIANT_RATE:
        return random.choice(variants[value])
    return value


def accidental_contact():
    return random.choice([fake.email(), fake.phone_number(), fake.name()])


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


# Active/soft-delete aware fetch helpers

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


# Noise helpers

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
    """cancel_note channel: presence gate (legacy CANCEL_NOTE_GARBAGE_RATE),
    then formatting anomalies as INDEPENDENT sub-channels via the model.
    PII contamination is its own independent gate."""
    if random.random() > CANCEL_NOTE_GARBAGE_RATE:
        return None

    base = random.choice([
        "No response",
        "Cancelled by mistake",
        "Driver did not arrive",
        "System unavailable",
        "Demasiada demora",
        fake.sentence(nb_words=6),
    ])

    if random.random() < CANCEL_NOTE_PII_RATE:
        base += " | contact: " + accidental_contact()

    return apply_note_dq(base)


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


def apply_trip_dq(plan, driver_ids, vehicle_ids):
    """Independent DQ channels over an already-clean business plan.

    Each anomaly is its own Bernoulli trial on the planned values; none of
    them gates another. Business variability (rush-hour delay, airport fares)
    is NOT handled here — this is only the corrupt-data channel.
    """
    status = plan["status"]
    requested = plan["requested_at"]

    # controlled driver/vehicle mismatch (independent)
    if plan["driver_id"] is not None and random.random() < VEHICLE_DRIVER_MISMATCH_RATE \
            and driver_ids and vehicle_ids:
        plan["driver_id"] = random.choice(driver_ids)
        plan["vehicle_id"] = random.choice(vehicle_ids)

    # fare NULL (independent; feeds Gold imputation + trust metrics)
    if random.random() < DQ_RATES["dq_trip_fare_null"]:
        plan["fare_amount"] = None
    else:
        plan["fare_amount"] = maybe_high_precision(plan["fare_amount"])

    # actual distance: business noise around the estimate, with an
    # independent sensor-outlier channel and the legacy missing channels
    est = plan["estimated_distance_km"]
    if status in ("completed", "started"):
        if random.random() < DISTANCE_OUTLIER_RATE:
            actual = est + random.uniform(12, 60)
        else:
            actual = est * (1.0 + random.gauss(0.02, 0.06))
        actual = max(0.5, round(actual, 2))
        if random.random() < (
            MISSING_DISTANCE_ON_COMPLETED_RATE if status == "completed"
            else MISSING_DISTANCE_ON_STARTED_RATE
        ):
            actual = None
        plan["actual_distance_km"] = actual
    else:
        plan["actual_distance_km"] = (
            max(0.5, round(est * 1.05, 2))
            if random.random() < INVALID_DISTANCE_IN_WRONG_STATUS_RATE else None
        )

    # corrupt-timestamp scenarios (independent)
    if plan["accepted_at"] and random.random() < LONG_ACCEPTANCE_DELAY_RATE:
        new_delay = timedelta(minutes=random.randint(45, 360))
        plan["accepted_at"] = requested + new_delay
        if plan["started_at"]:
            plan["started_at"] = plan["accepted_at"] + timedelta(minutes=random.randint(1, 20))
        if plan["ended_at"]:
            plan["ended_at"] = plan["started_at"] + timedelta(minutes=random.randint(5, 40))
    if plan["started_at"] and random.random() < LONG_TRIP_DURATION_RATE:
        plan["ended_at"] = plan["started_at"] + timedelta(minutes=random.randint(210, 720))

    if random.random() < TIME_WEIRDNESS_RATE:
        if random.random() < 0.5 and plan["accepted_at"] and plan["started_at"]:
            # started before accepted (still >= requested: DB CHECK respected)
            plan["started_at"] = requested + timedelta(minutes=random.randint(1, 5))
            plan["accepted_at"] = requested + timedelta(minutes=random.randint(6, 15))
        else:
            plan["accepted_at"] = requested + timedelta(hours=random.randint(1, 72))
            if plan["started_at"]:
                plan["started_at"] = plan["accepted_at"] + timedelta(minutes=random.randint(1, 20))
            if plan["ended_at"]:
                plan["ended_at"] = plan["started_at"] + timedelta(minutes=random.randint(5, 40))

    if status == "completed":
        if random.random() < MISSING_ENDED_AT_ON_COMPLETED_RATE:
            plan["ended_at"] = None

    return plan


# Accidental PII simulation helpers

def fake_provider_ref():
    if random.random() < PAYMENT_PROVIDER_REF_PII_RATE:
        return "contact:" + accidental_contact()
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

    return noisy_text(base)


# Seed functions (run once)

def seed_passengers(cur, model=None):
    logging.info("Seeding passengers...")
    ids = []
    attempts = 0
    inserted = 0

    while inserted < N_PASSENGERS:
        attempts += 1
        created_dt, _ = model.sample_datetime() if model else (datetime.now(), 0)
        name, email, phone, city = apply_passenger_dq(
            fake.name(), fake.email(), fake.phone_number(), fake.city(),
            incident=(model is not None and in_incident_window(created_dt, model)),
        )
        cur.execute(
            """
            INSERT INTO mobility.passengers (full_name, email, phone, city, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (email) DO NOTHING
            RETURNING passenger_id
            """,
            (
                name,
                email,
                phone,
                city,
                created_dt,
                created_dt + timedelta(minutes=random.uniform(0, 5)),
            ),
        )

        row = cur.fetchone()
        if row:
            ids.append(row[0])
            inserted += 1

        if attempts % 500 == 0:
            logging.info(f"Passengers inserted: {inserted}/{N_PASSENGERS}")

    return ids


def seed_drivers_and_vehicles(cur, model=None):
    """
    Seed drivers and 1 vehicle each, only if empty.
    Clean values first; independent DQ channels after. (drivers/vehicles have
    no created_at columns; passenger history carries the time dimension.)
    """
    logging.info("Seeding drivers and vehicles...")
    driver_ids, vehicle_ids = [], []

    status_choices = (["active", "inactive", "suspended"], [0.70, 0.18, 0.12])
    vehicle_status_choices = (["active", "inactive"], [0.85, 0.15])
    vehicle_type_choices = ["sedan", "hatchback", "motorbike"]

    inserted = 0
    attempts = 0
    max_attempts = max(N_DRIVERS * 50, 2000)

    while inserted < N_DRIVERS:
        attempts += 1
        if attempts > max_attempts:
            raise RuntimeError(f"seed_drivers_and_vehicles: max_attempts reached. Inserted {inserted}/{N_DRIVERS}")

        created_dt, _ = model.sample_datetime() if model else (datetime.now(), 0)

        d_name, d_license = apply_driver_dq(fake.name(), fake.bothify("LIC-#####"))

        # insert driver
        cur.execute(
            """
            INSERT INTO mobility.drivers (full_name, license_number, status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (license_number) DO NOTHING
            RETURNING driver_id
            """,
            (
                d_name,
                d_license,
                random.choices(*status_choices)[0],
                created_dt,
                created_dt,
            ),
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

            plate = apply_vehicle_dq(fake.license_plate())
            year = int(_clamp(random.gauss(2020, 3), 2004, 2025))
            if random.random() < DQ_RATES["dq_vehicle_year_invalid"]:
                year = None  # OLTP CHECK only admits plausible years; missing-year is the DQ scenario
            cur.execute(
                """
                INSERT INTO mobility.vehicles (driver_id, plate_number, vehicle_type, status,
                                               make, model, year, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (plate_number) DO NOTHING
                RETURNING vehicle_id
                """,
                (
                    driver_id,
                    plate,
                    noisy_vehicle_type(random.choice(vehicle_type_choices)),
                    random.choices(*vehicle_status_choices)[0],
                    fake.company(),
                    fake.bothify("Model-##"),
                    year,
                    created_dt,
                    created_dt,
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


# Incremental drivers/vehicles per run

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
        d_name, d_license = apply_driver_dq(fake.name(), fake.bothify("LIC-#####"))

        if "full_name" in drivers_cols:
            d_cols.append("full_name")
            d_vals.append(d_name)

        if "license_number" in drivers_cols:
            d_cols.append("license_number")
            d_vals.append(d_license)

        if "status" in drivers_cols:
            d_cols.append("status")
            d_vals.append(random.choices(status_choices, weights=[0.70, 0.18, 0.12])[0])

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
            v_vals.append(apply_vehicle_dq(fake.license_plate()))

        if "vehicle_type" in vehicles_cols:
            v_cols.append("vehicle_type")
            v_vals.append(noisy_vehicle_type(random.choice(vehicle_type_choices)))

        if "status" in vehicles_cols:
            v_cols.append("status")
            v_vals.append(random.choices(vehicle_status_choices, weights=[0.85, 0.15])[0])

        if "make" in vehicles_cols:
            v_cols.append("make")
            v_vals.append(fake.company())

        if "model" in vehicles_cols:
            v_cols.append("model")
            v_vals.append(fake.bothify("Model-##"))

        if "year" in vehicles_cols:
            _year = int(_clamp(random.gauss(2020, 3), 2004, 2025))
            if random.random() < DQ_RATES["dq_vehicle_year_invalid"]:
                _year = None
            v_cols.append("year")
            v_vals.append(_year)

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
                    v_vals[idx] = apply_vehicle_dq(fake.license_plate())

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
            vals.append(noisy_text(fake.name()))

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


# Incremental passengers per run

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
    duplicate_profiles = []
    if has_email:
        cur.execute(
            """
            SELECT full_name, email, phone, city
            FROM mobility.passengers
            WHERE email IS NOT NULL
              AND COALESCE(is_deleted, FALSE) = FALSE
            ORDER BY random()
            LIMIT 500
            """
        )
        duplicate_profiles = cur.fetchall()

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
        duplicate_profile = None
        if duplicate_profiles and random.random() < DUPLICATE_PASSENGER_RATE:
            duplicate_profile = random.choice(duplicate_profiles)
        profile_name, profile_email, profile_phone, profile_city = (
            duplicate_profile or (fake.name(), fake.email(), fake.phone_number(), fake.city())
        )

        if "full_name" in passengers_cols:
            cols.append("full_name")
            vals.append(noisy_text(profile_name))

        if has_email:
            email_val = None if random.random() < DQ_RATES["dq_passenger_email_null"] else noisy_email(profile_email)
            if duplicate_profile and email_val:
                # PostgreSQL's UNIQUE constraint is case/space sensitive. These
                # variants therefore model duplicated identities without
                # violating the operational schema.
                email_val = f" {profile_email.swapcase()} "
            cols.append("email")
            vals.append(email_val)

        if "phone" in passengers_cols:
            phone_val = None if random.random() < DQ_RATES["dq_passenger_phone_null"] else noisy_phone(profile_phone)
            cols.append("phone")
            vals.append(phone_val)

        if "city" in passengers_cols:
            cols.append("city")
            vals.append(noisy_text(profile_city))

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
            vals.append(noisy_text(fake.name()))

        if "phone" in passengers_cols and random.random() < 0.10:
            sets.append("phone = %s")
            vals.append(maybe_null(noisy_phone(fake.phone_number()), rate=0.15))

        if "city" in passengers_cols and random.random() < 0.10:
            sets.append("city = %s")
            vals.append(maybe_null(noisy_text(fake.city()), rate=0.15))

        if "updated_at" in passengers_cols:
            sets.append("updated_at = now()")

        if not sets:
            continue

        sql = f"UPDATE mobility.passengers SET {', '.join(sets)} WHERE passenger_id = %s"
        vals.append(pid)
        cur.execute(sql, tuple(vals))
        updated += 1

    return updated


# GDPR (OLTP-only) – call your OLTP functions

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


# Incremental inserts

def insert_trips(cur, passenger_ids, driver_ids, vehicle_ids, zone_ids, model):
    logging.info(f"Inserting {N_TRIPS} trips...")
    driver_vehicle_pairs = fetch_driver_vehicle_pairs(cur)
    if not driver_vehicle_pairs:
        raise RuntimeError("No active driver/vehicle pairs found. Seed drivers/vehicles first.")

    trip_ids = []
    seed_int = int(RANDOM_SEED) if RANDOM_SEED else 0

    for i in range(1, N_TRIPS + 1):
        dt, day_idx = model.sample_datetime()
        plan = model.plan_trip(driver_vehicle_pairs, day_idx, dt, seed_int)
        plan = apply_trip_dq(plan, driver_ids, vehicle_ids)

        passenger_id = random.choice(passenger_ids)
        start_lat, start_lng, end_lat, end_lng = generate_coords()

        if plan["status"] == "canceled":
            cancel_note = noisy_cancel_note()
        else:
            cancel_note = noisy_cancel_note() if random.random() < (CANCEL_NOTE_GARBAGE_RATE * 0.1) else None

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
                requested_at_source,
                accepted_at,
                started_at,
                ended_at,

                canceled_at,
                cancel_reason,
                cancel_by,
                cancel_note,

                estimated_distance_km,
                actual_distance_km,
                fare_amount,

                created_at,
                updated_at
            )
            VALUES (
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,
                %s,%s
            )
            RETURNING trip_id
            """,
            (
                passenger_id,
                plan["driver_id"],
                plan["vehicle_id"],
                plan["pickup_zone_id"],
                plan["dropoff_zone_id"],

                start_lat, start_lng, end_lat, end_lng,

                plan["status"],

                plan["requested_at"],
                requested_at_source_text(
                    plan["requested_at"], in_incident_window(plan["requested_at"], model, "trip")
                ),
                plan["accepted_at"],
                plan["started_at"],
                plan["ended_at"],

                plan["canceled_at"],
                plan["cancel_reason"],
                plan["cancel_by"],
                cancel_note,

                plan["estimated_distance_km"],
                plan["actual_distance_km"],
                plan["fare_amount"],

                # business history lives in the event timestamps above; the
                # operational columns stay wall-clock so Bronze's
                # updated_at watermark remains monotonic across runs.
                datetime.now(),
                datetime.now(),
            ),
        )

        trip_ids.append(cur.fetchone()[0])

        if i % LOG_EVERY == 0:
            logging.info(f"{i} trips inserted...")

    return trip_ids


def insert_payments(cur, trip_ids, model):
    logging.info("Inserting payments...")

    payments_cols = get_table_columns(cur, "payments")
    has_provider_ref = "provider_ref" in payments_cols

    cur.execute(
        """
        SELECT trip_id, status, requested_at, ended_at, fare_amount
        FROM mobility.trips
        WHERE trip_id = ANY(%s)
        """,
        (trip_ids,),
    )

    insert_sql = None
    for trip_id, status, requested_at, ended_at, fare_amount in cur.fetchall():
        pp = model.payment_plan({
            "status": status,
            "requested_at": requested_at,
            "ended_at": ended_at,
            "fare_amount": float(fare_amount) if fare_amount is not None else None,
        })
        if pp is None:
            continue

        provider_ref = None
        if has_provider_ref and random.random() < PAYMENT_PROVIDER_REF_RATE:
            provider_ref = fake_provider_ref()

        currency = pp["currency"]
        if random.random() < CATEGORY_VARIANT_RATE:
            currency = random.choice(["usd", "Usd", "US$"])

        paid_at = pp["paid_at"]
        if random.random() < PAYMENT_TIMESTAMP_INCONSISTENCY_RATE:
            # corrupt-timestamp channel: paid without paid_at / pending with one
            paid_at = (ended_at or requested_at) if pp["status"] != "paid" else None

        columns = ["trip_id", "method", "status", "amount", "currency", "paid_at",
                   "created_at", "updated_at"]
        anchor = ended_at or requested_at or datetime.now()
        # business reality lives in paid_at/amount; operational columns stay
        # wall-clock so the Bronze updated_at watermark stays monotonic.
        values = [trip_id, pp["method"], pp["status"], pp["amount"], currency, paid_at,
                  datetime.now(), datetime.now()]
        if has_provider_ref:
            columns.append("provider_ref")
            values.append(provider_ref)
        placeholders = ",".join(["%s"] * len(columns))
        insert_sql = f"INSERT INTO mobility.payments ({','.join(columns)}) VALUES ({placeholders})"
        cur.execute(insert_sql, tuple(values))

        # Gateway retries commonly create a second operational row with
        # the same non-null provider reference. Silver identifies the
        # canonical payment and Gold excludes the retry.
        if provider_ref and random.random() < DUPLICATE_PAYMENT_RATE:
            cur.execute(insert_sql, tuple(values))


def insert_ratings(cur, trip_ids, model):
    logging.info("Inserting ratings...")

    ratings_cols = get_table_columns(cur, "ratings")
    has_comment = "comment" in ratings_cols
    seed_int = int(RANDOM_SEED) if RANDOM_SEED else 0

    cur.execute(
        """
        SELECT t.trip_id, t.passenger_id, t.driver_id, t.requested_at, t.accepted_at,
               t.started_at, t.ended_at, v.vehicle_type
        FROM mobility.trips t
        LEFT JOIN mobility.vehicles v ON v.vehicle_id = t.vehicle_id
        WHERE t.trip_id = ANY(%s)
          AND t.driver_id IS NOT NULL
          AND t.passenger_id IS NOT NULL
          AND t.status = 'completed'
        """,
        (trip_ids,),
    )
    eligible = cur.fetchall()

    if not eligible:
        logging.info("No eligible trips for ratings (no completed trips with driver).")
        return

    rated = random.sample(eligible, k=int(len(eligible) * 0.6))

    for trip_id, passenger_id, driver_id, requested_at, accepted_at, started_at, ended_at, vehicle_type in rated:
        delay_min = (
            (accepted_at - requested_at).total_seconds() / 60.0
            if accepted_at and requested_at else 0.0
        )
        duration_min = (
            (ended_at - started_at).total_seconds() / 60.0
            if ended_at and started_at else 20.0
        )
        plan_like = {
            "quality": model.driver_quality(driver_id, seed_int),
            "delay_min": float(delay_min),
            "duration_min": float(duration_min),
            "requested_at": requested_at,
            "vehicle_type": vehicle_type,
        }
        score = model.rating_score(plan_like)
        # noisy_rating_comment already applies presence + independent PII channels
        comment = noisy_rating_comment() if has_comment else None
        if has_comment:
            cur.execute(
                """
                INSERT INTO mobility.ratings (trip_id, passenger_id, driver_id, score, comment)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (trip_id, passenger_id, driver_id, score, comment),
            )
        else:
            cur.execute(
                """
                INSERT INTO mobility.ratings (trip_id, passenger_id, driver_id, score)
                VALUES (%s, %s, %s, %s)
                """,
                (trip_id, passenger_id, driver_id, score),
            )


def _now_like(dt):
    """now() sharing dt's awareness/timezone, for safe subtraction."""
    if dt is not None and getattr(dt, "tzinfo", None) is not None:
        return datetime.now(dt.tzinfo)
    return datetime.now()


def update_trip_statuses(cur, max_updates=3000):
    logging.info("Updating existing trips statuses...")

    cur.execute(
        """
        SELECT trip_id, status, started_at, accepted_at, requested_at, estimated_distance_km
        FROM mobility.trips
        WHERE status IN ('requested','accepted','started')
          AND requested_at < now() - interval '2 days'
        ORDER BY random()
        LIMIT %s
        """,
        (max_updates,),
    )

    trips = cur.fetchall()

    for trip_id, t_status, started_at, accepted_at, requested_at, estimated_distance in trips:
        delay_min = (
            (accepted_at - requested_at).total_seconds() / 60.0
            if accepted_at and requested_at else 0.0
        )
        if t_status == "requested":
            delay_min = (_now_like(requested_at) - requested_at).total_seconds() / 60.0 if requested_at else 0.0

        p_cancel = _clamp(0.05 + 0.11 * max(0.0, delay_min - 8.0) / 40.0, 0.03, 0.55)
        new_status = "canceled" if random.random() < p_cancel else "completed"

        if new_status == "completed":
            base = started_at or requested_at or datetime.now()
            ended_at = base + timedelta(minutes=random.uniform(5, 45))

            raw_actual = (
                float(estimated_distance) * (1.0 + random.gauss(0.02, 0.06))
                if estimated_distance else None
            )
            if raw_actual is not None and random.random() < DISTANCE_OUTLIER_RATE:
                raw_actual += random.uniform(12, 60)
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
            canceled_at = (requested_at or datetime.now()) + timedelta(minutes=random.uniform(1, 20))

            raw_candidate = (
                float(estimated_distance) * 1.05 if estimated_distance else None
            )
            if raw_candidate is not None and random.random() < INVALID_DISTANCE_IN_WRONG_STATUS_RATE:
                actual_distance = maybe_high_precision(raw_candidate)
            else:
                actual_distance = None

            if accepted_at is None:
                cancel_by = cancel_reason = random.choices(["passenger", "system"], weights=[0.75, 0.25])[0]
            else:
                cancel_by = random.choices(["driver", "system", "passenger"], weights=[0.45, 0.35, 0.20])[0]
                cancel_reason = random.choices(["driver", "system", "passenger"], weights=[0.40, 0.40, 0.20])[0]

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
                    cancel_reason,
                    cancel_by,
                    noisy_cancel_note(),
                    actual_distance,
                    trip_id,
                ),
            )

    logging.info(f"Updated {len(trips)} trips")


# Main

def main():
    validate_configuration()
    logging.info("Starting OLTP data generation (PLUS GDPR)")
    logging.info("Intentional dirty-data profile: %s", DIRTY_DATA_RATES)

    conn = get_connection()
    cur = conn.cursor()

    try:
        if "requested_at_source" not in get_table_columns(cur, "trips"):
            raise RuntimeError(
                "Apply db/migrations/004_requested_at_source.sql before generating trips."
            )
        zone_ids = fetch_ids(cur, "zones", "zone_id")
        if not zone_ids:
            raise RuntimeError("No zones found. Seed zones before running generator.")

        model = MobilityModel(zone_ids)

        # PASSENGERS (seed/grow/update)
        passenger_ids = fetch_active_passenger_ids(cur)
        if not passenger_ids:
            _all_passenger_ids = fetch_ids(cur, "passengers", "passenger_id")
            if not _all_passenger_ids:
                passenger_ids = seed_passengers(cur, model)
            else:
                passenger_ids = fetch_active_passenger_ids(cur)

        new_passenger_ids = insert_new_passengers(cur, N_NEW_PASSENGERS_PER_RUN)
        if new_passenger_ids:
            logging.info(f"New passengers inserted this run: {len(new_passenger_ids)}")
            passenger_ids.extend(new_passenger_ids)

        n_p_updated = update_existing_passengers(cur, N_PASSENGER_UPDATES_PER_RUN)
        logging.info(f"Passengers updated this run: {n_p_updated}")

        # DRIVERS + VEHICLES (seed/grow/update)
        driver_ids = fetch_active_driver_ids(cur)
        vehicle_ids = fetch_active_vehicle_ids(cur)
        if not driver_ids or not vehicle_ids:
            driver_ids, vehicle_ids = seed_drivers_and_vehicles(cur, model)
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

        # CORE ACTIVITY
        if not passenger_ids:
            raise RuntimeError("No active passengers available for trips.")
        trip_ids = insert_trips(cur, passenger_ids, driver_ids, vehicle_ids, zone_ids, model)

        insert_payments(cur, trip_ids, model)
        logging.info("Payments inserted")

        insert_ratings(cur, trip_ids, model)
        logging.info("Ratings inserted")

        update_trip_statuses(cur, max_updates=3000)

        # GDPR erasure simulation sometimes
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
