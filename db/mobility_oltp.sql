-- ============================================================
-- Urban Mobility OLTP (PostgreSQL) - Schema (GDPR extended)
-- Schema: mobility
-- Idempotent: safe to re-run
-- ============================================================

-- 0) Schema
CREATE SCHEMA IF NOT EXISTS mobility;
SET search_path TO mobility;

-- ------------------------------------------------------------
-- 1) Types (enums)
-- ------------------------------------------------------------
DO $$ BEGIN
  CREATE TYPE driver_status AS ENUM ('active','inactive','suspended');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE vehicle_status AS ENUM ('active','inactive');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE trip_status AS ENUM ('requested','accepted','started','completed','canceled');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE payment_method AS ENUM ('cash','card','wallet','bank_transfer');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE payment_status AS ENUM ('pending','authorized','paid','failed','refunded');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE cancellation_reason AS ENUM ('passenger','driver','system','no_show','other');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- GDPR request log type
DO $$ BEGIN
  CREATE TYPE gdpr_request_type AS ENUM ('erasure','access','rectification');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- GDPR subject type (NEW)
DO $$ BEGIN
  CREATE TYPE gdpr_subject_type AS ENUM ('passenger','driver','vehicle');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- ------------------------------------------------------------
-- 2) Core tables
-- ------------------------------------------------------------

-- Passengers (PII allowed in OLTP)
CREATE TABLE IF NOT EXISTS passengers (
  passenger_id        BIGSERIAL PRIMARY KEY,
  full_name           TEXT NOT NULL,
  email               TEXT UNIQUE,
  phone               TEXT,
  city                TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  is_deleted          BOOLEAN NOT NULL DEFAULT FALSE,
  deleted_at          TIMESTAMPTZ
);

-- Drivers (PII allowed: full_name, license_number)
CREATE TABLE IF NOT EXISTS drivers (
  driver_id           BIGSERIAL PRIMARY KEY,
  full_name           TEXT NOT NULL,
  license_number      TEXT UNIQUE,
  status              driver_status NOT NULL DEFAULT 'active',
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  is_deleted          BOOLEAN NOT NULL DEFAULT FALSE,
  deleted_at          TIMESTAMPTZ
);

-- Vehicles (PII strong identifier: plate_number)
CREATE TABLE IF NOT EXISTS vehicles (
  vehicle_id          BIGSERIAL PRIMARY KEY,
  driver_id           BIGINT NOT NULL REFERENCES drivers(driver_id),
  plate_number        TEXT NOT NULL UNIQUE,
  vehicle_type        TEXT NOT NULL,                 -- e.g., "sedan", "motorbike"
  make                TEXT,
  model               TEXT,
  year                INT CHECK (year IS NULL OR (year >= 1980 AND year <= EXTRACT(YEAR FROM now())::INT + 1)),
  status              vehicle_status NOT NULL DEFAULT 'active',
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- (NEW) Soft-delete flags for vehicles to align with GDPR flows (idempotent)
ALTER TABLE mobility.vehicles
  ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE mobility.vehicles
  ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

-- Zones
CREATE TABLE IF NOT EXISTS zones (
  zone_id             BIGSERIAL PRIMARY KEY,
  zone_name           TEXT NOT NULL,
  city                TEXT,
  region              TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Trips (may contain accidental PII in cancel_note)
CREATE TABLE IF NOT EXISTS trips (
  trip_id             BIGSERIAL PRIMARY KEY,

  passenger_id        BIGINT NOT NULL REFERENCES passengers(passenger_id),
  driver_id           BIGINT REFERENCES drivers(driver_id),
  vehicle_id          BIGINT REFERENCES vehicles(vehicle_id),

  pickup_zone_id      BIGINT REFERENCES zones(zone_id),
  dropoff_zone_id     BIGINT REFERENCES zones(zone_id),

  start_lat           NUMERIC(9,6),
  start_lng           NUMERIC(9,6),
  end_lat             NUMERIC(9,6),
  end_lng             NUMERIC(9,6),

  status              trip_status NOT NULL DEFAULT 'requested',

  requested_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  accepted_at         TIMESTAMPTZ,
  started_at          TIMESTAMPTZ,
  ended_at            TIMESTAMPTZ,

  canceled_at         TIMESTAMPTZ,
  cancel_reason       cancellation_reason,
  cancel_by           cancellation_reason,  -- who triggered (passenger/driver/system/other)
  cancel_note         TEXT,                 -- may contain accidental PII

  estimated_distance_km NUMERIC(10,3) CHECK (estimated_distance_km IS NULL OR estimated_distance_km >= 0),
  actual_distance_km    NUMERIC(10,3) CHECK (actual_distance_km IS NULL OR actual_distance_km >= 0),
  fare_amount           NUMERIC(12,2) CHECK (fare_amount IS NULL OR fare_amount >= 0),

  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT trips_time_order_chk CHECK (
    (accepted_at IS NULL OR accepted_at >= requested_at)
    AND (started_at  IS NULL OR started_at  >= requested_at)
    AND (ended_at    IS NULL OR ended_at    >= requested_at)
  )
);

-- Payments (provider_ref may be a gateway identifier)
CREATE TABLE IF NOT EXISTS payments (
  payment_id          BIGSERIAL PRIMARY KEY,
  trip_id             BIGINT NOT NULL REFERENCES trips(trip_id),
  method              payment_method NOT NULL,
  status              payment_status NOT NULL DEFAULT 'pending',
  amount              NUMERIC(12,2) NOT NULL CHECK (amount >= 0),
  currency            CHAR(3) NOT NULL DEFAULT 'USD',
  provider_ref        TEXT, -- gateway reference id (potential identifier)
  paid_at             TIMESTAMPTZ,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Ratings (comment may contain accidental PII)
CREATE TABLE IF NOT EXISTS ratings (
  rating_id           BIGSERIAL PRIMARY KEY,
  trip_id             BIGINT NOT NULL UNIQUE REFERENCES trips(trip_id),
  passenger_id        BIGINT NOT NULL REFERENCES passengers(passenger_id),
  driver_id           BIGINT NOT NULL REFERENCES drivers(driver_id),
  score               SMALLINT NOT NULL CHECK (score BETWEEN 1 AND 5),
  comment             TEXT,  -- may contain accidental PII
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- GDPR request log (extended: generic subject)
CREATE TABLE IF NOT EXISTS gdpr_requests (
  request_id          BIGSERIAL PRIMARY KEY,

  -- legacy (kept for backward compatibility)
  passenger_id        BIGINT REFERENCES passengers(passenger_id),

  -- NEW (generic subject)
  subject_type        gdpr_subject_type,
  subject_id          BIGINT,

  request_type        gdpr_request_type NOT NULL,
  requested_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  processed_at        TIMESTAMPTZ,
  status              TEXT NOT NULL DEFAULT 'pending', -- pending/processed/rejected
  note                TEXT
);

-- If table existed previously, ensure new columns exist (idempotent)
ALTER TABLE mobility.gdpr_requests
  ADD COLUMN IF NOT EXISTS subject_type gdpr_subject_type;

ALTER TABLE mobility.gdpr_requests
  ADD COLUMN IF NOT EXISTS subject_id BIGINT;

-- ------------------------------------------------------------
-- 3) Seed zones (idempotent)
-- ------------------------------------------------------------
INSERT INTO zones (zone_name, city, region)
VALUES
  ('Manhattan', 'New York', 'NY'),
  ('Brooklyn', 'New York', 'NY'),
  ('Queens', 'New York', 'NY'),
  ('Bronx', 'New York', 'NY'),
  ('Staten Island', 'New York', 'NY'),

  ('Downtown', 'Los Angeles', 'CA'),
  ('Hollywood', 'Los Angeles', 'CA'),
  ('Santa Monica', 'Los Angeles', 'CA'),
  ('Venice', 'Los Angeles', 'CA'),

  ('Loop', 'Chicago', 'IL'),
  ('Hyde Park', 'Chicago', 'IL'),
  ('Lincoln Park', 'Chicago', 'IL'),

  ('Downtown', 'San Francisco', 'CA'),
  ('Mission District', 'San Francisco', 'CA'),
  ('SoMa', 'San Francisco', 'CA'),

  ('Downtown', 'Austin', 'TX'),
  ('South Congress', 'Austin', 'TX'),
  ('East Austin', 'Austin', 'TX'),

  ('Downtown', 'Miami', 'FL'),
  ('Brickell', 'Miami', 'FL'),
  ('Wynwood', 'Miami', 'FL')
ON CONFLICT DO NOTHING;

-- ------------------------------------------------------------
-- 4) Indexes
-- ------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_trips_requested_at   ON trips(requested_at);
CREATE INDEX IF NOT EXISTS idx_trips_updated_at     ON trips(updated_at);
CREATE INDEX IF NOT EXISTS idx_trips_status         ON trips(status);
CREATE INDEX IF NOT EXISTS idx_trips_driver         ON trips(driver_id);
CREATE INDEX IF NOT EXISTS idx_trips_passenger      ON trips(passenger_id);
CREATE INDEX IF NOT EXISTS idx_trips_vehicle        ON trips(vehicle_id);

CREATE INDEX IF NOT EXISTS idx_payments_trip        ON payments(trip_id);
CREATE INDEX IF NOT EXISTS idx_payments_updated_at  ON payments(updated_at);

CREATE INDEX IF NOT EXISTS idx_ratings_driver       ON ratings(driver_id);
CREATE INDEX IF NOT EXISTS idx_ratings_passenger    ON ratings(passenger_id);

CREATE INDEX IF NOT EXISTS idx_passengers_updated   ON passengers(updated_at);
CREATE INDEX IF NOT EXISTS idx_drivers_updated      ON drivers(updated_at);
CREATE INDEX IF NOT EXISTS idx_vehicles_updated     ON vehicles(updated_at);
CREATE INDEX IF NOT EXISTS idx_vehicles_driver      ON vehicles(driver_id);

CREATE INDEX IF NOT EXISTS idx_gdpr_subject         ON gdpr_requests(subject_type, subject_id);
CREATE INDEX IF NOT EXISTS idx_gdpr_requested_at    ON gdpr_requests(requested_at);

-- ------------------------------------------------------------
-- 5) updated_at auto-maintenance (trigger)
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Base triggers (idempotent)
DO $$ BEGIN
  CREATE TRIGGER trg_passengers_updated
  BEFORE UPDATE ON passengers
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TRIGGER trg_drivers_updated
  BEFORE UPDATE ON drivers
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TRIGGER trg_vehicles_updated
  BEFORE UPDATE ON vehicles
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TRIGGER trg_trips_updated
  BEFORE UPDATE ON trips
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TRIGGER trg_payments_updated
  BEFORE UPDATE ON payments
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TRIGGER trg_ratings_updated
  BEFORE UPDATE ON ratings
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- ------------------------------------------------------------
-- 6) Helpful view (optional)
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW v_trip_kpis AS
SELECT
  t.trip_id,
  t.status,
  t.requested_at,
  t.accepted_at,
  t.started_at,
  t.ended_at,
  EXTRACT(EPOCH FROM (t.accepted_at - t.requested_at))::BIGINT AS wait_time_sec,
  EXTRACT(EPOCH FROM (t.ended_at   - t.started_at))::BIGINT   AS trip_duration_sec,
  t.fare_amount,
  t.actual_distance_km
FROM trips t;

-- ------------------------------------------------------------
-- 7) GDPR helpers + erasure functions (PASSENGER / DRIVER / VEHICLE)
--    Notes:
--    - We DO NOT delete rows (keep FK integrity).
--    - We anonymize direct PII fields.
--    - We scrub "accidental PII" fields:
--        ratings.comment, trips.cancel_note, payments.provider_ref
--    - vehicles.plate_number is NOT NULL + UNIQUE => we set a unique placeholder.
-- ------------------------------------------------------------

-- Helper: generate a unique, deterministic anonymized plate (NOT NULL + UNIQUE safe)
CREATE OR REPLACE FUNCTION mobility.gdpr_vehicle_plate_placeholder(p_vehicle_id BIGINT)
RETURNS TEXT
LANGUAGE sql
AS $$
  SELECT 'ANON-PLATE-' || p_vehicle_id::text;
$$;

-- PASSENGER ERASURE (full: passengers + accidental PII in related tables)
CREATE OR REPLACE FUNCTION mobility.gdpr_erasure_passenger(
  p_passenger_id BIGINT,
  p_note TEXT DEFAULT NULL
)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_request_id BIGINT;
  v_exists BOOLEAN;
BEGIN
  SELECT EXISTS(
    SELECT 1 FROM mobility.passengers WHERE passenger_id = p_passenger_id
  ) INTO v_exists;

  IF NOT v_exists THEN
    INSERT INTO mobility.gdpr_requests (passenger_id, subject_type, subject_id, request_type, requested_at, processed_at, status, note)
    VALUES (p_passenger_id, 'passenger', p_passenger_id, 'erasure', now(), now(), 'rejected', COALESCE(p_note,'passenger_not_found'))
    RETURNING request_id INTO v_request_id;

    RETURN v_request_id;
  END IF;

  INSERT INTO mobility.gdpr_requests (passenger_id, subject_type, subject_id, request_type, requested_at, status, note)
  VALUES (p_passenger_id, 'passenger', p_passenger_id, 'erasure', now(), 'pending', p_note)
  RETURNING request_id INTO v_request_id;

  -- Direct PII
  UPDATE mobility.passengers
  SET
    full_name  = 'ANONYMIZED',
    email      = NULL,
    phone      = NULL,
    city       = NULL,
    is_deleted = TRUE,
    deleted_at = now()
  WHERE passenger_id = p_passenger_id;

  -- Accidental PII: ratings / trips / payments
  UPDATE mobility.ratings
  SET comment = NULL
  WHERE passenger_id = p_passenger_id
    AND comment IS NOT NULL;

  UPDATE mobility.trips
  SET cancel_note = NULL
  WHERE passenger_id = p_passenger_id
    AND cancel_note IS NOT NULL;

  UPDATE mobility.payments p
  SET provider_ref = NULL
  FROM mobility.trips t
  WHERE p.trip_id = t.trip_id
    AND t.passenger_id = p_passenger_id
    AND p.provider_ref IS NOT NULL;

  -- Mark processed
  UPDATE mobility.gdpr_requests
  SET processed_at = now(),
      status = 'processed'
  WHERE request_id = v_request_id;

  RETURN v_request_id;
END;
$$;

-- Backward-compatible wrapper (keeps your old name)
CREATE OR REPLACE FUNCTION mobility.gdpr_anonymize_passenger(
  p_passenger_id BIGINT,
  p_note TEXT DEFAULT NULL
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
  PERFORM mobility.gdpr_erasure_passenger(p_passenger_id, p_note);
END;
$$;

-- DRIVER ERASURE (drivers + license + vehicles plates + accidental PII)
CREATE OR REPLACE FUNCTION mobility.gdpr_erasure_driver(
  p_driver_id BIGINT,
  p_note TEXT DEFAULT NULL
)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_request_id BIGINT;
  v_exists BOOLEAN;
BEGIN
  SELECT EXISTS(
    SELECT 1 FROM mobility.drivers WHERE driver_id = p_driver_id
  ) INTO v_exists;

  IF NOT v_exists THEN
    INSERT INTO mobility.gdpr_requests (subject_type, subject_id, request_type, requested_at, processed_at, status, note)
    VALUES ('driver', p_driver_id, 'erasure', now(), now(), 'rejected', COALESCE(p_note,'driver_not_found'))
    RETURNING request_id INTO v_request_id;

    RETURN v_request_id;
  END IF;

  INSERT INTO mobility.gdpr_requests (subject_type, subject_id, request_type, requested_at, status, note)
  VALUES ('driver', p_driver_id, 'erasure', now(), 'pending', p_note)
  RETURNING request_id INTO v_request_id;

  -- Direct PII (driver)
  UPDATE mobility.drivers
  SET
    full_name       = 'ANONYMIZED',
    license_number  = NULL,
    status          = 'inactive',
    is_deleted      = TRUE,
    deleted_at      = now()
  WHERE driver_id = p_driver_id;

  -- Vehicle strong identifier (plate) + soft-delete the vehicle row (keep FK integrity)
  UPDATE mobility.vehicles
  SET
    plate_number = mobility.gdpr_vehicle_plate_placeholder(vehicle_id),
    is_deleted   = TRUE,
    deleted_at   = now()
  WHERE driver_id = p_driver_id;

  -- Accidental PII: ratings / trips / payments
  UPDATE mobility.ratings
  SET comment = NULL
  WHERE driver_id = p_driver_id
    AND comment IS NOT NULL;

  UPDATE mobility.trips
  SET cancel_note = NULL
  WHERE driver_id = p_driver_id
    AND cancel_note IS NOT NULL;

  UPDATE mobility.payments p
  SET provider_ref = NULL
  FROM mobility.trips t
  WHERE p.trip_id = t.trip_id
    AND t.driver_id = p_driver_id
    AND p.provider_ref IS NOT NULL;

  -- Mark processed
  UPDATE mobility.gdpr_requests
  SET processed_at = now(),
      status = 'processed'
  WHERE request_id = v_request_id;

  RETURN v_request_id;
END;
$$;

-- VEHICLE ERASURE (plate + accidental PII in trips/payments)
CREATE OR REPLACE FUNCTION mobility.gdpr_erasure_vehicle(
  p_vehicle_id BIGINT,
  p_note TEXT DEFAULT NULL
)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_request_id BIGINT;
  v_exists BOOLEAN;
BEGIN
  SELECT EXISTS(
    SELECT 1 FROM mobility.vehicles WHERE vehicle_id = p_vehicle_id
  ) INTO v_exists;

  IF NOT v_exists THEN
    INSERT INTO mobility.gdpr_requests (subject_type, subject_id, request_type, requested_at, processed_at, status, note)
    VALUES ('vehicle', p_vehicle_id, 'erasure', now(), now(), 'rejected', COALESCE(p_note,'vehicle_not_found'))
    RETURNING request_id INTO v_request_id;

    RETURN v_request_id;
  END IF;

  INSERT INTO mobility.gdpr_requests (subject_type, subject_id, request_type, requested_at, status, note)
  VALUES ('vehicle', p_vehicle_id, 'erasure', now(), 'pending', p_note)
  RETURNING request_id INTO v_request_id;

  -- Direct PII (vehicle plate)
  UPDATE mobility.vehicles
  SET
    plate_number = mobility.gdpr_vehicle_plate_placeholder(vehicle_id),
    is_deleted   = TRUE,
    deleted_at   = now()
  WHERE vehicle_id = p_vehicle_id;

  -- Accidental PII: trips / payments for trips using this vehicle
  UPDATE mobility.trips
  SET cancel_note = NULL
  WHERE vehicle_id = p_vehicle_id
    AND cancel_note IS NOT NULL;

  UPDATE mobility.payments p
  SET provider_ref = NULL
  FROM mobility.trips t
  WHERE p.trip_id = t.trip_id
    AND t.vehicle_id = p_vehicle_id
    AND p.provider_ref IS NOT NULL;

  -- Mark processed
  UPDATE mobility.gdpr_requests
  SET processed_at = now(),
      status = 'processed'
  WHERE request_id = v_request_id;

  RETURN v_request_id;
END;
$$;

-- Done
