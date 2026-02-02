-- ============================================================
-- Urban Mobility OLTP (PostgreSQL) - Schema (CONSISTENTE)
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

-- Drivers
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

-- Vehicles
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

-- Zones
CREATE TABLE IF NOT EXISTS zones (
  zone_id             BIGSERIAL PRIMARY KEY,
  zone_name           TEXT NOT NULL,
  city                TEXT,
  region              TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Trips
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
  cancel_note         TEXT,

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

-- Payments
CREATE TABLE IF NOT EXISTS payments (
  payment_id          BIGSERIAL PRIMARY KEY,
  trip_id             BIGINT NOT NULL REFERENCES trips(trip_id),
  method              payment_method NOT NULL,
  status              payment_status NOT NULL DEFAULT 'pending',
  amount              NUMERIC(12,2) NOT NULL CHECK (amount >= 0),
  currency            CHAR(3) NOT NULL DEFAULT 'USD',
  provider_ref        TEXT, -- gateway reference id
  paid_at             TIMESTAMPTZ,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Ratings (usually after completion; 1 rating per trip)
CREATE TABLE IF NOT EXISTS ratings (
  rating_id           BIGSERIAL PRIMARY KEY,
  trip_id             BIGINT NOT NULL UNIQUE REFERENCES trips(trip_id),
  passenger_id        BIGINT NOT NULL REFERENCES passengers(passenger_id),
  driver_id           BIGINT NOT NULL REFERENCES drivers(driver_id),
  score               SMALLINT NOT NULL CHECK (score BETWEEN 1 AND 5),
  comment             TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- GDPR request log
CREATE TABLE IF NOT EXISTS gdpr_requests (
  request_id          BIGSERIAL PRIMARY KEY,
  passenger_id        BIGINT REFERENCES passengers(passenger_id),
  request_type        gdpr_request_type NOT NULL,
  requested_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  processed_at        TIMESTAMPTZ,
  status              TEXT NOT NULL DEFAULT 'pending', -- pending/processed/rejected
  note                TEXT
);

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

CREATE INDEX IF NOT EXISTS idx_payments_trip        ON payments(trip_id);
CREATE INDEX IF NOT EXISTS idx_payments_updated_at  ON payments(updated_at);

CREATE INDEX IF NOT EXISTS idx_ratings_driver       ON ratings(driver_id);

CREATE INDEX IF NOT EXISTS idx_passengers_updated   ON passengers(updated_at);
CREATE INDEX IF NOT EXISTS idx_drivers_updated      ON drivers(updated_at);

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

-- ------------------------------------------------------------
-- 6) Ratings: add updated_at + trigger (TU CAMBIO)
-- ------------------------------------------------------------
-- trigger for ratings (idempotent)
DO $$ BEGIN
  CREATE TRIGGER trg_ratings_updated
  BEFORE UPDATE ON ratings
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- ------------------------------------------------------------
-- 7) Helpful view (optional)
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
-- 8) MINIMO GDPR en OLTP (anonimizar pasajero)
--    - No borra trips/payments/ratings (no rompe FKs)
--    - Marca is_deleted/deleted_at y pisa PII en passengers
--    - Registra la solicitud en gdpr_requests
-- ------------------------------------------------------------

-- helper: function to anonymize a passenger and mark gdpr request processed
CREATE OR REPLACE FUNCTION mobility.gdpr_anonymize_passenger(p_passenger_id BIGINT, p_note TEXT DEFAULT NULL)
RETURNS VOID AS $$
BEGIN
  -- 1) create request (erasure) as pending
  INSERT INTO mobility.gdpr_requests (passenger_id, request_type, status, note)
  VALUES (p_passenger_id, 'erasure', 'pending', p_note);

  -- 2) anonymize passenger record (keep row for FK integrity)
  UPDATE mobility.passengers
  SET
    full_name  = '[deleted]',
    email      = NULL,
    phone      = NULL,
    city       = NULL,
    is_deleted = TRUE,
    deleted_at = now(),
    updated_at = now()
  WHERE passenger_id = p_passenger_id;

  -- 3) mark latest request as processed
  UPDATE mobility.gdpr_requests
  SET status = 'processed', processed_at = now()
  WHERE request_id = (
    SELECT request_id
    FROM mobility.gdpr_requests
    WHERE passenger_id = p_passenger_id
      AND request_type = 'erasure'
    ORDER BY requested_at DESC
    LIMIT 1
  );
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- Minimal GDPR mechanism (OLTP) - anonymize + log request
-- Keeps referential integrity (no deletes)
-- ============================================================

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
  -- Check passenger exists
  SELECT EXISTS(
    SELECT 1 FROM mobility.passengers WHERE passenger_id = p_passenger_id
  )
  INTO v_exists;

  IF NOT v_exists THEN
    INSERT INTO mobility.gdpr_requests (passenger_id, request_type, requested_at, processed_at, status, note)
    VALUES (p_passenger_id, 'erasure', now(), now(), 'rejected', COALESCE(p_note,'passenger_not_found'))
    RETURNING request_id INTO v_request_id;

    RETURN v_request_id;
  END IF;

  -- Log request as pending
  INSERT INTO mobility.gdpr_requests (passenger_id, request_type, requested_at, status, note)
  VALUES (p_passenger_id, 'erasure', now(), 'pending', p_note)
  RETURNING request_id INTO v_request_id;

  -- Anonymize passenger PII (DO NOT DELETE ROW)
  -- full_name is NOT NULL -> keep placeholder.
  -- email is UNIQUE but allows NULL -> OK.
  UPDATE mobility.passengers
  SET
    full_name  = 'ANONYMIZED',
    email      = NULL,
    phone      = NULL,
    city       = NULL,
    is_deleted = TRUE,
    deleted_at = now()
  WHERE passenger_id = p_passenger_id;

  -- updated_at gets set by your trigger set_updated_at()

  -- Mark request as processed
  UPDATE mobility.gdpr_requests
  SET processed_at = now(),
      status = 'processed'
  WHERE request_id = v_request_id;

  RETURN v_request_id;
END;
$$;


-- Done
