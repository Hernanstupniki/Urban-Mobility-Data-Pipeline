-- Analytics (serving) database bootstrap for mobility_dw.
-- Schemas are also ensured idempotently by src/common/publish.py; this file
-- only guarantees the structure exists right after the first volume init.

CREATE SCHEMA IF NOT EXISTS reporting;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS control;

CREATE TABLE IF NOT EXISTS control.publish_state (
    table_name text PRIMARY KEY,
    last_batch_id text,
    last_row_count bigint,
    source_row_count bigint,
    last_successful_publish timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS control.publish_log (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id text NOT NULL,
    table_name text NOT NULL,
    status text NOT NULL,
    source_row_count bigint,
    published_row_count bigint,
    message text,
    created_at timestamptz NOT NULL DEFAULT now()
);
