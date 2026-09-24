-- Preserve the source timestamp text alongside the typed event time.
ALTER TABLE mobility.trips ADD COLUMN IF NOT EXISTS requested_at_source TEXT;
