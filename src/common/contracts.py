"""Versioned column contracts shared by Silver, Gold, GDPR and tests."""

from __future__ import annotations


DIMENSION_KEYS = {
    "passenger": "passenger_id",
    "driver": "driver_id",
    "vehicle": "vehicle_id",
}

DIMENSION_COLUMNS = {
    "passenger": [
        "passenger_id", "full_name", "email", "phone", "city", "is_deleted", "deleted_at",
        "created_at", "updated_at", "missing_full_name", "missing_email", "missing_phone",
        "invalid_email_format", "invalid_phone_format", "canonical_passenger_id",
        "potential_duplicate_passenger", "source_system", "batch_id", "raw_loaded_at",
    ],
    "driver": [
        "driver_id", "full_name", "license_number", "status", "is_deleted", "deleted_at",
        "created_at", "updated_at", "missing_full_name", "missing_license_number", "invalid_status",
        "source_system", "batch_id", "raw_loaded_at",
    ],
    "vehicle": [
        "vehicle_id", "driver_id", "plate_number", "vehicle_type", "make", "model", "year", "status",
        "is_deleted", "deleted_at", "created_at", "updated_at", "missing_plate_number",
        "missing_vehicle_type", "invalid_vehicle_type", "vehicle_type_was_normalized",
        "missing_driver_id", "invalid_year", "invalid_status",
        "source_system", "batch_id", "raw_loaded_at",
    ],
}

PII_COLUMNS = {
    "passenger": ["full_name", "email", "phone", "city"],
    "driver": ["full_name", "license_number"],
    "vehicle": ["plate_number"],
}
