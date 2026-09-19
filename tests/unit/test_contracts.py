from src.common.contracts import DIMENSION_COLUMNS, PII_COLUMNS, SCD3_PREVIOUS_COLUMNS


def test_scd3_does_not_duplicate_technical_columns():
    forbidden = {"raw_loaded_at", "batch_id", "source_system", "scd_hash", "is_current"}
    for columns in SCD3_PREVIOUS_COLUMNS.values():
        assert forbidden.isdisjoint(columns)


def test_all_pii_columns_are_part_of_dimension_contracts():
    for entity, pii_columns in PII_COLUMNS.items():
        assert set(pii_columns).issubset(DIMENSION_COLUMNS[entity])


def test_quality_contract_exposes_deduplication_and_normalization_evidence():
    assert {
        "canonical_passenger_id",
        "potential_duplicate_passenger",
        "invalid_phone_format",
    }.issubset(DIMENSION_COLUMNS["passenger"])
    assert "vehicle_type_was_normalized" in DIMENSION_COLUMNS["vehicle"]
