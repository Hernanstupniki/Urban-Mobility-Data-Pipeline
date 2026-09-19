import importlib.util
from pathlib import Path


def _load_generator():
    path = Path(__file__).parents[2] / "scripts" / "generate_oltp_data" / "generate_oltp_data.py"
    spec = importlib.util.spec_from_file_location("oltp_generator", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_default_profile_intentionally_generates_dirty_data(monkeypatch):
    monkeypatch.setenv("DB_PASSWORD", "test-only")
    generator = _load_generator()
    expected_dirty_signals = {
        "BROKEN_RATE", "TIME_WEIRDNESS_RATE", "COORDS_MISSING_RATE",
        "COORDS_OUT_OF_RANGE_RATE", "VEHICLE_DRIVER_MISMATCH_RATE",
        "RATINGS_COMMENT_PII_RATE", "PAYMENT_PROVIDER_REF_RATE",
        "TEXT_FORMAT_NOISE_RATE", "CATEGORY_VARIANT_RATE", "INVALID_CONTACT_RATE",
        "DUPLICATE_PASSENGER_RATE", "DUPLICATE_PAYMENT_RATE",
        "PAYMENT_PROVIDER_REF_PII_RATE", "CANCEL_NOTE_PII_RATE",
        "LONG_ACCEPTANCE_DELAY_RATE", "LONG_TRIP_DURATION_RATE",
        "DISTANCE_OUTLIER_RATE", "PAYMENT_TIMESTAMP_INCONSISTENCY_RATE",
    }
    assert expected_dirty_signals.issubset(generator.DIRTY_DATA_RATES)
    assert all(generator.DIRTY_DATA_RATES[name] > 0 for name in expected_dirty_signals)


def test_invalid_dirty_rate_is_rejected(monkeypatch):
    monkeypatch.setenv("DB_PASSWORD", "test-only")
    generator = _load_generator()
    generator.DIRTY_DATA_RATES["BROKEN_RATE"] = 1.5
    try:
        try:
            generator.validate_configuration()
        except ValueError as exc:
            assert "between 0 and 1" in str(exc)
        else:
            raise AssertionError("invalid rate was accepted")
    finally:
        generator.DIRTY_DATA_RATES["BROKEN_RATE"] = generator.BROKEN_RATE


def test_shared_quality_rules_cover_pii_and_category_variants():
    from src.common.data_quality import canonical_vehicle_type, contains_potential_pii

    assert canonical_vehicle_type(" HATCH BACK ") == "hatchback"
    assert canonical_vehicle_type("MotorCycle") == "motorbike"
    assert contains_potential_pii("delay | contact: person@example.com")
    assert contains_potential_pii("contact: Jane Example")
    assert not contains_potential_pii("driver arrived late")
