from pathlib import Path

from src.common import contracts as contracts_module
from src.common.contracts import DIMENSION_COLUMNS, DIMENSION_KEYS, PII_COLUMNS


def test_dimension_contracts_have_no_scd3_prev_columns():
    """SCD3 (prev_* copies) was retired; history lives in Silver SCD2 + Gold hist."""
    for entity, columns in DIMENSION_COLUMNS.items():
        assert not [c for c in columns if c.startswith("prev_")], entity


def test_gold_does_not_recompute_scd2_versions():
    """Silver is the only version writer: no Gold job may perform SCD2 MERGEs."""
    gold_root = Path(contracts_module.__file__).resolve().parent.parent / "gold"
    merged = [py for py in gold_root.rglob("*.py") if ".merge(" in py.read_text()]
    assert not merged, merged


def test_surrogate_key_not_part_of_silver_contract():
    """The surrogate is assigned once, deterministically, in the Gold projection."""
    for entity in DIMENSION_KEYS:
        assert "surrogate_key" not in DIMENSION_COLUMNS[entity]


def test_pii_columns_subset_of_dimension_columns():
    for entity, pii in PII_COLUMNS.items():
        assert set(pii).issubset(DIMENSION_COLUMNS[entity])
