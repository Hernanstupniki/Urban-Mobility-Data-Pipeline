"""Static shape checks for the serving-layer publishing specs."""

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.common.publish import SPECS  # noqa: E402

_SAFE_TABLE = re.compile(r"^[a-z][a-z0-9_]*$")


def test_specs_are_valid_tables_with_usable_keys():
    assert len(SPECS) >= 8
    for table, spec in SPECS.items():
        assert _SAFE_TABLE.match(table), table
        assert spec["source"], table
        assert all(_SAFE_TABLE.match(col) for col in spec["pk"]), table
        for index in spec["indexes"]:
            assert index and all(_SAFE_TABLE.match(col) for col in index), table
        assert set(spec) == {"source", "pk", "indexes"}, table


def test_sources_are_layered_paths_without_escape():
    for table, spec in SPECS.items():
        parts = spec["source"]
        assert parts[0] == "gold", table
        assert ".." not in parts and not any(p.startswith("/") for p in parts), table


def test_publishing_layer_names_do_not_collide_with_lake_control():
    assert "publish_state" not in {name for name in SPECS}
    assert set(SPECS).isdisjoint({"bronze", "silver"})
