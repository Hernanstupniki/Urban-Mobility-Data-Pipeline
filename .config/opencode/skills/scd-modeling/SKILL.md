---
name: scd-modeling
description: Use for dimensional modeling and Slowly Changing Dimension Type 1, Type 2, or Type 4 design, temporal joins, surrogate keys, and effective-date validation.
---

# SCD dimensional modeling

Apply to Gold dimensions, temporal lookups, and their tests. Identify the business key, surrogate key, grain, effective interval, and late-arriving behavior before editing. For SCD2, validate non-overlapping intervals, one current row per business key, and deterministic point-in-time lookup. Preserve the repository's implemented SCD contracts; do not change dimension type merely because another pattern exists.

Use `src/common/gold_dimensions.py`, `src/common/gold_marts.py`, and `tests/integration/test_scd2_temporal_lookup.py` as local context. Reference: https://github.com/MicrosoftDocs/fabric-docs/blob/main/docs/data-warehouse/dimensional-modeling-dimension-tables.md (documentation, not this skill's source).
