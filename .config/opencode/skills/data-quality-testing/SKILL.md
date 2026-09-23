---
name: data-quality-testing
description: Use for Urban Mobility data contracts, quality flags, validation queries, pytest coverage, and Bronze-to-Gold quality checks.
---

# Data quality and testing

Apply to `src/common/contracts.py`, `src/common/data_quality.py`, pipeline checks, and `tests`. State the invariant and grain first. Cover realistic null, duplicate, temporal, and referential-integrity cases that can fail in this pipeline. Reuse the repository's existing pytest and Spark test setup. Check counts and SQL baselines when validation affects published reporting data. Record reproducible new errors in `docs/troubleshooting.md` according to project `AGENTS.md`.

The supplied https://github.com/topics/skill-testing is a GitHub topic index, not an installable skill or formal testing specification. This `SKILL.md` is the project's own definition.
