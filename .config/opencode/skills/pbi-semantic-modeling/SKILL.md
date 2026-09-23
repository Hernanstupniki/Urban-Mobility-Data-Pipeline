---
name: pbi-semantic-modeling
description: Use for Urban Mobility Power BI star-schema relationships, TMDL measures, filter propagation, Gold reporting grain, and DAX validation.
---

# Power BI semantic modeling

Apply to `bi/UrbanMobility.SemanticModel`, `bi/validation`, and Gold-to-reporting contracts. Confirm fact and dimension grains, single filter paths, relationship cardinality, and UTC versus local-hour semantics. Compare DAX totals with `bi/validation/expected_metrics.sql` and filtered slices; structural checks alone do not execute DAX. Check the rendered result in Power BI Desktop when a measure or relationship changes.

Use `powerbi-optimization` for a measured performance problem. Use the agentic Power BI toolkit's PBIP/TMDL skills for file-format mechanics. Preserve the project's `gen_report.py` source-of-truth rule for report visuals.
