---
name: power-bi-agentic-development
description: Route Power BI agent-assisted authoring tasks to the relevant PBIP, PBIR, report-design, theme, and semantic-model skills from the data-goblin toolkit.
---

# Power BI agentic development toolkit

The upstream repository is a marketplace of multiple plugins and skills, not one root `SKILL.md`: https://github.com/data-goblin/power-bi-agentic-development . Select only the specialist skill needed for the request: `pbip` for project structure, `pbir-format` for PBIR metadata, `pbir-cli` for supported report operations, `pbi-report-design` for layout and chart choices, `modifying-theme-json` for themes, and `review-report` for report audits. For this project, combine with `pbi-semantic-modeling` for its actual model and SQL baselines. Follow `AGENTS.md`; `bi/validation/gen_report.py` remains the report visual source of truth.

Use the available tools and dependencies; a skill description does not imply that its CLI or MCP server is installed.
