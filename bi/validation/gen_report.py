#!/usr/bin/env python3
"""Generate UrbanMobility.Report (PBIR) from specs, cross-checking
measure/column references against the semantic model TMDL."""
import json, os, re, sys, shutil

ROOT = "/home/hernan/Urban-Mobility-Data-Pipeline/bi"
SM = os.path.join(ROOT, "UrbanMobility.SemanticModel", "definition", "tables")
RPT = os.path.join(ROOT, "UrbanMobility.Report")

# Read model inventory.
tables = {}
for fn in os.listdir(SM):
    txt = open(os.path.join(SM, fn)).read()
    m = re.match(r"table (?:'([^']+)'|([A-Za-z_][\w ]*))", txt)
    name = (m.group(1) or m.group(2)).strip()
    cols = {c.strip("'") for c in re.findall(r"^\tcolumn (.+)$", txt, re.M)}
    meas = {c.strip("'") for c in re.findall(r"^\tmeasure (.+?) = ", txt, re.M)}
    tables[name] = (cols, meas)

def measure(entity, name):
    assert name in tables.get(entity, (set(), set()))[1], f"unknown measure {entity}.{name}"
    return {"field": {"Measure": {"Expression": {"SourceRef": {"Entity": entity}}, "Property": name}},
            "queryRef": f"{entity}.{name}"}

def column(entity, name):
    assert name in tables.get(entity, (set(), set()))[0], f"unknown column {entity}.{name}"
    return {"field": {"Column": {"Expression": {"SourceRef": {"Entity": entity}}, "Property": name}},
            "queryRef": f"{entity}.{name}"}

# Power BI's model schema validator (ModelSchemaValidator.EnsureValidObjectName)
# rejects a table literally named "Measures" (collides with the reserved
# metadata keyword). The measure-holder table is named "_Measures".
M = lambda n: measure("_Measures", n)

def textbox(vid, pos, runs, order=0):
    return visual(vid, "textbox", pos, order=order, objects={
        "general": [{"properties": {"paragraphs": [{"textRuns": runs}]}}]})

def visual(vid, vtype, pos, query=None, objects=None, vco=None, order=0, title=None):
    # NOTE: PBIR visualContainer schema has NO root-level "order" property
    # (Desktop rejects it as additionalProperty). Layer order is expressed
    # through position.z only; the `order` argument is accepted but ignored
    # to keep call-sites stable.
    if title and vco is None:
        value = {"expr": {"Literal": {"Value": "'" + title + "'"}}}
        vco = {"title": [{"properties": {"text": value}}]}
    v = {"$schema": VIS_SCHEMA, "name": vid, "position": pos, "visual": {
        "visualType": vtype, "drillFilterOtherVisuals": True}}
    if query: v["visual"]["query"] = query
    if objects: v["visual"]["objects"] = objects
    if query:
        label = title or vid.replace("_", " ")
        vco = vco or {}
        vco["general"] = [{"properties": {"altText": {"expr": {"Literal": {"Value": chr(39) + label + chr(39)}}}}}]
    if vco: v["visual"]["visualContainerObjects"] = vco
    return v

def qs(**roles):
    return {"queryState": {k: {"projections": list(v) if not isinstance(v, dict) else [v]} for k, v in roles.items()}}

def qsort(field, direction="Descending"):
    return {"sortDefinition": {"sort": [{"field": field["field"], "direction": direction}],
                               "isDefaultSort": True}}

def slicer(vid, pos, col, order=0):
    return visual(vid, "slicer", pos, query=qs(Field=[col]), order=order, title="Date range")

VIS_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.2.0/schema.json"

W, H = 1280, 720
def pos(x, y, w, h, z=0): return {"x": x, "y": y, "z": z, "width": w, "height": h}

def heatmap_fill(measure_ref):
    color = lambda hex_value: {"Literal": {"Value": chr(39) + hex_value + chr(39)}}
    return {"values": [{"properties": {"backColor": {"solid": {"color": {"expr": {
        "FillRule": {
            "Input": {"SelectRef": {"ExpressionName": measure_ref}},
            "FillRule": {"linearGradient2": {
                "min": {"color": color("#EAF5F6")},
                "max": {"color": color("#087E8B")},
                "nullColoringStrategy": {"strategy": {"Literal": {"Value": chr(39) + "noColor" + chr(39)}}}
            }}
        }
    }}}}}, "selector": {"data": [{"dataViewWildcard": {"matchingOption": 1}}],
                         "metadata": measure_ref}}]}


def page_header(vid, title, subtitle):
    return textbox(vid, pos(28, 18, 1020, 48), [
        {"value": title, "textStyle": {"fontWeight": "bold", "fontSize": "20pt", "color": "#15253A"}},
        {"value": "   " + subtitle, "textStyle": {"fontSize": "10pt", "color": "#516276"}}])


def kpis(ids, labels, measures):
    return [visual(vid, "card", pos(28 + i * 312, 90, 292, 96),
                   query=qs(Values=[measure]), title=label)
            for i, (vid, label, measure) in enumerate(zip(ids, labels, measures))]


def date_filter(vid):
    return slicer(vid, pos(1052, 16, 200, 70), column("dim_date", "date"))


pages = {}

# Operations: demand, completion, cancellations, and delay by place/time.
pages["Operations"] = ("Operations", [
    page_header("ops_header", "OPERATIONS", "When and where does demand peak?"),
    date_filter("ops_sl_date"),
    *kpis(["ops_card_trips", "ops_card_completion", "ops_card_cancel", "ops_card_delay"],
          ["Trips", "Completion rate", "Cancellation rate", "P90 acceptance delay (min)"],
          [M("Trips"), M("Completion Rate"), M("Cancellation Rate"), M("P90 Acceptance Delay")]),
    visual("ops_heatmap", "matrix", pos(28, 210, 780, 464),
           query=qs(Rows=[column("Zone", "zone_name")],
                    Columns=[column("fact_trips", "requested_local_hour")],
                    Values=[M("Trips")]),
           objects=heatmap_fill("_Measures.Trips"),
           title="Demand by pickup zone and local hour"),
    visual("ops_col_hour", "clusteredColumnChart", pos(828, 210, 424, 220),
           query=qs(Category=[column("fact_trips", "requested_local_hour")], Y=[M("Trips")]),
           title="Trips by local request hour"),
    visual("ops_delay_zone", "barChart", pos(828, 450, 424, 224),
           query={**qs(Category=[column("Zone", "zone_name")], Y=[M("P90 Acceptance Delay")]),
                  **qsort(M("P90 Acceptance Delay"))},
           title="Where is P90 acceptance delay highest?"),
])

# Revenue: generated, collected, and leakage; payment and driver breakdown.
pages["Revenue"] = ("Revenue", [
    page_header("rev_header", "REVENUE", "Where is value created and collected?"),
    date_filter("rev_sl_date"),
    *kpis(["rev_card_gmv", "rev_card_collected", "rev_card_collection", "rev_card_leakamount"],
          ["GMV", "Collected amount", "Collection rate", "Revenue leakage"],
          [M("GMV"), M("Collected Amount"), M("Collection Rate"), M("Revenue Leakage Amount")]),
    visual("rev_bar_zone", "barChart", pos(28, 210, 600, 216),
           query={**qs(Category=[column("Zone", "zone_name")], Y=[M("GMV")]),
                  **qsort(M("GMV"))},
           title="GMV by pickup zone"),
    visual("rev_stacked_method", "clusteredColumnChart", pos(648, 210, 604, 216),
           query=qs(Category=[column("dim_payment_method", "payment_method_name")],
                    Series=[column("fact_payments", "status")], Y=[M("Billed Amount")]),
           title="Billed amount by payment method and status"),
    visual("rev_leak_zone", "barChart", pos(28, 446, 600, 228),
           query={**qs(Category=[column("Zone", "zone_name")], Y=[M("Revenue Leakage Amount")]),
                  **qsort(M("Revenue Leakage Amount"))},
           title="Where does revenue leakage concentrate?"),
    visual("rev_matrix_drivers", "matrix", pos(648, 446, 604, 228),
           query={**qs(Rows=[column("dim_driver", "driver_id")],
                       Values=[M("GMV"), M("Completed Trips")]),
                  **qsort(M("GMV"))},
           title="Top drivers by GMV (ID only)"),
])

# Experience: four core metrics and four questions, with driver-grain scatter.
pages["Experience"] = ("Experience", [
    page_header("exp_header", "EXPERIENCE", "What shapes the ride experience?"),
    date_filter("exp_sl_date"),
    *kpis(["exp_card_rating", "exp_card_count", "exp_card_delay", "exp_card_cancel"],
          ["Average rating", "Rating count", "P90 acceptance delay (min)", "Cancellation rate"],
          [M("Average Rating"), M("Rating Count"), M("P90 Acceptance Delay"), M("Cancellation Rate")]),
    visual("exp_scatter_delay", "scatterChart", pos(28, 210, 760, 464),
           query=qs(X=[M("Median Acceptance Delay")], Y=[M("Average Rating")],
                    Category=[column("dim_driver", "driver_id")]),
           title="Acceptance delay vs rating by driver"),
    visual("exp_score_dist", "clusteredColumnChart", pos(808, 210, 444, 142),
           query=qs(Category=[column("fact_ratings", "score")], Y=[M("Rating Count")]),
           title="Rating distribution"),
    visual("exp_bar_vtype", "barChart", pos(808, 368, 444, 142),
           query=qs(Category=[column("dim_vehicle", "vehicle_type")], Y=[M("Average Rating")]),
           title="Rating by vehicle type"),
    visual("exp_cancel_reason", "barChart", pos(808, 526, 444, 148),
           query=qs(Category=[column("fact_trips", "cancel_reason")], Y=[M("Cancelled Trips")]),
           title="Why are trips cancelled?"),
])

# Data Health: quality, its trend, compliant erasures, and freshness.
pages["DataHealth"] = ("Data Health & Compliance", [
    page_header("dq_header", "DATA HEALTH", "Can the analytics be trusted?"),
    date_filter("dq_sl_date"),
    *kpis(["dq_card_trust", "dq_card_issue", "dq_card_imputed", "dq_card_erased"],
          ["Data trust rate", "DQ issue trips", "Imputed fare rate", "Erased subjects"],
          [M("Data Trust Rate"), M("DQ Issue Trips"), M("Imputed Fare Rate"),
           M("Total Erased Subjects")]),
    visual("dq_trend", "lineChart", pos(28, 210, 1224, 220),
           query=qs(Category=[column("dim_date", "date")], Y=[M("DQ Issue Rate")]),
           title="How do quality issues change over time?"),
    visual("dq_matrix_compliance", "matrix", pos(28, 450, 600, 224),
           query=qs(Rows=[column("dim_driver", "status")],
                    Values=[M("Drivers Erased"), M("DQ Issue Rate")]),
           title="Erasure and trust by driver status"),
    visual("dq_fresh", "card", pos(648, 450, 260, 224),
           query=qs(Values=[M("Last Gold Load")]), title="Last Gold load (UTC)"),
    textbox("dq_note", pos(928, 450, 324, 224), [
        {"value": "WHY QUALITY MATTERS", "textStyle": {"fontWeight": "bold", "fontSize": "12pt", "color": "#15253A"}},
        {"value": "\nQuality flags separate source errors from service behavior. Compare issue rates before interpreting demand, revenue or experience changes.",
         "textStyle": {"fontSize": "11pt", "color": "#516276"}},
        {"value": "\nUnknown driver = unassigned trip, not a data error. Date trends follow UTC calendar.",
         "textStyle": {"fontSize": "10pt", "color": "#516276"}},
    ]),
])

# Write report files.
os.makedirs(os.path.join(RPT, "definition", "pages"), exist_ok=True)

json.dump({"$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
           "version": "4.0",
           "datasetReference": {"byPath": {"path": "../UrbanMobility.SemanticModel"}}},
          open(os.path.join(RPT, "definition.pbir"), "w"), indent=2)

json.dump({"$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/versionMetadata/1.0.0/schema.json",
           "version": "2.0.0"},
          open(os.path.join(RPT, "definition", "version.json"), "w"), indent=2)

json.dump({
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/3.0.0/schema.json",
    "themeCollection": {"customTheme": {"name": "UrbanLight",
                                        "reportVersionAtImport": {"visual": "2.2.0", "report": "2.2.0", "page": "2.0.0"},
                                        "type": "SharedResources"}},
    "resourcePackages": [{"name": "SharedResources", "type": "SharedResources",
                          "items": [{"name": "UrbanLight", "path": "BuiltInThemes/UrbanLight.json", "type": "CustomTheme"}]}],
    "settings": {"useStylableVisualContainerHeader": True,
                 # enum of the official schema: AllowSummarized |
                 # AllowSummarizedAndUnderlying | None (AllowAll is invalid)
                 "exportDataMode": "AllowSummarized",
                 "useEnhancedTooltips": True, "defaultDrillFilterOtherVisuals": True},
}, open(os.path.join(RPT, "definition", "report.json"), "w"), indent=2)

theme = {
  "name": "UrbanLight",
  "dataColors": ["#087E8B", "#4169B1", "#2E8B74", "#C78529", "#BD5263", "#516276", "#277FB2", "#7863B0"],
  "good": "#2E8B74", "neutral": "#C78529", "bad": "#BD5263",
  "maximum": "#2E8B74", "center": "#C78529", "minimum": "#BD5263",
  "foreground": "#15253A", "background": "#F7F9FC", "tableAccent": "#087E8B", "null": "#334155",
  "visualStyles": {
    "page": {
      "*": {
        "background": [{"transparency": 0, "solidColor": {"solid": {"color": {"expr": {"Literal": {"Value": "\"#F7F9FC\""}}}}}}],
        "outspace": [{"transparency": 0, "solidColor": {"solid": {"color": {"expr": {"Literal": {"Value": "\"#E9EEF4\""}}}}}}],
      }
    },
    "visualHeaders": {
      "*": {
        "general": [{"textColor": {"expr": {"Literal": {"Value": "\"#516276\""}}}}]
      }
    },
  },
}
os.makedirs(os.path.join(RPT, "StaticResources", "SharedResources", "BuiltInThemes"), exist_ok=True)
json.dump(theme, open(os.path.join(RPT, "StaticResources", "SharedResources", "BuiltInThemes", "UrbanLight.json"), "w"), indent=2)

page_order = []
for key, (display, visuals) in pages.items():
    pdir = os.path.join(RPT, "definition", "pages", key)
    os.makedirs(os.path.join(pdir, "visuals"), exist_ok=True)
    json.dump({"$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.0.0/schema.json",
               "name": key, "displayName": display, "displayOption": "FitToPage",
               "width": W, "height": H},
              open(os.path.join(pdir, "page.json"), "w"), indent=2)
    # NOTE: page.json schema 2.0.0 does NOT allow "visualStyles" at root
    # (Desktop rejects it as additionalProperty). Page canvas darkness comes
    # from the UrbanLight theme (visualStyles.page in the theme file).
    expected = {v["name"] for v in visuals if v}
    for stale in os.listdir(os.path.join(pdir, "visuals")):
        if stale not in expected:
            shutil.rmtree(os.path.join(pdir, "visuals", stale))
    for i, v in enumerate(v for v in visuals if v):
        vd = os.path.join(pdir, "visuals", v["name"])
        os.makedirs(vd, exist_ok=True)
        json.dump(v, open(os.path.join(vd, "visual.json"), "w"), indent=2)
    page_order.append(key)

json.dump({"$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.0.0/schema.json",
           "pageOrder": page_order, "activePageName": page_order[0]},
          open(os.path.join(RPT, "definition", "pages", "pages.json"), "w"), indent=2)

print("PAGES:", page_order)
for k, (_, vs) in pages.items():
    print(f"  {k}: {len([v for v in vs if v])} visuals")
print("OK")
