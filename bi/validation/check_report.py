#!/usr/bin/env python3
"""Independent PBIR checks: every JSON parses; every query field resolves
against the TMDL model; no visual rectangles collide on a page."""
import json, os, re, glob, sys, itertools

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SM = os.path.join(ROOT, "UrbanMobility.SemanticModel", "definition", "tables")
RP = os.path.join(ROOT, "UrbanMobility.Report", "definition")

tables = {}
for f in glob.glob(os.path.join(SM, "*.tmdl")):
    txt = open(f).read()
    m = re.match(r"table (?:'([^']+)'|([A-Za-z_][\w -]*))", txt)
    name = (m.group(1) or m.group(2)).strip()
    cols = {c.strip("'") for c in re.findall(r"^\tcolumn (.+)$", txt, re.M)}
    meas = {c.strip("'") for c in re.findall(r"^\tmeasure (.+?) = ", txt, re.M)}
    tables[name] = (cols, meas)

problems = []

def resolve_field(field):
    if "Measure" in field:
        ent = field["Measure"]["Expression"]["SourceRef"]["Entity"]
        prop = field["Measure"]["Property"]
        if ent not in tables or prop not in tables[ent][1]:
            problems.append(f"measure not found: {ent}.{prop}")
    elif "Column" in field:
        ent = field["Column"]["Expression"]["SourceRef"]["Entity"]
        prop = field["Column"]["Property"]
        if ent not in tables or prop not in tables[ent][0]:
            problems.append(f"column not found: {ent}.{prop}")

pages_json = json.load(open(os.path.join(RP, "pages", "pages.json")))
for page in pages_json["pageOrder"]:
    pdir = os.path.join(RP, "pages", page)
    json.load(open(os.path.join(pdir, "page.json")))
    rects = []
    for vf in sorted(glob.glob(os.path.join(pdir, "visuals", "*", "visual.json"))):
        v = json.load(open(vf))
        pos = v["position"]
        q = v["visual"].get("query")
        if q:
            for role, blob in q.get("queryState", {}).items():
                for proj in blob.get("projections", []):
                    resolve_field(proj["field"])
        x, y, w, h = pos["x"], pos["y"], pos["width"], pos["height"]
        if w <= 0 or h <= 0:
            problems.append(f"{page}/{v['name']}: zero-size")
        if x + w > 1281 or y + h > 721:
            problems.append(f"{page}/{v['name']}: out of canvas")
        for (x2, y2, w2, h2, other) in rects:
            ix = max(0, min(x + w, x2 + w2) - max(x, x2))
            iy = max(0, min(y + h, y2 + h2) - max(y, y2))
            if ix * iy > 0.15 * min(w * h, w2 * h2):
                problems.append(f"{page}: overlap {v['name']} x {other} ({ix:.0f}x{iy:.0f}px)")
        rects.append((x, y, w, h, v["name"]))

for extra in ["report.json", "version.json",
              os.path.join("..", "definition.pbir"),
              os.path.join("..", "StaticResources", "SharedResources", "BuiltInThemes", "UrbanLight.json"),
              os.path.join("..", "..", "UrbanMobility.pbip")]:
    json.load(open(os.path.join(RP, extra)))

print("VISUALS OK" if not problems else "PROBLEMS:")
print("\n".join(problems) if problems else f"{sum(len(os.listdir(os.path.join(RP,'pages',p,'visuals'))) for p in pages_json['pageOrder'])} visuals across {len(pages_json['pageOrder'])} pages, refs valid, no collisions")
sys.exit(1 if problems else 0)
