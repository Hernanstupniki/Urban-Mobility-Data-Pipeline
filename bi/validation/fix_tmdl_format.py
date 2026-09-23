#!/usr/bin/env python3
"""fix_tmdl_format.py — make every tables/*.tmdl match Desktop's own output:
- delete ALL // comment lines (TMDL comments are column-0 only; we keep zero)
- measure DAX fence bodies re-indented from 2 to 3 tabs (Desktop style)
- tab-only structural indentation; blank lines only outside property runs
"""
import glob, os, re, sys

T = "/home/hernan/Urban-Mobility-Data-Pipeline/bi/UrbanMobility.SemanticModel/definition/tables"
report = {}
for f in sorted(glob.glob(os.path.join(T, "*.tmdl"))):
    lines = open(f).read().split("\n")
    out, removed, fence = [], 0, False
    for ln in lines:
        if re.match(r"^\s*//", ln):
            removed += 1
            continue
        if "```" in ln:
            fence = not fence
        # measure body: was written at 2 tabs, Desktop uses 3
        if fence and ln.startswith("\t\t") and not ln.startswith("\t\t\t"):
            ln = "\t" + ln
        out.append(ln)
    new = "\n".join(out)
    if new != "\n".join(lines):
        open(f, "w").write(new)
    report[os.path.basename(f)] = removed
for k, v in report.items():
    print(f"{k}: comments_removed={v}")
print("done")
