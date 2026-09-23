#!/usr/bin/env python3
"""Deterministic TMDL name-quoting fixes across all table files."""
import os, re, sys, glob

T = "/home/hernan/Urban-Mobility-Data-Pipeline/bi/UrbanMobility.SemanticModel/definition/tables"
changes = []

def q(name):
    return "'" + name.replace("'", "''") + "'"

BARE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

for f in glob.glob(os.path.join(T, "*.tmdl")):
    txt = open(f).read()
    orig = txt
    lines = txt.split("\n")
    out = []
    in_fence = False
    for ln in lines:
        fenced_line = "```" in ln
        # measure names: identifier position -> quote when not bare-valid
        m = re.match(r"^(\tmeasure )(.+?)( = ```.*)$", ln)
        if m and not in_fence and not BARE.match(m.group(2)):
            name = m.group(2).strip("'")
            ln = f"{m.group(1)}{q(name)}{m.group(3)}"
            changes.append((os.path.basename(f), "measure", name))
        # partition names: collapse partial quoting -> fully quoted or bare
        m = re.match(r"^(\tpartition )(.+?)( = m)$", ln)
        if m:
            name = m.group(2).strip()
            fixed = name.replace("'", "")  # drop quotes -> plain text
            if " " in fixed:
                fixed = q(fixed)       # 'Dropoff Zone-01'
            changes.append((os.path.basename(f), "partition", name)) if fixed != name else None
            ln = f"{m.group(1)}{fixed}{m.group(3)}"
        # value properties: double-quote everything non-trivial
        m = re.match(r"^(\t+(?:displayFolder|formatString): )(.+)$", ln)
        if m and not in_fence and not re.match(r"^\t+partition", ln):
            val = m.group(2).strip()
            if not (val.startswith('"') and val.endswith('"')):
                if not re.match(r"^[A-Za-z0-9_.]+$", val):
                    ln = f'{m.group(1)}"{val}"'
                    changes.append((os.path.basename(f), "value", val))
        # isKey must carry a value
        if re.match(r"^\t\tisKey$", ln):
            ln = "\t\tisKey: true"
            changes.append((os.path.basename(f), "isKey", ""))
        # track ``` fences so property rules above never touch DAX bodies
        if "```" in ln:
            in_fence = not in_fence
        out.append(ln)
    new = "\n".join(out)
    if new != orig:
        open(f, "w").write(new)
        print(f"updated {os.path.basename(f)}")

print("---- changes ----")
for c in changes[:80]:
    print(c)
print(f"total: {len(changes)}")
