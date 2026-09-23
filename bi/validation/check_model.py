#!/usr/bin/env python3
"""Structural validation of the PBIP semantic model BEFORE Power BI Desktop
sees it. Catches the exact class of defects Desktop reports as `InvalidName`:
partially-quoted identifiers, unquoted names with spaces, unquoted property
values, malformed fences, bad partition/table/measure/column/relationship
identifiers, duplicates and dangling references."""
import re, sys, glob, os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEF = os.path.join(ROOT, "UrbanMobility.SemanticModel", "definition")
errors = []
BARE_ID = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
BARE_ID_HYPH = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]*$")
# Object names Desktop's ModelSchemaValidator rejects outright. Empirically
# confirmed for THIS project: a table literally named "Measures" fails
# EnsureValidObjectName ("Table nombre Measures no admitido"). The model uses
# "_Measures" instead. Keep this the *confirmed* set only — Microsoft's own
# samples use "Table" as a valid table name, so it must NOT be listed here.
RESERVED_TABLE_NAMES = {"Measures"}

def unquote(tok):
    """Return plain name if tok is bare-valid or fully/correctly quoted; else error."""
    tok = tok.strip()
    if tok.startswith("'"):
        if not (tok.endswith("'") and len(tok) >= 2):
            return None, f"partially quoted identifier (trailing junk): {tok!r}"
        inner = tok[1:-1]
        if "'" in inner:  # escaped quotes would be '' — single leftover is invalid
            return None, f"unescaped quote inside quoted identifier: {tok!r}"
        return inner, None
    if re.search(r"\s", tok):
        return None, f"identifier with spaces not quoted: {tok!r}"
    if not BARE_ID_HYPH.match(tok):
        return None, f"invalid bare identifier: {tok!r}"
    return tok, None

def item_name(line, keyword, allow_hyphen=True):
    m = re.match(rf"^\t*{keyword} (.+?)( =| \()?$", line) if keyword != "column" else re.match(r"^\tcolumn (.+)$", line)
    if not m:
        return None, None, None
    raw = m.group(1)
    name, err = unquote(raw)
    if err:
        errors.append(err)
    if name and not raw.startswith("'") and not BARE_ID_HYPH.match(name):
        errors.append(f"bare identifier has invalid chars: {raw!r}")
    return name, raw.startswith("'"), None

tables = {}
for f in sorted(glob.glob(os.path.join(DEF, "tables", "*.tmdl"))):
    fname = os.path.basename(f)
    txt = open(f).read()
    lines = txt.split("\n")
    # fence balance
    if txt.count("```") % 2 != 0:
        errors.append(f"{fname}: unbalanced ``` fences")
    # TMDL-level comments must never be indented (Desktop "Indentation" error).
    # Comments inside the M `source =` block are Power Query, not TMDL, so we
    # only flag `//` lines that are NOT within a partition source region.
    in_m = False
    for i, ln in enumerate(lines, 1):
        if re.match(r"^\tpartition ", ln):
            in_m = True
        elif re.match(r"^\t(annotation|column|measure)\b", ln):
            in_m = False
        if in_m:
            continue
        if re.match(r"^\s+//", ln):
            errors.append(f"{fname}:{i}: indented // comment (invalid TMDL)")
        if ln.startswith(" ") and ln.strip():
            errors.append(f"{fname}:{i}: space-indented line (TMDL uses tabs): {ln[:40]!r}")
        if re.match(r"^\t+```", ln) and not re.match(r"^\t\t\t```", ln):
            errors.append(f"{fname}:{i}: measure fence closer must be 3 tabs: {ln!r}")
    m = re.match(r"table ('(?:[^']|'')+'|[A-Za-z_][\w -]*)$", lines[0])
    if not m:
        errors.append(f"{fname}: bad table line: {lines[0]!r}")
        continue
    tname, err = unquote(m.group(1))
    if err: errors.append(f"{fname}: {err}")
    cols, meas, parts = set(), set(), set()
    for ln in lines:
        if ln.startswith("\tcolumn "):
            name, e = unquote(ln[len("\tcolumn "):])
            if e: errors.append(f"{fname}: {e}")
            if name in cols: errors.append(f"{fname}: duplicate column {name!r}")
            cols.add(name)
        elif ln.startswith("\tmeasure "):
            mm = re.match(r"^\tmeasure (.+?) = ", ln)
            if mm:
                name, e = unquote(mm.group(1))
                e and errors.append(f"{fname}: {e}")
                if name in meas: errors.append(f"{fname}: duplicate measure {name!r}")
                meas.add(name)
        elif ln.startswith("\tpartition "):
            pm = re.match(r"^\tpartition (.+?) = m$", ln)
            if not pm:
                errors.append(f"{fname}: malformed partition line: {ln!r}")
            else:
                name, e = unquote(pm.group(1))
                e and errors.append(f"{fname}: {e}")
                if name in parts: errors.append(f"{fname}: duplicate partition {name!r}")
                parts.add(name)
        elif re.match(r"^\t+(displayFolder|formatString): ", ln):
            val = ln.split(": ", 1)[1].strip()
            if not (val.startswith('"') and val.endswith('"')) and not re.match(r"^[A-Za-z0-9_.]+$", val):
                errors.append(f"{fname}: unquoted property value with special chars: {ln.strip()!r}")
    tables[tname] = (cols, meas)

# reserved object names (Desktop schema validator)
for t in tables:
    if t in RESERVED_TABLE_NAMES:
        errors.append(f"reserved table name rejected by Desktop: {t!r}")
# DAX rule: a measure cannot share a name with ANY column in the model
all_columns = {c for cols, _ in tables.values() for c in cols}
for t, (_, meas) in tables.items():
    clash = meas & all_columns
    if clash:
        errors.append(f"measure names collide with model columns in {t}: {sorted(clash)}")
for t, (c, m) in tables.items():
    overlap = c & m
    if overlap:
        errors.append(f"column/measure name collision in {t}: {sorted(overlap)}")

# model refs
model = open(os.path.join(DEF, "model.tmdl")).read()
refs = []
for r in re.findall(r"^ref table (.+)$", model, re.M):
    name, e = unquote(r)
    e and errors.append(f"model.tmdl: {e}")
    refs.append(name)
for r in refs:
    if r not in tables: errors.append(f"model.tmdl ref missing table: {r}")
for t in tables:
    if t not in refs: errors.append(f"table file not referenced in model: {t}")

# relationships
rel = open(os.path.join(DEF, "relationships.tmdl")).read()
for blk in re.finditer(r"^relationship (.+)$((?:\n\t.+)*)", rel, re.M):
    rname, e = unquote(blk.group(1))
    e and errors.append(f"relationships: {e}")
    for side in ("fromColumn", "toColumn"):
        mm = re.search(rf"{side}: (.+)", blk.group(2))
        if not mm: errors.append(f"relationship {rname}: missing {side}"); continue
        val = mm.group(1).strip()
        pm = re.match(r"('(?:[^']|'')+'|[A-Za-z_][\w]*)\.(.+)$", val)
        if not pm:
            errors.append(f"relationship {rname}: bad column ref {val!r}"); continue
        tent = unquote(pm.group(1))[0] or ""
        if tent not in tables:
            errors.append(f"relationship {rname}: unknown table {tent}")
        elif pm.group(2) not in tables[tent][0]:
            errors.append(f"relationship {rname}: unknown column {tent}.{pm.group(2)}")
    if re.search(r"crossFilteringBehavior:\s*(bothDirections|crossFilteringBoth)", blk.group(2)):
        errors.append(f"relationship {rname}: non-single filter direction (star model must be single)")

print("== TABLES ==")
for t, (c, m) in sorted(tables.items()):
    print(f"{t}: {len(c)} cols, {len(m)} measures")
print("== ERRORS ==")
if errors:
    print("\n".join(errors)); sys.exit(1)
print("none — model structure valid")
