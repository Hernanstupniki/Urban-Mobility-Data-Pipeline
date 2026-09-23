#!/usr/bin/env python3
"""validate_schemas.py — validate every PBIR/PBIP JSON against the OFFICIAL
Microsoft json-schemas declared via $schema.

Mapping: https://developer.microsoft.com/json-schemas/<path>
  -> https://raw.githubusercontent.com/microsoft/json-schemas/main/<path>
Schemas are cached under bi/validation/.cache/schemas (gitignored).
Exit 1 on any additionalProperties/enum/required/type violation.
"""
import json, os, re, sys, glob, urllib.request
from urllib.parse import urljoin

try:
    from jsonschema import Draft7Validator, validators, RefResolver
except ImportError:
    sys.exit("install: pip3 install --user jsonschema")

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(HERE, ".cache", "schemas")
os.makedirs(CACHE, exist_ok=True)
ROOT = os.path.dirname(HERE)
RPT = os.path.join(ROOT, "UrbanMobility.Report")
RAW = "https://raw.githubusercontent.com/microsoft/json-schemas/main/"

def map_url(uri):
    m = re.match(r"https://developer\.microsoft\.com/json-schemas/(.+)", uri)
    return RAW + m.group(1) if m else uri

def load_uri(uri, store, depth=0):
    """recursively preload a schema (and its $refs) into store{absolute uri}."""
    if uri in store or depth > 60:
        return store.get(uri)
    local = os.path.join(CACHE, re.sub(r"\W", "_", uri)[:150] + ".json")
    try:
        if os.path.exists(local):
            doc = json.load(open(local))
        else:
            with urllib.request.urlopen(map_url(uri), timeout=30) as r:
                doc = json.loads(r.read().decode("utf-8"))
            json.dump(doc, open(local, "w"))
    except Exception as e:
        print(f"!! fetch failed {uri}: {e}")
        return None
    store[uri] = doc
    # follow refs (absolute, relative and sibling-file)
    def walk(node, base):
        if isinstance(node, dict):
            ref = node.get("$ref")
            if isinstance(ref, str) and not ref.startswith("#"):
                target = urljoin(base, ref)
                load_uri(target, store, depth + 1)
            for v in node.values():
                walk(v, base)
        elif isinstance(node, list):
            for v in node:
                walk(v, base)
    base = uri[: uri.rfind("/") + 1] if "/" in uri else ""
    walk(doc, base)
    return doc

targets = []
targets.append(("definition.pbir", os.path.join(RPT, "definition.pbir")))
D = os.path.join(RPT, "definition")
for rel in ["report.json", "version.json", "pages/pages.json"]:
    targets.append((rel, os.path.join(D, rel)))
for p in sorted(glob.glob(os.path.join(D, "pages", "*", "page.json"))):
    targets.append((os.path.relpath(p, ROOT), p))
for v in sorted(glob.glob(os.path.join(D, "pages", "*", "visuals", "*", "visual.json"))):
    targets.append((os.path.relpath(v, ROOT), v))
pbip_path = os.path.join(ROOT, "UrbanMobility.pbip")
targets.append(("UrbanMobility.pbip", pbip_path))

store = {}
ok = 0
problems = []
for name, path in targets:
    doc = json.load(open(path))
    surl = doc.get("$schema")
    if not surl:
        problems.append(f"{name}: no $schema")
        continue
    schema = load_uri(surl, store)
    if schema is None:
        problems.append(f"{name}: schema unavailable {surl}")
        continue
    resolver = RefResolver(base_uri=surl, referrer=schema, store=store)
    val = Draft7Validator(schema, resolver=resolver)
    errs = sorted(val.iter_errors(doc), key=lambda e: list(e.path))
    if errs:
        for e in errs[:6]:
            loc = "/".join(str(p) for p in e.path) or "<root>"
            problems.append(f"{name}: {loc}: {e.message[:170]}")
        if len(errs) > 6:
            problems.append(f"{name}: … +{len(errs)-6} more")
    else:
        ok += 1

print(f"validated OK: {ok}/{len(targets)}")
if problems:
    print("VIOLACIONES:")
    for x in problems:
        print(" -", x)
    sys.exit(1)
print("ALL PBIR FILES CONFORM TO OFFICIAL MICROSOFT SCHEMAS")
