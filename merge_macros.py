cat > merge_macros.py <<'PY'
#!/usr/bin/env python3
"""
merge_macros.py

Usage:
  python merge_macros.py --input-dir originals --output-dir out --versions 5 --seed 12345

Behavior:
 - Recursively loads JSON files in input-dir.
 - Normalizes several common JSON shapes to a dict with 'events' list.
 - Skips files that cannot be parsed or normalized (prints warnings).
 - Produces merged_bundle.json and merged_bundle.zip in output-dir.
 - Produces `versions` deterministic shuffles of the merged events using `seed`.
"""

import argparse
import json
import sys
import os
import glob
from pathlib import Path
import zipfile
import random
import shutil
from copy import deepcopy

def parse_args():
    p = argparse.ArgumentParser(description="Merge macro JSON files into a bundle.")
    p.add_argument("--input-dir", required=True, help="Directory with input JSON macro files (recursive).")
    p.add_argument("--output-dir", required=True, help="Directory where merged bundle will be written.")
    p.add_argument("--versions", type=int, default=1, help="Number of versions to output (shuffled variants).")
    p.add_argument("--seed", type=int, default=None, help="Random seed for deterministic shuffles.")
    return p.parse_args()

def load_json(path):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as e:
        try:
            with open(path, "rb") as fh:
                raw = fh.read(400)
            snippet = repr(raw)
        except Exception:
            snippet = "<could not read snippet>"
        raise ValueError(f"JSON parse error: {e}; snippet={snippet}")

def load_and_normalize_events(path):
    """
    Load JSON from `path` and normalize it to a dict with an 'events' list or return None if not possible.
    Accepts:
      - { "events": [...] } -> returned as-is
      - top-level list [ {...}, {...} ] -> {'events': list}
      - { "items" | "entries" | "records" | "actions" : [...] } -> {'events': alt_list}
      - { "event": {...} } -> {'events': [that_object]}
      - {single-event-dict} (heuristic: has event-like keys) -> {'events':[that_dict]}
    """
    try:
        data = load_json(path)
    except ValueError as e:
        print(f"WARNING: cannot parse {path}: {e}", file=sys.stderr)
        return None

    if isinstance(data, dict) and isinstance(data.get("events"), list):
        return data

    if isinstance(data, list):
        return {"events": data}

    if isinstance(data, dict):
        for alt in ("items", "entries", "records", "actions", "eventsList", "events_array"):
            if isinstance(data.get(alt), list):
                return {"events": data[alt]}

        if "event" in data and isinstance(data["event"], dict):
            return {"events": [data["event"]]}

        event_like_keys = ("Type", "Time", "X", "Y", "KeyCode", "type", "time", "x", "y", "keyCode", "id", "timestamp")
        if any(k in data for k in event_like_keys):
            return {"events": [data]}

    return None

def find_json_files(input_dir):
    p = Path(input_dir)
    if not p.exists():
        raise FileNotFoundError(f"Input directory does not exist: {input_dir}")
    pattern = str(p / "**" / "*.json")
    files = sorted(glob.glob(pattern, recursive=True))
    return files

def build_merged_events(valid_datas):
    merged = []
    for d in valid_datas:
        events = d.get("events", [])
        if isinstance(events, list):
            merged.extend(deepcopy(events))
    return merged

def write_outputs(merged_versions, output_dir):
    outp = Path(output_dir)
    outp.mkdir(parents=True, exist_ok=True)

    bundle = {
        "meta": {
            "generator": "merge_macros.py",
            "versions_count": len(merged_versions)
        },
        "versions": merged_versions
    }

    json_path = outp / "merged_bundle.json"
    zip_path = outp / "merged_bundle.zip"

    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(bundle, fh, indent=2, ensure_ascii=False)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(json_path, arcname="merged_bundle.json")

    print(f"WROTE: {json_path}")
    print(f"WROTE: {zip_path}")

def main():
    args = parse_args()

    files = find_json_files(args.input_dir)
    if not files:
        raise SystemExit(f"ERROR: No JSON files found in input-dir: {args.input_dir}")

    print(f"Found {len(files)} JSON files; attempting to load and normalize...")

    valid_datas = []
    skipped = 0
    for path in files:
        normalized = load_and_normalize_events(path)
        if normalized is None:
            print(f"SKIP: {path} (unexpected schema or parse error)", file=sys.stderr)
            skipped += 1
            continue
        valid_datas.append(normalized)

    total_valid = len(valid_datas)
    print(f"Loaded {total_valid} usable file(s); skipped {skipped} file(s).")

    if total_valid == 0:
        raise SystemExit("ERROR: No input files with usable 'events' were found. Check originals/ for corrupted or differently-shaped JSON.")

    merged_all = build_merged_events(valid_datas)
    print(f"Merged total events: {len(merged_all)}")

    versions = max(1, args.versions)
    base_seed = args.seed if args.seed is not None else random.randrange(2**31)
    merged_versions = []
    for i in range(versions):
        events_copy = deepcopy(merged_all)
        r = random.Random(base_seed + i)
        r.shuffle(events_copy)
        merged_versions.appe_
