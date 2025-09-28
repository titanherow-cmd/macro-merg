#!/usr/bin/env python3
"""
merge_macros.py

Simplified script matching the compact UI:

- Hardcoded defaults
    input_dir = "input"
    output_dir = "output"
    seed = 12345

- CLI-exposed:
    --versions (default 6)
    --force
    --within-max-time (e.g. "1.30", "1m30s", "1:30", "90s")
    --within-max-pauses (integer)
    --between-max-time
    --between-max-pauses
    (optional overrides: --input-dir, --output-dir, --seed)

Behavior:
- Always inserts intra-file pauses (within) and inter-file pauses (between),
  except the first file is exempt from both.
- Pause durations sampled uniformly between 1s and the provided max time.
- Number of intra pauses per file is random between 1 and within-max-pauses (capped by gaps).
- Number of between-file pauses per gap is random between 1 and between-max-pauses.
- Filenames: {TOTALm}_v{VERSION}_<parts>.json
"""
from pathlib import Path
import argparse
import json
import glob
import random
from zipfile import ZipFile
import sys
import re

# Hardcoded defaults
DEFAULT_INPUT_DIR = "input"
DEFAULT_OUTPUT_DIR = "output"
DEFAULT_SEED = 12345

# ---------- time parsing ----------
def parse_time_str_to_seconds(s: str):
    if s is None:
        raise ValueError("time string is None")
    s0 = str(s).strip().lower()
    if not s0:
        raise ValueError("empty time string")

    mdot = re.match(r'^(\d+)\.(\d{1,2})$', s0)
    if mdot:
        mins = int(mdot.group(1))
        secs = int(mdot.group(2))
        if secs >= 60:
            raise ValueError(f"seconds part must be <60 in '{s}'")
        return mins * 60 + secs

    mcol = re.match(r'^(\d+):(\d{1,2})$', s0)
    if mcol:
        mins = int(mcol.group(1)); secs = int(mcol.group(2))
        if secs >= 60:
            raise ValueError(f"seconds part must be <60 in '{s}'")
        return mins * 60 + secs

    m = re.match(r'^(?:(\d+)m)?\s*(?:(\d+)s)?$', s0)
    if m and (m.group(1) or m.group(2)):
        mm = int(m.group(1)) if m.group(1) else 0
        ss = int(m.group(2)) if m.group(2) else 0
        return mm * 60 + ss

    if re.match(r'^\d+(\.\d+)?s?$', s0):
        s2 = s0[:-1] if s0.endswith('s') else s0
        val = float(s2)
        return int(round(val))

    raise ValueError(f"Could not parse time value '{s}'")

# ---------- I/O helpers ----------
def load_json(path: Path):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as e:
        print(f"WARNING: cannot parse {path}: {e}", file=sys.stderr)
        return None

def normalize_json(data):
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("events","items","entries","records","actions","eventsList","events_array"):
            if key in data and isinstance(data[key], list):
                return data[key]
        if "events" in data and isinstance(data["events"], list):
            return data["events"]
        return [data]
    return []

def part_from_filename(fname: str):
    stem = Path(fname).stem
    return ''.join(ch for ch in stem if ch.isalnum())

def find_groups(input_dir: Path):
    if not input_dir.exists():
        print(f"ERROR: Input directory '{input_dir}' does not exist.", file=sys.stderr)
        sys.exit(1)
    return [p for p in sorted(input_dir.iterdir()) if p.is_dir()]

def find_json_files(group_path: Path):
    return sorted(glob.glob(str(group_path / "*.json")))

def apply_shifts(events, shift_ms):
    shifted = []
    for e in events:
        try:
            t = int(e.get("Time", 0))
        except Exception:
            try:
                t = int(float(e.get("Time", 0)))
            except:
                t = 0
        new_event = {
            "Type": e.get("Type"),
            "Time": t + int(shift_ms),
            "X": e.get("X"),
            "Y": e.get("Y"),
            "Delta": e.get("Delta"),
            "KeyCode": e.get("KeyCode")
        }
        shifted.append(new_event)
    return shifted

def get_previously_processed_files(zip_path: Path):
    processed = set()
    if zip_path.exists():
        try:
            with ZipFile(zip_path, "r") as zf:
                for name in zf.namelist():
                    if name.endswith(".json") and not name.endswith("_log.txt"):
                        processed.add(Path(name).name)
        except:
            pass
    return processed

# ---------- core merging ----------
# ... rest of your merging logic remains unchanged ...
# generate_version(), parse_args(), main() remain exactly the same

def parse_args():
    p = argparse.ArgumentParser(description="Merge macros - compact UI (within & between pauses).")
    p.add_argument("--versions", type=int, default=6, help="How many versions per group")
    p.add_argument("--force", action="store_true", help="Force reprocessing even if outputs exist")
    p.add_argument("--within-max-time", required=True, help="Within-file max pause time")
    p.add_argument("--within-max-pauses", type=int, required=True, help="Max number of pauses inside each file")
    p.add_argument("--between-max-time", required=True, help="Between-files max pause time")
    p.add_argument("--between-max-pauses", type=int, required=True, help="Max number of pauses between files")
    p.add_argument("--input-dir", default=DEFAULT_INPUT_DIR, help=argparse.SUPPRESS)
    p.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help=argparse.SUPPRESS)
    p.add_argument("--seed", type=int, default=DEFAULT_SEED, help=argparse.SUPPRESS)
    return p.parse_args()

def main():
    args = parse_args()
    try:
        args.within_max_secs = parse_time_str_to_seconds(args.within_max_time)
        args.between_max_secs = parse_time_str_to_seconds(args.between_max_time)
    except Exception as e:
        print("ERROR parsing time inputs:", e, file=sys.stderr)
        sys.exit(2)

    if args.within_max_secs < 1 or args.between_max_secs < 1:
        print("ERROR: max times must represent at least 1 second", file=sys.stderr)
        sys.exit(2)
    if args.within_max_pauses < 1 or args.between_max_pauses < 1:
        print("ERROR: max pauses must be >= 1", file=sys.stderr)
        sys.exit(2)

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    base_seed = args.seed
    processed = get_previously_processed_files(output_dir / "merged_bundle.zip")
    global_pauses = set()
    zip_items = []

    for gi, grp in enumerate(find_groups(input_dir)):
        files = find_json_files(grp)
        if not files:
            continue
        if not args.force and all(Path(f).name in processed for f in files):
            continue

        log = {"group": grp.name, "versions": []}
        for v in range(1, args.versions + 1):
            version_rng = random.Random(base_seed + gi*1000 + v)
            fname, merged, finals, pauses, excl, total = generate_version(
                files, version_rng, base_seed, v, args, global_pauses
            )
            if not fname:
                continue
            out_file_path = output_dir / fname
            with open(out_file_path, "w", encoding="utf-8") as fh:
                json.dump(merged, fh, indent=2, ensure_ascii=False)
            zip_items.append((grp.name, out_file_path))
            log["versions"].append({
                "version": v,
                "filename": fname,
                "excluded": [Path(x).name for x in excl],
                "final_order": [Path(x).name for x in finals],
                "pause_details": pauses,
                "total_minutes": total
            })

        log_file_path = output_dir / f"{grp.name}_log.txt"
        with open(log_file_path, "w", encoding="utf-8") as fh:
            json.dump(log, fh, indent=2, ensure_ascii=False)
        zip_items.append((grp.name, log_file_path))

    zip_path = output_dir / "merged_bundle.zip"
    with ZipFile(zip_path, "w") as zf:
        for group_name, file_path in zip_items:
            zf.write(file_path, arcname=f"{group_name}/{file_path.name}")

    print("DONE. Outputs in:", output_dir)

if __name__ == "__main__":
    main()
