#!/usr/bin/env python3
"""
merge_macros.py  (auto-defaults + elastic validation)

This version keeps the same command-line arguments expected by existing workflows/UI but:
 - provides defaults for --input-dir and --output-dir so it can be run without args
 - validates intra-file min/max and makes them elastic: if the provided max is less than the configured min,
   the script will shrink the min to match the max (so the code remains functional)
 - fixes total-time computation so filename totals include inter-file pauses
 - keeps all UI inputs unchanged (no new CLI flags added)
"""

from pathlib import Path
import argparse
import json
import glob
import random
from copy import deepcopy
from zipfile import ZipFile
import sys

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input-dir", default="originals",
                   help="Parent directory containing group subfolders (default: originals)")
    p.add_argument("--output-dir", default="output",
                   help="Directory to write merged files and ZIP (default: output)")
    p.add_argument("--versions", type=int, default=5, help="Number of versions per group (default: 5)")
    p.add_argument("--seed", type=int, default=None, help="Base random seed (optional)")
    p.add_argument("--force", action="store_true", help="Force processing groups even if previously processed")
    p.add_argument("--exclude-count", type=int, default=1, help="How many files to randomly exclude per version")
    p.add_argument("--intra-file-enabled", action="store_true", help="Enable intra-file random pauses")
    p.add_argument("--intra-file-max", type=int, default=4, help="Max intra-file pauses per file (default:4)")
    p.add_argument("--intra-file-min-mins", type=int, default=1, help="Min intra-file pause length (minutes)")
    p.add_argument("--intra-file-max-mins", type=int, default=3, help="Max intra-file pause length (minutes)")
    return p.parse_args()

def load_json(path: Path):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as e:
        print(f"WARNING: cannot parse {path}: {e}", file=sys.stderr)
        return None

def normalize_json(data):
    if isinstance(data, dict) and "events" in data and isinstance(data["events"], list):
        return data["events"]
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in ("items", "entries", "records", "actions", "eventsList", "events_array"):
            if k in data and isinstance(data[k], list):
                return data[k]
        return [data]
    return []

def find_groups(input_dir: Path):
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
            except Exception:
                t = 0
        new = {
            "Type": e.get("Type"),
            "Time": t + int(shift_ms),
            "X": e.get("X"),
            "Y": e.get("Y"),
            "Delta": e.get("Delta"),
            "KeyCode": e.get("KeyCode"),
        }
        shifted.append(new)
    return shifted

def get_previously_processed_files(zip_path: Path):
    processed = set()
    if zip_path.exists():
        try:
            with ZipFile(zip_path, "r") as zf:
                for name in zf.namelist():
                    if name.endswith(".json") and not name.endswith("_log.txt"):
                        processed.add(Path(name).name)
        except Exception:
            pass
    return processed

def part_from_filename(fname: str):
    stem = Path(fname).stem
    return "".join(ch for ch in stem if ch.isalnum())

def insert_intra_pauses_fixed(events, rng, max_pauses, min_minutes, max_minutes, intra_log):
    if not events or max_pauses <= 0:
        return deepcopy(events)
    evs = deepcopy(events)
    n = len(evs)
    if n < 2:
        return evs
    k = rng.randint(0, min(max_pauses, n - 1))
    if k == 0:
        return evs
    chosen = rng.sample(range(n - 1), k)
    for gap_idx in sorted(chosen):
        pause_ms = rng.randint(min_minutes, max_minutes) * 60000
        for j in range(gap_idx + 1, n):
            evs[j]["Time"] = int(evs[j].get("Time", 0)) + pause_ms
        intra_log.append({"after_event_index": gap_idx, "pause_ms": pause_ms})
    return evs

def generate_version(files, seed, global_pause_set, version_num, exclude_count,
                     intra_enabled, intra_max, intra_min_mins, intra_max_mins):
    rng = random.Random(seed)
    if not files:
        return None, [], [], {}, [], 0

    m = len(files)
    exclude_count = max(0, min(exclude_count, m - 1))
    excluded = rng.sample(files, k=exclude_count) if exclude_count else []
    included = [f for f in files if f not in excluded]

    # Safe duplication and extra insertion logic
    dup_files = []
    if included:
        dup_count = min(2, len(included))
        if len(included) == 1:
            dup_files = [included[0]]
        else:
            dup_files = rng.sample(included, k=dup_count)

    final_files = included + dup_files

    if included:
        try:
            choice_k = rng.choice([1, 2])
            if len(included) >= choice_k:
                extra_files = rng.sample(included, k=choice_k)
            else:
                extra_files = included.copy()
        except Exception:
            extra_files = []
        for ef in extra_files:
            pos = rng.randrange(len(final_files) + 1)
            if pos > 0 and final_files[pos - 1] == ef:
                pos = min(pos + 1, len(final_files))
            final_files.insert(min(pos, len(final_files)), ef)

    rng.shuffle(final_files)

    merged = []
    pause_log = {"inter_file_pauses": [], "intra_file_pauses": []}
    time_cursor = 0
    play_times = {}

    for idx, f in enumerate(final_files):
        evs = normalize_json(load_json(Path(f)) or [])
        intra_log_local = []
        if intra_enabled and evs:
            intra_rng = random.Random((hash(f) & 0xFFFFFFFF) ^ (seed + version_num))
            evs = insert_intra_pauses_fixed(evs, intra_rng, intra_max,
                                            intra_min_mins, intra_max_mins, intra_log_local)
        if intra_log_local:
            pause_log["intra_file_pauses"].append({"file": Path(f).name, "pauses": intra_log_local})

        # inter-file pause before appending this file (60% chance)
        if rng.random() < 0.6:
            while True:
                inter_ms = rng.randint(120000, 180000) if idx == 0 else rng.randint(120000, 780000)
                if inter_ms not in global_pause_set:
                    global_pause_set.add(inter_ms)
                    break
            time_cursor += inter_ms
            pause_log["inter_file_pauses"].append({"after_file": Path(f).name,
                                                     "pause_ms": inter_ms,
                                                     "is_before_index": idx})

        shifted = apply_shifts(evs, time_cursor)
        merged.extend(shifted)
        if shifted:
            max_t = max(int(e.get("Time", 0)) for e in shifted)
            min_t = min(int(e.get("Time", 0)) for e in shifted)
            play_times[f] = max_t - min_t
            time_cursor = max_t + 30
        else:
            play_times[f] = 0

    parts = [part_from_filename(f) + f"[{round((play_times[f] or 0) / 60000)}m] " for f in final_files]

    # Compute total minutes from merged events (includes pauses)
    if merged:
        min_t = min(int(e.get("Time", 0)) for e in merged)
        max_t = max(int(e.get("Time", 0)) for e in merged)
        total_minutes = round((max_t - min_t) / 60000)
    else:
        total_minutes = 0

    merged_fname = "".join(parts).rstrip() + f"_v{version_num}[{total_minutes}m].json"
    return merged_fname, merged, final_files, pause_log, excluded, total_minutes

def main():
    a = parse_args()

    # Elastic validation: ensure intra-file min <= max; if max < min, shrink min to max and warn
    if a.intra_file_max_mins < a.intra_file_min_mins:
        print(f"NOTICE: intra-file max ({a.intra_file_max_mins}m) < intra-file min ({a.intra_file_min_mins}m). "
              f"Shrinking min to {a.intra_file_max_mins} to remain elastic.", file=sys.stderr)
        a.intra_file_min_mins = a.intra_file_max_mins

    in_dir, out_dir = Path(a.input_dir), Path(a.output_dir)

    # Ensure output folder exists
    out_dir.mkdir(parents=True, exist_ok=True)

    base_seed = a.seed if a.seed is not None else random.randrange(2**31)
    processed = get_previously_processed_files(out_dir / "merged_bundle.zip")
    global_pauses = set()
    zip_items = []

    for gi, grp in enumerate(find_groups(in_dir)):
        files = find_json_files(grp)
        if not files:
            continue
        if not a.force and all(Path(f).name in processed for f in files):
            continue

        log = {"group": grp.name, "versions": []}
        for v in range(1, a.versions + 1):
            fname, merged, finals, pauses, excl, total = generate_version(
                files, base_seed + gi * 1000 + v, global_pauses, v,
                a.exclude_count, a.intra_file_enabled, a.intra_file_max,
                a.intra_file_min_mins, a.intra_file_max_mins)
            if not fname:
                continue
            out_file_path = out_dir / fname
            with open(out_file_path, "w", encoding="utf-8") as fh:
                json.dump(merged, fh, indent=2, ensure_ascii=False)
            zip_items.append((grp.name, out_file_path))
            log["versions"].append({"version": v, "filename": fname,
                                     "excluded": [Path(x).name for x in excl],
                                     "final_order": [Path(x).name for x in finals],
                                     "pause_details": pauses,
                                     "total_minutes": total})

        log_file_path = out_dir / f"{grp.name}_log.txt"
        with open(log_file_path, "w", encoding="utf-8") as fh:
            json.dump(log, fh, indent=2, ensure_ascii=False)
        zip_items.append((grp.name, log_file_path))

    # Create ZIP
    zip_path = out_dir / "merged_bundle.zip"
    with ZipFile(zip_path, "w") as zf:
        for group_name, file_path in zip_items:
            zf.write(file_path, arcname=f"{group_name}/{file_path.name}")

    print("DONE.")

if __name__ == "__main__":
    main()
