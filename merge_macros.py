#!/usr/bin/env python3
"""
merge_macros.py

Same features as before, but merged version files are written as a top-level JSON array
(i.e. [...] ), compatible with programs that expect a list of MacroEvent objects.
Group logs are .txt. Outputs grouped into ZIP subfolders per group.
"""

import argparse
import json
import glob
import random
from pathlib import Path
from copy import deepcopy
from zipfile import ZipFile

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True, help="Parent directory containing macro groups")
    parser.add_argument("--output-dir", required=True, help="Directory to write merged versions and ZIP")
    parser.add_argument("--versions", type=int, default=5, help="Number of versions per group")
    parser.add_argument("--seed", type=int, default=None, help="Random seed")
    parser.add_argument("--force", action="store_true", help="Force processing all groups even if previously processed")
    return parser.parse_args()

def load_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"WARNING: Cannot parse {path}: {e}")
        return None

def normalize_json(data):
    if isinstance(data, dict) and "events" in data:
        return data["events"]
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in ["items","entries","records","actions","eventsList","events_array"]:
            if k in data and isinstance(data[k], list):
                return data[k]
        return [data]
    return []

def find_groups(input_dir):
    p = Path(input_dir)
    return [x for x in p.iterdir() if x.is_dir()]

def find_json_files(group_path):
    return sorted(glob.glob(str(group_path / "*.json")))

def apply_shifts(events, shift_ms):
    return [
        {
            "Type": e.get("Type"),
            "Time": int(e.get("Time", 0)) + int(shift_ms),
            "X": e.get("X"),
            "Y": e.get("Y"),
            "Delta": e.get("Delta"),
            "KeyCode": e.get("KeyCode")
        }
        for e in events
    ]

def get_previously_processed_files(zip_path):
    processed = set()
    if zip_path.exists():
        with ZipFile(zip_path, "r") as zf:
            for f in zf.namelist():
                if f.endswith(".json") and f != "bundle_log.json":
                    processed.add(Path(f).name)
    return processed

def generate_version(files, seed, global_pause_set, version_num):
    r = random.Random(seed)
    exclude_idx = r.randrange(len(files))
    included_files = [f for i,f in enumerate(files) if i != exclude_idx]

    # duplicate 2 files (must exist)
    dup_files = r.sample(included_files, 2)
    final_files = deepcopy(included_files) + dup_files

    # add 1 or 2 extra copies (not adjacent to same file if possible)
    extra_count = r.choice([1,2])
    extra_candidates = [f for f in included_files if f not in dup_files]
    extra_files = r.sample(extra_candidates, k=min(extra_count,len(extra_candidates)))

    for f in extra_files:
        insert_pos = r.randrange(len(final_files)+1)
        # adjust to avoid adjacent duplicate
        if insert_pos > 0 and final_files[insert_pos-1] == f:
            insert_pos += 1
        if insert_pos > len(final_files):
            insert_pos = len(final_files)
        final_files.insert(insert_pos, f)

    r.shuffle(final_files)

    merged_events = []
    pause_log = []
    time_cursor = 0
    for f in final_files:
        events = normalize_json(load_json(f)) or []
        add_pause = r.random() < 0.6
        pause_duration = None
        if add_pause:
            # pick unique pause (ms) between 2 and 13 minutes
            while True:
                pause_duration = r.randint(2*60*1000, 13*60*1000)
                if pause_duration not in global_pause_set:
                    global_pause_set.add(pause_duration)
                    break
            time_cursor += pause_duration
            pause_log.append({"after_file": Path(f).name, "pause_ms": pause_duration})
        shifted = apply_shifts(events, time_cursor)
        merged_events.extend(shifted)
        max_time = max((e.get("Time",0) for e in shifted), default=0)
        time_cursor = int(max_time) + 30

    filename_parts = [Path(f).name[:3] for f in final_files]
    version_filename = "".join(filename_parts) + f"_v{version_num}.json"

    return version_filename, merged_events, final_files, pause_log

def main():
    args = parse_args()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    global_pause_set = set()
    zip_files = []

    seed_base = args.seed if args.seed is not None else random.randrange(2**31)
    zip_path = out_dir / "merged_bundle.zip"
    already_processed = get_previously_processed_files(zip_path)

    for g_idx, g in enumerate(find_groups(args.input_dir)):
        g_name = g.name
        files = find_json_files(g)
        if not files:
            print(f"Skipping group {g_name} — no JSON files found")
            continue
        if not args.force and all(Path(f).name in already_processed for f in files):
            print(f"Skipping group {g_name} — all files already processed (use --force to override)")
            continue

        group_log = {"group": g_name, "versions": []}

        for v in range(1, args.versions+1):
            version_seed = seed_base + g_idx*1000 + v
            version_filename, merged_events, final_files, pause_log = generate_version(files, version_seed, global_pause_set, v)
            version_path = out_dir / version_filename

            # *** WRITE AS TOP-LEVEL ARRAY (compatible with SimplyMacro) ***
            with open(version_path, "w", encoding="utf-8") as f:
                json.dump(merged_events, f, indent=2, ensure_ascii=False)

            zip_files.append((g_name, version_path))

            group_log["versions"].append({
                "version": v,
                "filename": version_filename,
                "excluded_file": [Path(x).name for x in set(files)-set(final_files)],
                "duplicated_files": [Path(x).name for x in final_files],
                "pause_log": pause_log
            })

        # Save group-specific log as .txt (JSON-formatted for readability)
        group_log_filename = f"{g_name}_log.txt"
        group_log_path = out_dir / group_log_filename
        with open(group_log_path, "w", encoding="utf-8") as f:
            json.dump(group_log, f, indent=2, ensure_ascii=False)
        zip_files.append((g_name, group_log_path))

    # Create ZIP with each group's folder
    zip_path = out_dir / "merged_bundle.zip"
    with ZipFile(zip_path, "w") as zf:
        for group_name, f in zip_files:
            arcname = f"{group_name}/{f.name}" if group_name else f.name
            zf.write(f, arcname=arcname)

    print(f"Wrote ZIP: {zip_path}")
    print("DONE.")

if __name__ == "__main__":
    main()
