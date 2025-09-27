#!/usr/bin/env python3
"""
merge_macros.py

Updated: naming includes alphanumeric (letters + numbers) and times in filename are minutes (rounded).
Other features:
 - Groups = subfolders in --input-dir
 - Multiple versions per group (--versions)
 - Random exclusion (--exclude-count), duplicates, extra copies
 - Inter-file pauses (60% chance), unique across run; first-file pause if chosen is 2..3 minutes
 - Optional intra-file pauses (per-file up to --intra-file-max, durations in minutes)
 - Writes merged versions as top-level JSON arrays
 - Per-group logs as .txt (JSON inside)
 - Outputs a ZIP with each group's files and its log in a folder
 - Deterministic via --seed
"""

from pathlib import Path
import argparse
import json
import glob
import random
from copy import deepcopy
from zipfile import ZipFile
import sys
import math

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input-dir", required=True, help="Parent directory containing group subfolders")
    p.add_argument("--output-dir", required=True, help="Directory to write merged files and ZIP")
    p.add_argument("--versions", type=int, default=5, help="Number of versions per group")
    p.add_argument("--seed", type=int, default=None, help="Base random seed (optional)")
    p.add_argument("--force", action="store_true", help="Force processing groups even if previously processed")
    p.add_argument("--exclude-count", type=int, default=1, help="How many files to randomly exclude per version (default 1)")
    # Intra-file pause options
    p.add_argument("--intra-file-enabled", action="store_true", help="Enable intra-file random pauses (default off)")
    p.add_argument("--intra-file-max", type=int, default=4, help="Max number of intra-file pauses per file (default 4)")
    p.add_argument("--intra-file-min-mins", type=int, default=1, help="Min intra-file pause length (minutes) default 1")
    p.add_argument("--intra-file-max-mins", type=int, default=3, help="Max intra-file pause length (minutes) default 3")
    return p.parse_args()

def load_json(path: Path):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as e:
        print(f"WARNING: cannot parse {path}: {e}", file=sys.stderr)
        return None

def normalize_json(data):
    """Return list of events from possible shapes."""
    if isinstance(data, dict) and "events" in data and isinstance(data["events"], list):
        return data["events"]
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in ("items","entries","records","actions","eventsList","events_array"):
            if k in data and isinstance(data[k], list):
                return data[k]
        return [data]
    return []

def find_groups(input_dir: Path):
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")
    return [p for p in sorted(input_dir.iterdir()) if p.is_dir()]

def find_json_files(group_path: Path):
    return sorted(glob.glob(str(group_path / "*.json")))

def apply_shifts(events, shift_ms):
    shifted = []
    for e in events:
        t = e.get("Time", 0)
        try:
            t_int = int(t)
        except Exception:
            try:
                t_int = int(float(t))
            except Exception:
                t_int = 0
        new = {
            "Type": e.get("Type"),
            "Time": t_int + int(shift_ms),
            "X": e.get("X"),
            "Y": e.get("Y"),
            "Delta": e.get("Delta"),
            "KeyCode": e.get("KeyCode")
        }
        shifted.append(new)
    return shifted

def get_previously_processed_files(zip_path: Path):
    processed = set()
    if zip_path.exists():
        try:
            with ZipFile(zip_path, "r") as zf:
                for name in zf.namelist():
                    # treat only JSON merged files (ignore group logs)
                    if name.endswith(".json") and not name.endswith("_log.json") and not name.endswith("_log.txt"):
                        processed.add(Path(name).name)
        except Exception:
            pass
    return processed

def alnum_part_from_filename(fname: str):
    """
    Return 3-char alphanumeric part (lowercase), pad with 'x' if needed.
    Extracts A-Z, a-z, 0-9 only and lowercases letters.
    """
    s = ''.join(ch for ch in Path(fname).name if ch.isalnum())
    s = s.lower()
    if len(s) >= 3:
        return s[:3]
    if len(s) == 2:
        return s + 'x'
    if len(s) == 1:
        return s + 'xx'
    return 'xxx'

def insert_intra_pauses_fixed(events, rng, max_pauses, min_minutes, max_minutes, intra_log):
    """
    Insert up to max_pauses random pauses inside events.
    For n events there are n-1 gaps (after index 0..n-2).
    Choose k = random.randint(0, max_pauses) distinct gaps (k <= n-1),
    then for each gap chosen (ascending) pick duration in minutes [min,max],
    convert to ms and shift subsequent events forward by that ms.
    """
    if not events or max_pauses <= 0:
        return deepcopy(events)
    evs = deepcopy(events)
    n = len(evs)
    possible_gaps = list(range(0, max(0, n-1)))
    if not possible_gaps:
        return evs
    k = rng.randint(0, max_pauses)
    k = min(k, len(possible_gaps))
    if k == 0:
        return evs
    chosen = rng.sample(possible_gaps, k=k)
    chosen.sort()
    for gap_idx in chosen:
        pause_minutes = rng.randint(min_minutes, max_minutes)
        pause_ms = pause_minutes * 60 * 1000
        for j in range(gap_idx + 1, n):
            t = evs[j].get("Time", 0)
            try:
                t_int = int(t)
            except Exception:
                try:
                    t_int = int(float(t))
                except Exception:
                    t_int = 0
            evs[j]["Time"] = t_int + pause_ms
        intra_log.append({"after_event_index": gap_idx, "pause_ms": pause_ms})
    return evs

def generate_version(files, seed, global_pause_set, version_num, exclude_count,
                     intra_enabled, intra_max, intra_min_mins, intra_max_mins):
    rng = random.Random(seed)
    m = len(files)
    if m == 0:
        return None, [], [], {}, [], 0
    # Cap exclude_count
    if exclude_count < 0:
        exclude_count = 0
    if exclude_count >= m:
        exclude_count = max(0, m - 1)

    excluded = rng.sample(files, k=exclude_count) if exclude_count > 0 else []
    included = [f for f in files if f not in excluded]

    # choose duplicates
    if len(included) >= 2:
        dup_files = rng.sample(included, 2)
    elif len(included) == 1:
        dup_files = [included[0], included[0]]
    else:
        dup_files = rng.sample(files, min(2, len(files)))
        while len(dup_files) < 2 and files:
            dup_files.append(files[0])

    final_files = deepcopy(included) + dup_files

    # add extras (1 or 2)
    extra_count = rng.choice([1,2]) if included else 0
    extra_candidates = [f for f in included if f not in dup_files]
    if extra_candidates and extra_count > 0:
        k = min(extra_count, len(extra_candidates))
        extra_files = rng.sample(extra_candidates, k=k)
        for ef in extra_files:
            pos = rng.randrange(len(final_files) + 1)
            if pos > 0 and final_files[pos-1] == ef:
                pos += 1
            if pos > len(final_files):
                pos = len(final_files)
            final_files.insert(pos, ef)

    rng.shuffle(final_files)

    merged_events = []
    version_pause_log = {"inter_file_pauses": [], "intra_file_pauses": []}
    time_cursor = 0

    for idx, fpath in enumerate(final_files):
        raw = load_json(Path(fpath))
        evs = normalize_json(raw) or []

        # Intra-file pauses (up to intra_max), deterministic per file/version
        intra_log_local = []
        if intra_enabled and evs:
            fname_seed = (hash(fpath) & 0xffffffff) ^ (seed + version_num)
            intra_rng = random.Random(fname_seed)
            evs = insert_intra_pauses_fixed(evs, intra_rng, intra_max, intra_min_mins, intra_max_mins, intra_log_local)

        if intra_log_local:
            version_pause_log["intra_file_pauses"].append({"file": Path(fpath).name, "pauses": intra_log_local})

        # Inter-file pause: 60% chance. If this is the *first file* (idx==0) and a pause is chosen,
        # choose it from 2..3 minutes (special rule). Otherwise use 2..13 minutes.
        add_inter = rng.random() < 0.6
        inter_pause_ms = None
        if add_inter:
            attempt = 0
            while True:
                if idx == 0:
                    inter_pause_ms = rng.randint(2*60*1000, 3*60*1000)  # 2..3 minutes
                else:
                    inter_pause_ms = rng.randint(2*60*1000, 13*60*1000)  # 2..13 minutes
                attempt += 1
                if inter_pause_ms not in global_pause_set:
                    global_pause_set.add(inter_pause_ms)
                    break
                if attempt > 1000:
                    inter_pause_ms += attempt
                    if inter_pause_ms not in global_pause_set:
                        global_pause_set.add(inter_pause_ms)
                        break
            time_cursor += inter_pause_ms
            version_pause_log["inter_file_pauses"].append({"after_file": Path(fpath).name, "pause_ms": inter_pause_ms, "is_before_index": idx})

        shifted = apply_shifts(evs, time_cursor)
        merged_events.extend(shifted)
        max_t = max((e.get("Time", 0) for e in shifted), default=time_cursor)
        time_cursor = int(max_t) + 30

    # compute total play time (ms): last_time - first_time (includes initial pause if any)
    if merged_events:
        times = [int(e.get("Time", 0)) for e in merged_events]
        min_t = min(times)
        max_t = max(times)
        total_play_time_ms = max_t - min_t
    else:
        total_play_time_ms = 0

    # convert to minutes and round to nearest integer
    total_play_time_mins = int(round(total_play_time_ms / 60000.0))

    # build alphanumeric filename parts and join, then insert minutes before version suffix
    parts = [alnum_part_from_filename(Path(f).name) + str(total_play_time_mins) for f in final_files]
    version_filename = "".join(parts) + f"_v{version_num}.json"

    return version_filename, merged_events, final_files, version_pause_log, excluded, total_play_time_mins

def main():
    args = parse_args()
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    base_seed = args.seed if args.seed is not None else random.randrange(2**31)
    zip_path = output_dir / "merged_bundle.zip"
    already_processed = get_previously_processed_files(zip_path)

    shared_global_pause_set = set()
    zip_items = []

    for g_idx, group in enumerate(find_groups(input_dir)):
        gname = group.name
        files = find_json_files(group)
        if not files:
            print(f"Skipping group {gname}: no JSON files found")
            continue
        if (not args.force) and all(Path(f).name in already_processed for f in files):
            print(f"Skipping group {gname}: all files previously processed (use --force to override)")
            continue

        group_log = {"group": gname, "versions": []}

        for v in range(1, args.versions + 1):
            seed = base_seed + g_idx * 1000 + v
            version_filename, merged_events, final_files, pause_log, excluded, total_mins = generate_version(
                files=files,
                seed=seed,
                global_pause_set=shared_global_pause_set,
                version_num=v,
                exclude_count=args.exclude_count,
                intra_enabled=args.intra_file_enabled,
                intra_max=args.intra_file_max,
                intra_min_mins=args.intra_file_min_mins,
                intra_max_mins=args.intra_file_max_mins
            )

            if version_filename is None:
                continue

            version_path = output_dir / version_filename
            with open(version_path, "w", encoding="utf-8") as fh:
                json.dump(merged_events, fh, indent=2, ensure_ascii=False)
            zip_items.append((gname, version_path))

            group_log["versions"].append({
                "version": v,
                "filename": version_filename,
                "total_play_time_mins": total_mins,
                "excluded_files": [Path(x).name for x in excluded],
                "final_order": [Path(x).name for x in final_files],
                "pause_details": pause_log
            })

        # write group log as .txt
        group_log_filename = f"{gname}_log.txt"
        group_log_path = output_dir / group_log_filename
        with open(group_log_path, "w", encoding="utf-8") as fh:
            json.dump(group_log, fh, indent=2, ensure_ascii=False)
        zip_items.append((gname, group_log_path))

    # create ZIP with per-group folders
    zip_file_path = output_dir / "merged_bundle.zip"
    with ZipFile(zip_file_path, "w") as zf:
        for group_name, p in zip_items:
            arcname = f"{group_name}/{p.name}" if group_name else p.name
            zf.write(p, arcname=arcname)

    print(f"Wrote ZIP: {zip_file_path}")
    print("DONE.")

if __name__ == "__main__":
    main()
