#!/usr/bin/env python3
"""
merge_macros.py — patched

Behavior:
 - Groups are subfolders of --input-dir (direct children). Each group is processed separately.
 - JSON files are discovered RECURSIVELY inside each group folder (fixes missing files inside nested fused_bundle folders).
 - Keeps previous features: versions per group, deterministic --seed, random exclusions, duplicates & extra copies,
   intra-file pauses (optional), inter-file pauses, shifts event times, outputs top-level JSON arrays,
   creates a zip with per-group folders, and persists deterministic behaviour with --seed.
 - Hardened against sampling errors and parsing edge cases.
"""

from pathlib import Path
import argparse
import json
import random
from copy import deepcopy
from zipfile import ZipFile
import sys
import math

# ----- Argument parsing ----------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Merge macro JSONs per-group (recursively).")
    p.add_argument("--input-dir", required=True, help="Parent directory containing group subfolders")
    p.add_argument("--output-dir", required=True, help="Directory to write merged files and ZIP")
    p.add_argument("--versions", type=int, default=5, help="Number of versions per group")
    p.add_argument("--seed", type=int, default=None, help="Base random seed (optional)")
    p.add_argument("--force", action="store_true", help="Force processing groups even if previously processed")
    p.add_argument("--exclude-count", type=int, default=1, help="How many files to randomly exclude per version (0..N-1)")
    p.add_argument("--intra-file-enabled", action="store_true", help="Enable intra-file random pauses")
    p.add_argument("--intra-file-max", type=int, default=4, help="Max intra-file pauses per file")
    p.add_argument("--intra-file-min-mins", type=int, default=1, help="Min intra-file pause minutes")
    p.add_argument("--intra-file-max-mins", type=int, default=3, help="Max intra-file pause minutes")
    return p.parse_args()

# ----- JSON load & normalization -------------------------------------------
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
        for k in ("items","entries","records","actions","eventsList","events_array"):
            if k in data and isinstance(data[k], list):
                return data[k]
        # treat a single dict with Time as a single-event list
        if "Time" in data:
            return [data]
        return [data]
    return []

# ----- filesystem helpers --------------------------------------------------
def find_groups(input_dir: Path):
    """
    Return sorted list of direct child directories of input_dir.
    Each child directory is treated as a "group".
    """
    if not input_dir.exists():
        return []
    return [p for p in sorted(input_dir.iterdir()) if p.is_dir()]

def find_json_files(group_path: Path, output_dir: Path = None):
    """
    Recursively find all .json files under group_path (including nested folders).
    Optionally ignore any files that are inside output_dir to avoid re-processing outputs.
    Returns absolute Path strings.
    """
    files = []
    for p in sorted(group_path.rglob("*.json")):
        # skip files that are inside the output_dir (if provided)
        if output_dir is not None:
            try:
                if output_dir.resolve() in p.resolve().parents or p.resolve().is_relative_to(output_dir.resolve()):
                    continue
            except Exception:
                # older Python may not have is_relative_to; fall back to parent check above
                pass
        files.append(str(p.resolve()))
    return files

# ----- time / event helpers -----------------------------------------------
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
        new = dict(e)
        new["Time"] = t + int(shift_ms)
        shifted.append(new)
    return shifted

def insert_intra_pauses_fixed(events, rng, max_pauses, min_minutes, max_minutes, intra_log):
    """
    Insert up to k pauses (randomly chosen positions between events) inside the given events list.
    The function returns a new list with shifted times and appends pause details to intra_log list.
    """
    if not events or max_pauses <= 0:
        return deepcopy(events)
    evs = deepcopy(events)
    n = len(evs)
    if n < 2:
        return evs
    k = rng.randint(0, min(max_pauses, n-1))
    if k == 0:
        return evs
    chosen = rng.sample(range(n-1), k)
    for gap_idx in sorted(chosen):
        pause_ms = rng.randint(min_minutes * 60000, max_minutes * 60000)
        for j in range(gap_idx+1, n):
            evs[j]["Time"] = int(evs[j].get("Time", 0)) + pause_ms
        intra_log.append({"after_event_index": gap_idx, "pause_ms": pause_ms})
    return evs

# ----- filename / token helpers -------------------------------------------
def part_from_filename(fname: str):
    # Keep all letters and numbers from the filename **without the extension**
    stem = Path(fname).stem
    return ''.join(ch for ch in stem if ch.isalnum())

def minutes_from_ms(ms):
    return round(ms/60000)

# ----- processed detection (when using zipped previous outputs) ------------
def get_previously_processed_files(zip_path: Path):
    """
    If a zip exists at zip_path, read its contents and return the set of top-level JSON filenames
    previously produced. This is used to optionally skip re-processing files if --force is not set.
    """
    processed = set()
    if zip_path.exists():
        try:
            with ZipFile(zip_path, "r") as zf:
                for name in zf.namelist():
                    if name.endswith(".json"):
                        processed.add(Path(name).name)
        except Exception:
            pass
    return processed

# ----- version generation --------------------------------------------------
def generate_version(files, seed, global_pause_set, version_num, exclude_count,
                     intra_enabled, intra_max, intra_min_mins, intra_max_mins):
    """
    Compose a merged version from provided file paths (absolute strings).
    Returns (merged_filename, merged_events_list, final_files_order, pause_log, excluded_list, total_minutes)
    """
    rng = random.Random(seed)
    if not files:
        return None, [], [], {}, [], 0

    # sanitize exclude_count
    m = len(files)
    exclude_count = max(0, min(exclude_count, max(0, m-1)))
    excluded = rng.sample(files, k=exclude_count) if exclude_count and m>0 else []
    included = [f for f in files if f not in excluded]

    # choose 0-2 duplicate files from included (if included empty, allow duplicates from files)
    dup_candidates = included or files
    dup_count = min(2, len(dup_candidates))
    dup_files = rng.sample(dup_candidates, k=dup_count) if dup_candidates else []

    final_files = included + dup_files

    # optionally insert extra copies of some included files (extra non-adjacent insertion)
    if included:
        # choose either 1 or 2 extra copy insertions but ensure sample bounds
        extra_k = rng.choice([1,2]) if len(included) >= 1 else 0
        extra_k = min(extra_k, len(included))
        extra_files = rng.sample(included, k=extra_k) if extra_k>0 else []
        for ef in extra_files:
            pos = rng.randrange(len(final_files)+1)
            # avoid placing a duplicate immediately after same file
            if pos > 0 and final_files[pos-1] == ef:
                pos = min(len(final_files), pos+1)
            final_files.insert(min(pos, len(final_files)), ef)

    # shuffle final order
    rng.shuffle(final_files)

    merged = []
    pause_log = {"inter_file_pauses":[], "intra_file_pauses":[]}
    time_cursor = 0
    play_times = {}

    for idx, f in enumerate(final_files):
        evs_raw = load_json(Path(f))
        evs = normalize_json(evs_raw) if evs_raw is not None else []
        intra_log_local = []
        if intra_enabled and evs:
            # deterministic intra RNG per-file+version
            intra_rng = random.Random((hash(f)&0xffffffff) ^ (seed + version_num))
            evs = insert_intra_pauses_fixed(evs, intra_rng, intra_max, intra_min_mins, intra_max_mins, intra_log_local)
        if intra_log_local:
            pause_log["intra_file_pauses"].append({"file": Path(f).name, "pauses": intra_log_local})

        # Decide whether to insert an inter-file pause BEFORE appending this file's events
        # (we follow the earlier logic: 60% chance to insert a pause; different ranges for first file)
        if rng.random() < 0.6:
            # first file shorter pause range or same rule? use 2-3 min for first, others 2-13 min per earlier description
            while True:
                if idx == 0:
                    inter_ms = rng.randint(120000, 180000)
                else:
                    inter_ms = rng.randint(120000, 780000)
                # ensure uniqueness across global pause set to avoid duplicates (if desired)
                if inter_ms not in global_pause_set:
                    global_pause_set.add(inter_ms)
                    break
            time_cursor += inter_ms
            pause_log["inter_file_pauses"].append({"after_file": Path(f).name, "pause_ms": inter_ms, "is_before_index": idx})

        shifted = apply_shifts(evs, time_cursor)
        merged.extend(shifted)

        if shifted:
            max_t = max(int(e.get("Time", 0)) for e in shifted)
            min_t = min(int(e.get("Time", 0)) for e in shifted)
            play_times[f] = max_t - min_t
            # set cursor to last event time plus small gap (30 ms)
            time_cursor = max_t + 30
        else:
            play_times[f] = 0

    # Build filename parts using sanitized part_from_filename + duration in minutes
    parts = [part_from_filename(Path(f).name) + f"[{minutes_from_ms(play_times[f])}m] " for f in final_files]
    total_minutes = max(0, round(sum(play_times.values()) / 60000))
    merged_fname = "".join(parts).rstrip() + f"_v{version_num}[{total_minutes}m].json"
    return merged_fname, merged, final_files, pause_log, excluded, total_minutes

# ----- main ----------------------------------------------------------------
def main():
    args = parse_args()
    in_dir = Path(args.input_dir)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    base_seed = args.seed if args.seed is not None else random.randrange(2**31)
    prev_processed = get_previously_processed_files(out_dir/"merged_bundle.zip")
    global_pauses = set()
    zip_items = []

    groups = find_groups(in_dir)
    if not groups:
        print(f"No group directories found in {in_dir}", file=sys.stderr)
        return

    for gi, grp in enumerate(groups):
        # collect JSON files recursively within this group
        files = find_json_files(grp, output_dir=out_dir)
        if not files:
            print(f"Skipping group '{grp.name}' — no JSON files found inside (recursively).")
            continue

        # if not forcing and all files were previously processed, skip (old logic)
        if not args.force and all(Path(f).name in prev_processed for f in files):
            print(f"Skipping group '{grp.name}' — files already processed and --force not set.")
            continue

        # For each requested version: generate merged JSON and write
        for v in range(1, args.versions+1):
            fname, merged, finals, pauses, excl, total = generate_version(
                files, base_seed + gi*1000 + v, global_pauses, v,
                args.exclude_count, args.intra_file_enabled, args.intra_file_max,
                args.intra_file_min_mins, args.intra_file_max_mins)

            if not fname:
                continue

            # Where to place output: group-specific folder inside out_dir, mirroring group name
            group_out_dir = out_dir / grp.name
            group_out_dir.mkdir(parents=True, exist_ok=True)
            out_file_path = group_out_dir / fname
            try:
                with open(out_file_path, "w", encoding="utf-8") as fh:
                    # write top-level JSON array
                    json.dump(merged, fh, indent=2, ensure_ascii=False)
                zip_items.append((grp.name, out_file_path))
                # do not create separate per-group log files (user requested no logs earlier)
                print(f"Wrote merged version: {out_file_path} (total_minutes={total})")
            except Exception as e:
                print(f"ERROR writing {out_file_path}: {e}", file=sys.stderr)

    # Create a single ZIP with per-group folders inside the archive
    zip_path = out_dir / "merged_bundle.zip"
    try:
        with ZipFile(zip_path, "w") as zf:
            for group_name, file_path in zip_items:
                # arcname -> group_name/file_name.json
                arcname = f"{group_name}/{file_path.name}"
                zf.write(file_path, arcname=arcname)
        print(f"Created ZIP: {zip_path}")
    except Exception as e:
        print(f"ERROR creating ZIP {zip_path}: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
