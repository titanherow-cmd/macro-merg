#!/usr/bin/env python3
"""
merge_macros.py

- Recursively finds groups under --input-dir (default "originals").
- Randomly excludes 1..--exclude-max files per version (clamped).
- Inserts intra-file pauses (except first file) and inter-file pauses (unique where possible).
- Avoids adjacent duplicates when inserting extra copies.
- Produces merged JSON files in --output-dir (default "output").
- Writes numbered zip bundles merged_bundle_1.zip, merged_bundle_2.zip, ... and also keeps merged_bundle.zip as "latest".
- Filenames start with a version letter prefix: V<letters> (1->'a' => Va), then _{TotalMinutes}m= <parts_joined>.json
  Parts are joined with `"- "` so the dash appears immediately after the closing bracket: e.g. a[1m]- b[2m]
"""
from pathlib import Path
import argparse
import json
import glob
import random
from zipfile import ZipFile
import sys
import re
import math
import os
import shutil

DEFAULT_INPUT_DIR = "originals"
DEFAULT_OUTPUT_DIR = "output"
DEFAULT_SEED = 12345
DEFAULT_EXCLUDE_MAX = 3

# ---------- utilities ----------
def index_to_letters(idx: int) -> str:
    """Convert 1-based index to letters: 1->'a', 2->'b', ..., 26->'z', 27->'aa'."""
    if idx < 1:
        return "a"
    s = ""
    n = idx
    while n > 0:
        n -= 1
        s = chr(ord('a') + (n % 26)) + s
        n //= 26
    return s

def parse_time_str_to_seconds(s: str) -> int:
    """Parse time strings like '1.30', '1:30', '1m30s', '90s', '2m' into seconds."""
    if s is None:
        raise ValueError("time string is None")
    s0 = str(s).strip().lower()
    if not s0:
        raise ValueError("empty time string")

    mdot = re.match(r'^(\d+)\.(\d{1,2})$', s0)
    if mdot:
        mins = int(mdot.group(1)); secs = int(mdot.group(2))
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

def part_from_filename(fname: str) -> str:
    stem = Path(fname).stem
    return ''.join(ch for ch in stem if ch.isalnum())

# --- recursive group discovery ---
def find_groups_recursive(input_dir: Path):
    groups = []
    for root, dirs, files in os.walk(input_dir):
        jsons = [f for f in files if f.lower().endswith(".json")]
        if jsons:
            groups.append(Path(root))
    groups = sorted(groups)
    return groups

def find_json_files(group_path: Path):
    return sorted(glob.glob(str(group_path / "*.json")))

def apply_shifts(events, shift_ms: int):
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

# --- helper to avoid adjacent duplicates ---
def _find_non_adjacent_insertion_pos(final_files, candidate):
    n = len(final_files)
    for pos in range(0, n+1):
        left_ok = True
        right_ok = True
        if pos-1 >= 0:
            left_ok = (final_files[pos-1] != candidate)
        if pos < n:
            right_ok = (final_files[pos] != candidate)
        if left_ok and right_ok:
            return pos
    for _ in range(min(10, n+1)):
        pos = random.randrange(0, n+1)
        if not ((pos-1>=0 and final_files[pos-1]==candidate) or (pos<n and final_files[pos]==candidate)):
            return pos
    return n//2

# ---------- core merging ----------
def generate_version(files, rng, seed_for_intra, version_num, args, global_pause_set):
    """
    files: list of file paths (strings)
    rng: per-version random.Random
    seed_for_intra: base seed for per-file intra RNG generation
    version_num: 1-based version index (used to derive letter prefix)
    """
    if not files:
        return None, [], [], {}, [], 0

    # RANDOM EXCLUSION
    if len(files) <= 1:
        excluded = []
        included = list(files)
    else:
        max_excl = min(args.exclude_max, len(files)-1)
        if max_excl >= 1:
            excl_count = rng.randint(1, max_excl)
            excluded = rng.sample(files, excl_count) if excl_count > 0 else []
        else:
            excluded = []
        included = [f for f in files if f not in excluded]

    if not included and files:
        included = [files[0]]
        excluded = [f for f in files if f != files[0]]

    # duplicates & extras
    population = included or files
    dup_count = min(2, len(population))
    dup_files = rng.sample(population, dup_count) if dup_count > 0 else []
    final_files = included + dup_files

    if included:
        k_choice = rng.choice([1,2])
        k = min(k_choice, len(included))
        if k > 0:
            extra_files = rng.sample(included, k=k) if k <= len(included) else included[:k]
            for ef in extra_files:
                pos = _find_non_adjacent_insertion_pos(final_files, ef)
                final_files.insert(min(pos, len(final_files)), ef)

    rng.shuffle(final_files)

    merged = []
    pause_log = {"inter_file_pauses": [], "intra_file_pauses": [], "post_file_buffers": []}
    time_cursor = 0
    play_times = {}

    within_max_ms = max(1000, args.within_max_secs * 1000)
    between_max_ms = max(1000, args.between_max_secs * 1000)
    within_max_p = max(1, args.within_max_pauses)
    between_max_p = max(1, args.between_max_pauses)

    for idx, f in enumerate(final_files):
        evs_raw = load_json(Path(f)) or []
        evs = normalize_json(evs_raw)
        intra_log_local = []

        # INTRA-FILE: skip for first file (idx==0)
        if idx != 0 and evs and len(evs) > 1:
            n_gaps = len(evs) - 1
            intra_rng = random.Random((hash(f) & 0xffffffff) ^ (seed_for_intra + version_num))
            chosen_count = intra_rng.randint(1, within_max_p)
            chosen_count = min(chosen_count, n_gaps)
            if chosen_count > 0:
                sample_k = min(chosen_count, n_gaps)
                chosen_gaps = intra_rng.sample(range(n_gaps), sample_k) if sample_k > 0 else []
                for gap_idx in sorted(chosen_gaps):
                    pause_ms = intra_rng.randint(1000, within_max_ms)
                    # shift subsequent events
                    for j in range(gap_idx+1, len(evs)):
                        evs[j]["Time"] = int(evs[j].get("Time", 0)) + pause_ms
                    intra_log_local.append({"after_event_index": gap_idx, "pause_ms": pause_ms})
        if intra_log_local:
            pause_log["intra_file_pauses"].append({"file": Path(f).name, "pauses": intra_log_local})

        # INTER-FILE: before this file (except before first)
        if idx != 0:
            k = rng.randint(1, between_max_p)
            k = min(k, 50)
            total_inter_ms = 0
            inter_list = []
            for i in range(k):
                attempts = 0
                inter_ms = None
                # try to pick a unique inter pause value for the run
                while attempts < 200:
                    candidate = rng.randint(1000, between_max_ms)
                    if candidate not in global_pause_set:
                        inter_ms = candidate
                        global_pause_set.add(candidate)
                        break
                    attempts += 1
                if inter_ms is None:
                    inter_ms = rng.randint(1000, between_max_ms)
                total_inter_ms += inter_ms
                inter_list.append(inter_ms)
            time_cursor += total_inter_ms
            pause_log["inter_file_pauses"].append({"before_file": Path(f).name, "pause_ms_list": inter_list, "is_before_index": idx})

        # apply shifts and append
        shifted = apply_shifts(evs, time_cursor)
        merged.extend(shifted)
        if shifted:
            max_t = max(int(e.get("Time", 0)) for e in shifted)
            min_t = min(int(e.get("Time", 0)) for e in shifted)
            play_times[f] = max_t - min_t

            # post-file buffer (10-30s)
            buffer_ms = rng.randint(10_000, 30_000)
            time_cursor = max_t + buffer_ms
            pause_log["post_file_buffers"].append({"file": Path(f).name, "buffer_ms": buffer_ms})
        else:
            play_times[f] = 0

    total_ms = sum(play_times.values())
    total_minutes = math.ceil(total_ms / 60000) if total_ms > 0 else 0

    parts_list = [part_from_filename(f) + f"[{math.ceil((play_times.get(f,0))/60000)}m]" for f in final_files]
    # join with dash immediately after bracket (no leading space), but keep a space after the dash
    parts_joined = "- ".join(parts_list)

    letters = index_to_letters(version_num)
    merged_fname = f"V{letters}_{total_minutes}m= {parts_joined}.json"

    return merged_fname, merged, final_files, pause_log, excluded, total_minutes

# ---------- CLI ----------
def parse_args():
    p = argparse.ArgumentParser(description="Merge macros - recursive group discovery (defaults to 'originals').")
    p.add_argument("--versions", type=int, default=6, help="How many versions per group")
    p.add_argument("--force", action="store_true", help="Force reprocessing even if outputs exist")
    p.add_argument("--within-max-time", required=True, help="Within-file max pause time (e.g. 1.30, 1m30s, 1:30)")
    p.add_argument("--within-max-pauses", type=int, required=True, help="Max number of pauses to add inside each file")
    p.add_argument("--between-max-time", required=True, help="Between-files max pause time (e.g. 10s, 1m)")
    p.add_argument("--between-max-pauses", type=int, required=True, help="Max number of pauses to insert between files (usually 1)")
    p.add_argument("--exclude-max", type=int, default=DEFAULT_EXCLUDE_MAX, help="Maximum number of files to randomly exclude per version (min 1)")
    p.add_argument("--input-dir", default=DEFAULT_INPUT_DIR, help="Top-level folder containing original groups (default: originals)")
    p.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Where to write outputs (default: ./output)")
    p.add_argument("--seed", type=int, default=DEFAULT_SEED, help=argparse.SUPPRESS)
    return p.parse_args()

def _next_zip_path(output_dir: Path):
    """Return next numbered merged_bundle_N.zip path and also return path for latest merged_bundle.zip"""
    pattern = "merged_bundle_*.zip"
    existing = list(output_dir.glob(pattern))
    max_n = 0
    for p in existing:
        m = re.search(r"merged_bundle_(\d+)\.zip$", p.name)
        if m:
            try:
                n = int(m.group(1))
                if n > max_n:
                    max_n = n
            except:
                pass
    next_n = max_n + 1
    numbered = output_dir / f"merged_bundle_{next_n}.zip"
    latest = output_dir / "merged_bundle.zip"
    return numbered, latest

def main():
    args = parse_args()

    if args.exclude_max is None or args.exclude_max < 1:
        print("ERROR: --exclude-max must be an integer >= 1", file=sys.stderr)
        sys.exit(2)

    try:
        args.within_max_secs = parse_time_str_to_seconds(args.within_max_time)
        args.between_max_secs = parse_time_str_to_seconds(args.between_max_time)
    except Exception as e:
        print("ERROR parsing time inputs:", e, file=sys.stderr)
        sys.exit(2)

    if args.within_max_secs < 1 or args.between_max_secs < 1:
        print("ERROR: max times must be >= 1 second", file=sys.stderr)
        sys.exit(2)
    if args.within_max_pauses < 1 or args.between_max_pauses < 1:
        print("ERROR: max pauses must be >= 1", file=sys.stderr)
        sys.exit(2)

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Output folder (absolute):", str(output_dir.resolve()))

    if not input_dir.exists():
        print(f"NOTICE: input folder '{input_dir}' does not exist. Creating it now.", file=sys.stderr)
        try:
            input_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"ERROR: could not create input dir: {e}", file=sys.stderr)
            sys.exit(2)
        print(f"Created empty '{input_dir}'. Please add groups (folders) with .json files and re-run.", file=sys.stderr)
        sys.exit(0)

    groups = find_groups_recursive(input_dir)
    print(f"DEBUG: found {len(groups)} group(s) under '{input_dir}':")
    for g in groups:
        files = find_json_files(g)
        print(f" - {g}  -> {len(files)} json file(s)")

    base_seed = args.seed
    processed = get_previously_processed_files(output_dir/"merged_bundle.zip")
    global_pause_set = set()
    zip_items = []

    for gi, grp in enumerate(groups):
        files = find_json_files(grp)
        if not files:
            continue
        if not args.force and all(Path(f).name in processed for f in files):
            print(f"Skipping group '{grp}' (already processed).")
            continue

        log = {"group": grp.name, "group_path": str(grp), "versions": []}
        for v in range(1, args.versions + 1):
            version_rng = random.Random(base_seed + gi*1000 + v)
            fname, merged, finals, pauses, excl, total = generate_version(
                files, version_rng, base_seed, v, args, global_pause_set
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

    # create numbered zip and also a latest copy
    numbered_zip, latest_zip = _next_zip_path(output_dir)
    with ZipFile(numbered_zip, "w") as zf:
        for group_name, file_path in zip_items:
            if file_path.exists():
                zf.write(file_path, arcname=f"{group_name}/{file_path.name}")
    try:
        shutil.copy(numbered_zip, latest_zip)
    except Exception as e:
        print(f"WARNING: could not copy {numbered_zip} to {latest_zip}: {e}", file=sys.stderr)

    print("DONE. Outputs in:", str(output_dir.resolve()))
    print("Created:", numbered_zip.name, "and updated:", latest_zip.name)

if __name__ == "__main__":
    main()
