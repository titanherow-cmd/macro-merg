#!/usr/bin/env python3
"""
merge_macros.py

Patched: randomized post-file buffer (configurable with --post-buffer-min / --post-buffer-max).
Other behaviour preserved:
 - recursive group discovery under 'originals'
 - no per-group log files produced
 - zip contains merged JSONs; if BUNDLE_SEQ env var present the zip places files under merged_bundle_<SEQ>/...
 - naming: <UPPER_LETTERS>_<TotalMinutes>m= part1[ Xm]- part2[ Ym].json
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

DEFAULT_INPUT_DIR = "originals"
DEFAULT_OUTPUT_DIR = "output"
DEFAULT_SEED = 12345
DEFAULT_EXCLUDE_MAX = 3
DEFAULT_POST_BUFFER_MIN = "10s"
DEFAULT_POST_BUFFER_MAX = "30s"

def index_to_letters(idx: int) -> str:
    """1->'A', 2->'B', ..., 26->'Z', 27->'AA'."""
    if idx < 1:
        return "A"
    s = ""
    n = idx
    while n > 0:
        n -= 1
        s = chr(ord('A') + (n % 26)) + s
        n //= 26
    return s

def parse_time_str_to_seconds(s: str) -> int:
    if s is None:
        raise ValueError("time string is None")
    s0 = str(s).strip().lower()
    if not s0:
        raise ValueError("empty time string")
    # formats: "M.SS" (like 1.30), "M:SS", "Xm Ys", "Xm", "Ys", "90", "90s"
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

def generate_version(files, rng, seed_for_intra, version_num, args, global_pause_set):
    if not files:
        return None, [], [], {}, [], 0

    # RANDOM EXCLUSION: allow 0..max_excl
    if len(files) <= 1:
        excluded = []
        included = list(files)
    else:
        max_excl = min(args.exclude_max, len(files)-1)
        if max_excl >= 0:
            excl_count = rng.randint(0, max_excl)
            excluded = rng.sample(files, excl_count) if excl_count > 0 else []
        else:
            excluded = []
        included = [f for f in files if f not in excluded]

    if not included and files:
        included = [files[0]]
        excluded = [f for f in files if f != files[0]]

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
    time_cursor = 0
    play_times = {}

    within_max_ms = max(1000, args.within_max_secs * 1000)
    between_max_ms = max(1000, args.between_max_secs * 1000)
    within_max_p = max(1, args.within_max_pauses)
    between_max_p = max(1, args.between_max_pauses)

    # post-buffer range (ms)
    post_min_ms = max(0, int(args.post_buffer_min_secs * 1000))
    post_max_ms = max(post_min_ms, int(args.post_buffer_max_secs * 1000))

    for idx, f in enumerate(final_files):
        evs_raw = load_json(Path(f)) or []
        evs = normalize_json(evs_raw)

        # INTRA-FILE (skip first file)
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
                    for j in range(gap_idx+1, len(evs)):
                        evs[j]["Time"] = int(evs[j].get("Time", 0)) + pause_ms

        # INTER-FILE: add pause before this file (except before first)
        if idx != 0:
            k = rng.randint(1, between_max_p)
            k = min(k, 50)
            total_inter_ms = 0
            for i in range(k):
                attempts = 0
                inter_ms = None
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
            time_cursor += total_inter_ms

        shifted = apply_shifts(evs, time_cursor)
        merged.extend(shifted)
        if shifted:
            max_t = max(int(e.get("Time", 0)) for e in shifted)
            min_t = min(int(e.get("Time", 0)) for e in shifted)
            play_times[f] = max_t - min_t

            # POST-FILE BUFFER: randomized between configured min/max (ms)
            buffer_ms = rng.randint(post_min_ms, post_max_ms) if post_max_ms >= post_min_ms else post_min_ms
            time_cursor = max_t + buffer_ms
        else:
            play_times[f] = 0

    total_ms = sum(play_times.values())
    total_minutes = math.ceil(total_ms / 60000) if total_ms > 0 else 0

    parts_list = [part_from_filename(f) + f"[{math.ceil((play_times.get(f,0))/60000)}m]" for f in final_files]
    parts_joined = "- ".join(parts_list)

    letters = index_to_letters(version_num)
    merged_fname = f"{letters}_{total_minutes}m= {parts_joined}.json"

    return merged_fname, merged, final_files, {}, [], total_minutes

def parse_args():
    p = argparse.ArgumentParser(description="Merge macros - recursive group discovery (defaults to 'originals').")
    p.add_argument("--versions", type=int, default=6, help="How many versions per group")
    p.add_argument("--force", action="store_true", help="Force reprocessing even if outputs exist")
    p.add_argument("--within-max-time", required=True, help="Within-file max pause time (e.g. 1.30, 1m30s, 1:30)")
    p.add_argument("--within-max-pauses", type=int, required=True, help="Max number of pauses to add inside each file")
    p.add_argument("--between-max-time", required=True, help="Between-files max pause time (e.g. 10s, 1m)")
    p.add_argument("--between-max-pauses", type=int, required=True, help="Max number of pauses to insert between files (usually 1)")
    p.add_argument("--exclude-max", type=int, default=DEFAULT_EXCLUDE_MAX, help="Maximum number of files to randomly exclude per version (0..N-1)")
    p.add_argument("--post-buffer-min", default=DEFAULT_POST_BUFFER_MIN, help="Post-file buffer minimum (e.g. 10s, 1m30s)")
    p.add_argument("--post-buffer-max", default=DEFAULT_POST_BUFFER_MAX, help="Post-file buffer maximum (e.g. 30s, 1m)")
    p.add_argument("--input-dir", default=DEFAULT_INPUT_DIR, help="Top-level folder containing original groups (default: originals)")
    p.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Where to write outputs (default: ./output)")
    p.add_argument("--seed", type=int, default=DEFAULT_SEED, help=argparse.SUPPRESS)
    return p.parse_args()

def main():
    args = parse_args()

    # parse times
    try:
        args.within_max_secs = parse_time_str_to_seconds(args.within_max_time)
        args.between_max_secs = parse_time_str_to_seconds(args.between_max_time)
        args.post_buffer_min_secs = parse_time_str_to_seconds(args.post_buffer_min)
        args.post_buffer_max_secs = parse_time_str_to_seconds(args.post_buffer_max)
    except Exception as e:
        print("ERROR parsing time inputs:", e, file=sys.stderr)
        sys.exit(2)

    if args.post_buffer_min_secs < 0 or args.post_buffer_max_secs < 0:
        print("ERROR: post-buffer times must be >= 0", file=sys.stderr)
        sys.exit(2)
    if args.post_buffer_max_secs < args.post_buffer_min_secs:
        print("ERROR: post-buffer-max must be >= post-buffer-min", file=sys.stderr)
        sys.exit(2)

    if args.exclude_max is None or args.exclude_max < 0:
        print("ERROR: --exclude-max must be an integer >= 0", file=sys.stderr)
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

    if not input_dir.exists():
        placeholder = output_dir / "merged_bundle.zip"
        with ZipFile(placeholder, "w") as zf:
            zf.writestr("placeholder.txt", "No originals folder found.")
        print("Wrote placeholder zip:", placeholder)
        sys.exit(0)

    groups = find_groups_recursive(input_dir)
    base_seed = args.seed
    global_pause_set = set()
    merged_outputs = []

    for gi, grp in enumerate(groups):
        files = find_json_files(grp)
        if not files:
            continue

        for v in range(1, args.versions + 1):
            version_rng = random.Random(base_seed + gi*1000 + v)
            fname, merged, finals, _, excl, total = generate_version(
                files, version_rng, base_seed, v, args, global_pause_set
            )
            if not fname:
                continue
            out_file_path = output_dir / fname
            with open(out_file_path, "w", encoding="utf-8") as fh:
                json.dump(merged, fh, indent=2, ensure_ascii=False)
            merged_outputs.append((grp, out_file_path))

    # Create merged_bundle.zip that contains only merged JSONs.
    bundle_path = output_dir / "merged_bundle.zip"
    bundle_seq = os.environ.get("BUNDLE_SEQ", "").strip()
    top_folder = f"merged_bundle_{bundle_seq}" if bundle_seq else None

    if merged_outputs:
        with ZipFile(bundle_path, "w") as zf:
            for group_path, file_path in merged_outputs:
                if not file_path.exists():
                    continue
                try:
                    rel_group = group_path.relative_to(input_dir)
                except Exception:
                    rel_group = Path(group_path.name)
                if top_folder:
                    arc = Path(top_folder) / rel_group / file_path.name
                else:
                    arc = rel_group / file_path.name
                zf.write(file_path, arcname=str(arc.as_posix()))
    else:
        with ZipFile(bundle_path, "w") as zf:
            txt = "No merged files were produced.\n"
            zf.writestr("placeholder.txt", txt)

    print("DONE. Outputs in:", str(output_dir.resolve()))
    print("Created:", bundle_path.name)

if __name__ == "__main__":
    main()
