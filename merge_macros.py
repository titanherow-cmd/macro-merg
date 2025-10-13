#!/usr/bin/env python3
"""
merge_macros.py

Behavior:
 - Recursively finds groups under --input-dir (default "originals").
 - Generates `--versions` merged files per group.
 - Intra-file pauses: applied to ALL files (including the first). Each pause chosen between 4s and within-max-time.
 - Inter-file pause: single pause before each file except the very first. For each boundary:
     * choose min_rand_secs = random.randint(30,57)  (hard lower bound)
     * parse between_max_secs from --between-max-time (default 4m53s)
     * if between_max_secs < min_rand_secs -> silently clamp between_max_secs = min_rand_secs
     * choose pause_ms = rand(min_rand_secs*1000, between_max_secs*1000)
 - Post-file buffer (after each file): random between --post-buffer-min and --post-buffer-max (defaults 30s..1m)
 - Attached file "close reopen mobile screensharelink.json" appended only for groups under any MOBILE folder (case-insensitive):
     * appended immediately after merged content (no inter pause before it),
     * receives no intra pauses and no post-buffer after it.
 - Filenames: <LETTERS>_<TotalEffectiveMinutes>m= part1[+Xm]- part2[+Ym].json
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
DEFAULT_WITHIN_MAX = "2m47s"    # intra-file default max
DEFAULT_BETWEEN_MAX = "4m53s"   # between-file default max
DEFAULT_POST_MIN = "30s"        # post-file buffer default min
DEFAULT_POST_MAX = "1m"         # post-file buffer default max

ATTACHED_FILENAME = "close reopen mobile screensharelink.json"  # expected at repo root

def index_to_letters(idx: int) -> str:
    if idx < 1:
        return "A"
    s = ""
    n = idx
    while n > 0:
        n -= 1
        s = chr(ord('A') + (n % 26)) + s
        n //= 26
    return s

_time_re_dot = re.compile(r'^(\d+)\.(\d{1,2})$')
_time_re_colon = re.compile(r'^(\d+):(\d{1,2})$')
_time_re_ms = re.compile(r'^(?:(\d+)m)?\s*(?:(\d+)s)?$')

def parse_time_str_to_seconds(s: str) -> int:
    if s is None:
        raise ValueError("time string is None")
    s0 = str(s).strip().lower()
    if not s0:
        raise ValueError("empty time string")
    m = _time_re_dot.match(s0)
    if m:
        mins = int(m.group(1)); secs = int(m.group(2))
        if secs >= 60:
            raise ValueError(f"seconds part must be <60 in '{s}'")
        return mins * 60 + secs
    m = _time_re_colon.match(s0)
    if m:
        mins = int(m.group(1)); secs = int(m.group(2))
        if secs >= 60:
            raise ValueError(f"seconds part must be <60 in '{s}'")
        return mins * 60 + secs
    m = _time_re_ms.match(s0)
    if m and (m.group(1) or m.group(2)):
        mm = int(m.group(1)) if m.group(1) else 0
        ss = int(m.group(2)) if m.group(2) else 0
        return mm * 60 + ss
    if re.match(r'^\d+(\.\d+)?s?$', s0):
        s2 = s0[:-1] if s0.endswith('s') else s0
        val = float(s2)
        return int(round(val))
    if re.match(r'^\d+$', s0):
        # treat plain integer as seconds
        return int(s0)
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

def generate_version(files, rng, seed_for_intra, version_num, args, global_pause_set, group_path: Path, attached_path: Path):
    excluded = []
    if not files:
        return None, [], [], {}, [], 0

    # RANDOM EXCLUSION: allow 0..max_excl
    if len(files) <= 1:
        included = list(files)
        excluded = []
    else:
        max_excl = min(args.exclude_max, len(files)-1)
        excl_count = rng.randint(0, max_excl) if max_excl >= 0 else 0
        excluded = rng.sample(files, excl_count) if excl_count > 0 else []
        included = [f for f in files if f not in excluded]

    if not included and files:
        included = [files[0]]
        excluded = [f for f in files if f != files[0]]

    population = included or files
    dup_count = min(2, len(population))
    dup_files = rng.sample(population, dup_count) if dup_count > 0 else []
    final_files = included + dup_files

    # insert some extra files non-adjacent
    if included:
        k_choice = rng.choice([1,2])
        k = min(k_choice, len(included))
        if k > 0:
            extra_files = rng.sample(included, k=k)
            for ef in extra_files:
                n = len(final_files)
                pos = None
                for attempt in range(n+3):
                    p = rng.randrange(0, n+1)
                    left_ok = (p-1 < 0) or (final_files[p-1] != ef)
                    right_ok = (p >= n) or (final_files[p] != ef)
                    if left_ok and right_ok:
                        pos = p
                        break
                if pos is None:
                    pos = min(rng.randrange(0, n+1), n)
                final_files.insert(min(pos, len(final_files)), ef)

    rng.shuffle(final_files)

    merged = []
    time_cursor = 0
    play_times = {}

    # bookkeeping
    inter_before_ms_map = {}
    intra_sum_ms_map = {}
    post_buffer_ms_map = {}

    # parse configured maxima
    within_max_ms = max(4000, args.within_max_secs * 1000)  # enforce at least 4s for intra pauses
    between_max_secs = max(57, args.between_max_secs)       # silently clamp: between_max_secs >= 57
    between_max_ms = between_max_secs * 1000
    post_min_ms = max(0, args.post_buffer_min_secs * 1000)
    post_max_ms = max(post_min_ms, args.post_buffer_max_secs * 1000)

    for idx, f in enumerate(final_files):
        evs_raw = load_json(Path(f)) or []
        evs = normalize_json(evs_raw)

        inter_before_ms_map[f] = 0
        intra_sum_ms_map[f] = 0
        post_buffer_ms_map[f] = 0

        # INTRA-FILE pauses: apply to ALL files (including first)
        if evs and len(evs) > 1:
            n_gaps = len(evs) - 1
            intra_rng = random.Random((hash(f) & 0xffffffff) ^ (seed_for_intra + version_num))
            chosen_count = intra_rng.randint(1, min(args.within_max_pauses, n_gaps))
            if chosen_count > 0:
                chosen_gaps = intra_rng.sample(range(n_gaps), chosen_count)
                for gap_idx in sorted(chosen_gaps):
                    pause_ms = intra_rng.randint(4000, within_max_ms)
                    for j in range(gap_idx+1, len(evs)):
                        evs[j]["Time"] = int(evs[j].get("Time", 0)) + pause_ms
                    intra_sum_ms_map[f] += pause_ms

        # INTER-FILE pause before this file (skip before the very first file)
        if idx != 0:
            # choose min_rand between 30..57 seconds (hard lower bound)
            min_rand_secs = rng.randint(30, 57)
            min_rand_ms = min_rand_secs * 1000
            # ensure between_max_ms respects the per-boundary min; silently clamp if needed
            if between_max_ms < min_rand_ms:
                between_max_ms_eff = min_rand_ms
            else:
                between_max_ms_eff = between_max_ms
            pause_ms = rng.randint(min_rand_ms, between_max_ms_eff)
            time_cursor += pause_ms
            inter_before_ms_map[f] = pause_ms

        # apply shifts for this file
        shifted = apply_shifts(evs, time_cursor)
        merged.extend(shifted)
        if shifted:
            max_t = max(int(e.get("Time", 0)) for e in shifted)
            min_t = min(int(e.get("Time", 0)) for e in shifted)
            duration_ms = max_t - min_t
            play_times[f] = duration_ms

            # POST-FILE buffer after this file (applies to all files, including first)
            post_ms = rng.randint(post_min_ms, post_max_ms) if post_max_ms >= post_min_ms else post_min_ms
            post_buffer_ms_map[f] = post_ms
            time_cursor = max_t + post_ms
        else:
            play_times[f] = 0

    # Append attached file for MOBILE groups only (no inter pause before it; no intra; no post-buffer after it)
    try:
        parts_lower = [p.lower() for p in group_path.parts]
    except Exception:
        parts_lower = []
    appended_path_str = None
    if "mobile" in parts_lower:
        cand = Path(ATTACHED_FILENAME)
        if cand.exists():
            evs_raw = load_json(cand) or []
            evs = normalize_json(evs_raw)
            # append immediately at current time_cursor with no added pause
            shifted = apply_shifts(evs, time_cursor)
            merged.extend(shifted)
            if shifted:
                max_t = max(int(e.get("Time", 0)) for e in shifted)
                min_t = min(int(e.get("Time", 0)) for e in shifted)
                play_times[str(cand)] = max_t - min_t
                inter_before_ms_map[str(cand)] = 0
                intra_sum_ms_map[str(cand)] = 0
                post_buffer_ms_map[str(cand)] = 0
                time_cursor = max_t  # no post-buffer
                final_files.append(str(cand))
                appended_path_str = str(cand)

    # compute per-file effective times: inter_before + duration + intra + post_buffer
    per_file_effective_ms = {}
    for f in final_files:
        dur = play_times.get(f, 0)
        inter_b = inter_before_ms_map.get(f, 0)
        intra_s = intra_sum_ms_map.get(f, 0)
        post_b = post_buffer_ms_map.get(f, 0)
        per_file_effective_ms[f] = inter_b + dur + intra_s + post_b

    total_effective_ms = sum(per_file_effective_ms.values())
    total_minutes = math.ceil(total_effective_ms / 60000) if total_effective_ms > 0 else 0

    # parts: show +<minutes> per file (rounded up)
    parts = []
    for f in final_files:
        minutes = math.ceil(per_file_effective_ms.get(f, 0) / 60000)
        parts.append(part_from_filename(f) + f"[+{minutes}m]")

    parts_joined = "- ".join(parts)
    letters = index_to_letters(version_num)
    merged_fname = f"{letters}_{total_minutes}m= {parts_joined}.json"

    return merged_fname, merged, final_files, {}, excluded, total_minutes

def parse_args():
    p = argparse.ArgumentParser(description="Merge macros - recursive group discovery (defaults to 'originals').")
    p.add_argument("--versions", type=int, default=6, help="How many versions per group")
    p.add_argument("--force", action="store_true", help="Force reprocessing even if outputs exist (optional)")
    p.add_argument("--within-max-time", default=DEFAULT_WITHIN_MAX, help="Within-file max pause time (e.g. 2m47s)")
    p.add_argument("--within-max-pauses", type=int, default=5, help="Max intra-file pauses")
    p.add_argument("--between-max-time", default=DEFAULT_BETWEEN_MAX, help="Between-files max pause time (e.g. 4m53s) - min will be >=57s")
    p.add_argument("--between-max-pauses", type=int, default=1, help="(ignored) kept for UI compatibility")
    p.add_argument("--post-buffer-min", default=DEFAULT_POST_MIN, help="Post-file buffer MIN (e.g. 30s)")
    p.add_argument("--post-buffer-max", default=DEFAULT_POST_MAX, help="Post-file buffer MAX (e.g. 1m)")
    p.add_argument("--exclude-max", type=int, default=DEFAULT_EXCLUDE_MAX, help="Maximum number of files to randomly exclude per version (0..N-1)")
    p.add_argument("--input-dir", default=DEFAULT_INPUT_DIR, help="Top-level folder containing original groups (default: originals)")
    p.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Where to write outputs (default: ./output)")
    p.add_argument("--seed", type=int, default=DEFAULT_SEED, help=argparse.SUPPRESS)
    return p.parse_args()

def main():
    args = parse_args()

    # parse times (seconds)
    try:
        args.within_max_secs = parse_time_str_to_seconds(args.within_max_time)
        args.between_max_secs = parse_time_str_to_seconds(args.between_max_time)
        args.post_buffer_min_secs = parse_time_str_to_seconds(args.post_buffer_min)
        args.post_buffer_max_secs = parse_time_str_to_seconds(args.post_buffer_max)
    except Exception as e:
        print("ERROR parsing time inputs:", e, file=sys.stderr)
        sys.exit(2)

    # enforce minima
    if args.within_max_secs < 4:
        print("WARNING: --within-max-time too small; raising to 4s.")
        args.within_max_secs = 4

    # silently clamp between_max to at least 57s per your choice (B)
    if args.between_max_secs < 57:
        args.between_max_secs = 57

    if args.post_buffer_min_secs < 0 or args.post_buffer_max_secs < 0:
        print("ERROR: post-buffer times must be >= 0", file=sys.stderr)
        sys.exit(2)
    if args.post_buffer_max_secs < args.post_buffer_min_secs:
        print("ERROR: post-buffer-max must be >= post-buffer-min", file=sys.stderr)
        sys.exit(2)

    if args.exclude_max is None or args.exclude_max < 0:
        print("ERROR: --exclude-max must be integer >= 0", file=sys.stderr)
        sys.exit(2)
    if args.within_max_pauses < 1:
        print("ERROR: --within-max-pauses must be >= 1", file=sys.stderr)
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

    attached_path = Path(ATTACHED_FILENAME)
    for gi, grp in enumerate(groups):
        files = find_json_files(grp)
        if not files:
            continue
        for v in range(1, args.versions + 1):
            version_rng = random.Random(base_seed + gi*1000 + v)
            fname, merged, finals, _, excl, total = generate_version(
                files, version_rng, base_seed, v, args, global_pause_set, grp, attached_path
            )
            if not fname:
                continue
            out_file_path = output_dir / fname
            with open(out_file_path, "w", encoding="utf-8") as fh:
                json.dump(merged, fh, indent=2, ensure_ascii=False)
            merged_outputs.append((grp, out_file_path))

    # Create merged_bundle.zip with group-preserving structure
    bundle_path = output_dir / "merged_bundle.zip"
    bundle_seq = os.environ.get("BUNDLE_SEQ", "").strip()
    top_folder = f"merged_bundle_{bundle_seq}" if bundle_seq else None

    if merged_outputs:
        with ZipFile(bundle_path, "w") as zf:
            for group_path, file_path in merged_outputs:
                if not file_path.exists() or file_path.suffix.lower() != ".json":
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
            zf.writestr("placeholder.txt", "No merged files were produced.")

    print("DONE. Outputs in:", str(output_dir.resolve()))
    print("Created:", bundle_path.name)

if __name__ == "__main__":
    main()
