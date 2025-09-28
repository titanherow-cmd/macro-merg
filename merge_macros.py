#!/usr/bin/env python3
"""
merge_macros.py

Pause bounds may be provided as strings like '1m', '2m47s', '1:30', '45s', or plain '90'.
Probabilities are percentages (0-100). All time inputs are converted to seconds internally.
"""

from pathlib import Path
import argparse
import json
import glob
import random
from copy import deepcopy
from zipfile import ZipFile
import sys
import re

def parse_time_str_to_seconds(s: str):
    """
    Accepts:
      - '90' or '90s' -> 90
      - '1m' -> 60
      - '2m47s' -> 167
      - '1:30' -> 90
      - '1:5' -> 65
      - '  1m30s ' (whitespace tolerated)
    Returns integer seconds.
    Raises ValueError on invalid input.
    """
    if s is None:
        raise ValueError("time string is None")
    s = str(s).strip().lower()
    if not s:
        raise ValueError("empty time string")

    # format mm:ss or m:ss
    if re.match(r'^\d+:\d{1,2}$', s):
        parts = s.split(':')
        mins = int(parts[0])
        secs = int(parts[1])
        if secs >= 60:
            raise ValueError(f"seconds part must be < 60 in '{s}'")
        return mins * 60 + secs

    # combined format like '2m47s' or '1m' or '30s'
    m = re.match(r'^(?:(\d+)m)?\s*(?:(\d+)s)?$', s)
    if m:
        mm = int(m.group(1)) if m.group(1) else 0
        ss = int(m.group(2)) if m.group(2) else 0
        return mm * 60 + ss

    # numeric like '90' or '90.0' interpreted as seconds (allow floats)
    if re.match(r'^\d+(\.\d+)?s?$', s):
        # strip optional trailing 's'
        s2 = s[:-1] if s.endswith('s') else s
        val = float(s2)
        return int(round(val))

    raise ValueError(f"Could not parse time value '{s}'")

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input-dir", required=True, help="Parent directory containing group subfolders")
    p.add_argument("--output-dir", required=True, help="Directory to write merged files and ZIP")
    p.add_argument("--versions", type=int, default=5, help="Number of versions per group")
    p.add_argument("--seed", type=int, default=None, help="Base random seed (optional)")
    p.add_argument("--force", action="store_true", help="Force processing groups even if previously processed")
    p.add_argument("--exclude-count", type=int, default=1, help="How many files to randomly exclude per version")

    # Intra-file pause rules (between actions inside a single file)
    p.add_argument("--intra-file-enabled", action="store_true", help="Enable intra-file random pauses")
    p.add_argument("--intra-long-prob", type=float, default=70.0,
                   help="Percent chance to choose long-style intra pauses (0-100)")
    p.add_argument("--intra-long-min-count", type=int, default=2, help="Min number of long intra pauses per file")
    p.add_argument("--intra-long-max-count", type=int, default=9, help="Max number of long intra pauses per file")
    p.add_argument("--intra-long-min", type=str, default="1m", help="Long intra pause min (e.g. '1m' or '60s' or '1:00')")
    p.add_argument("--intra-long-max", type=str, default="2m47s", help="Long intra pause max (e.g. '2m47s')")
    p.add_argument("--intra-short-min", type=str, default="6s", help="Short intra pause min (e.g. '6s')")
    p.add_argument("--intra-short-max", type=str, default="30s", help="Short intra pause max (e.g. '30s')")
    p.add_argument("--intra-max-pauses-cap", type=int, default=9,
                   help="Hard cap on number of intra pauses per file (will cap the chosen count)")

    # Inter-file pause rules (pauses between whole files in merged output)
    p.add_argument("--inter-insert-prob", type=float, default=60.0,
                   help="Percent chance to insert an inter-file pause at each file (0-100)")
    p.add_argument("--inter-long-prob", type=float, default=20.0,
                   help="Percent chance that an inserted inter-file pause will be chosen from the LONG band (0-100)")
    p.add_argument("--inter-short-max", type=str, default="4m53s",
                   help="If selecting SHORT inter-file band, choose uniformly between 1s and this many seconds (e.g. '4m53s')")
    p.add_argument("--inter-long-min", type=str, default="7m", help="Long inter-file band min (e.g. '7m')")
    p.add_argument("--inter-long-max", type=str, default="10m", help="Long inter-file band max (e.g. '10m')")

    # Post-file buffer (small buffer after each file to ensure playback completes)
    p.add_argument("--post-buffer-min", type=str, default="10s", help="Min per-file post buffer (e.g. '10s' or '10')")
    p.add_argument("--post-buffer-max", type=str, default="30s", help="Max per-file post buffer (e.g. '30s')")

    args = p.parse_args()

    # parse time strings into integer seconds and validate ranges
    try:
        args.intra_long_min_secs = parse_time_str_to_seconds(args.intra_long_min)
        args.intra_long_max_secs = parse_time_str_to_seconds(args.intra_long_max)
        args.intra_short_min_secs = parse_time_str_to_seconds(args.intra_short_min)
        args.intra_short_max_secs = parse_time_str_to_seconds(args.intra_short_max)
        args.inter_short_max_secs = parse_time_str_to_seconds(args.inter_short_max)
        args.inter_long_min_secs = parse_time_str_to_seconds(args.inter_long_min)
        args.inter_long_max_secs = parse_time_str_to_seconds(args.inter_long_max)
        args.post_buffer_min_secs = parse_time_str_to_seconds(args.post_buffer_min)
        args.post_buffer_max_secs = parse_time_str_to_seconds(args.post_buffer_max)
    except ValueError as ve:
        print(f"ERROR parsing time argument: {ve}", file=sys.stderr)
        sys.exit(2)

    # basic validation
    def check_min_le_max(name, mn, mx):
        if mn > mx:
            print(f"ERROR: {name} min ({mn}s) > max ({mx}s). Fix CLI args.", file=sys.stderr)
            sys.exit(2)

    check_min_le_max("intra-long", args.intra_long_min_secs, args.intra_long_max_secs)
    check_min_le_max("intra-short", args.intra_short_min_secs, args.intra_short_max_secs)
    check_min_le_max("inter-short (1s..X)", 1, args.inter_short_max_secs)
    check_min_le_max("inter-long", args.inter_long_min_secs, args.inter_long_max_secs)
    check_min_le_max("post-buffer", args.post_buffer_min_secs, args.post_buffer_max_secs)

    # clamp probabilities
    for p_name in ("intra_long_prob", "inter_insert_prob", "inter_long_prob"):
        val = getattr(args, p_name)
        if val < 0 or val > 100:
            print(f"ERROR: {p_name} must be between 0 and 100 (got {val})", file=sys.stderr)
            sys.exit(2)

    return args

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
            except:
                t = 0
        new = {
            "Type": e.get("Type"),
            "Time": t + int(shift_ms),
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
                    if name.endswith(".json") and not name.endswith("_log.txt"):
                        processed.add(Path(name).name)
        except:
            pass
    return processed

def part_from_filename(fname: str):
    stem = Path(fname).stem
    return ''.join(ch for ch in stem if ch.isalnum())

def insert_intra_pauses_fixed(events, rng,
                             long_prob_pct, long_min_count, long_max_count, long_min_ms, long_max_ms,
                             short_min_ms, short_max_ms, cap_max_pauses, intra_log):
    if not events or cap_max_pauses <= 0:
        return deepcopy(events)

    evs = deepcopy(events)
    n = len(evs)
    if n < 2:
        return evs

    gaps = n - 1

    if rng.random() < (long_prob_pct / 100.0):
        # long style
        min_k = max(1, long_min_count)
        max_k = min(gaps, long_max_count, cap_max_pauses)
        if max_k < min_k:
            # fallback to a single short pause
            k = 1
            chosen = rng.sample(range(gaps), k)
            pause_ms = rng.randint(short_min_ms, short_max_ms)
            for gap_idx in sorted(chosen):
                for j in range(gap_idx+1, n):
                    evs[j]["Time"] = int(evs[j].get("Time", 0)) + pause_ms
                intra_log.append({"after_event_index": gap_idx, "pause_ms": pause_ms})
            return evs
        k = rng.randint(min_k, max_k)
        chosen = rng.sample(range(gaps), k)
        for gap_idx in sorted(chosen):
            pause_ms = rng.randint(long_min_ms, long_max_ms)
            for j in range(gap_idx+1, n):
                evs[j]["Time"] = int(evs[j].get("Time", 0)) + pause_ms
            intra_log.append({"after_event_index": gap_idx, "pause_ms": pause_ms})
        return evs
    else:
        # short style: single pause
        k = 1
        chosen = rng.sample(range(gaps), k)
        pause_ms = rng.randint(short_min_ms, short_max_ms)
        for gap_idx in sorted(chosen):
            for j in range(gap_idx+1, n):
                evs[j]["Time"] = int(evs[j].get("Time", 0)) + pause_ms
            intra_log.append({"after_event_index": gap_idx, "pause_ms": pause_ms})
        return evs

def generate_version(files, seed, global_pause_set, version_num, exclude_count, args):
    rng = random.Random(seed)
    if not files:
        return None, [], [], {}, [], 0
    m = len(files)
    exclude_count = max(0, min(exclude_count, m-1))
    excluded = rng.sample(files, k=exclude_count) if exclude_count else []
    included = [f for f in files if f not in excluded]
    dup_files = rng.sample(included or files, min(2, len(included or files)))
    final_files = included + dup_files
    if included:
        extra_files = rng.sample(included, k=rng.choice([1,2]))
        for ef in extra_files:
            pos = rng.randrange(len(final_files)+1)
            if pos > 0 and final_files[pos-1] == ef:
                pos += 1
            final_files.insert(min(pos, len(final_files)), ef)
    rng.shuffle(final_files)

    merged = []
    pause_log = {"inter_file_pauses": [], "intra_file_pauses": [], "post_file_buffers": []}
    time_cursor = 0
    play_times = {}

    for idx, f in enumerate(final_files):
        evs_raw = load_json(Path(f)) or []
        evs = normalize_json(evs_raw)
        intra_log_local = []

        if args.intra_file_enabled and evs:
            intra_rng = random.Random((hash(f) & 0xffffffff) ^ (seed + version_num))
            evs = insert_intra_pauses_fixed(
                evs,
                intra_rng,
                long_prob_pct = args.intra_long_prob,
                long_min_count = args.intra_long_min_count,
                long_max_count = args.intra_long_max_count,
                long_min_ms = args.intra_long_min_secs * 1000,
                long_max_ms = args.intra_long_max_secs * 1000,
                short_min_ms = args.intra_short_min_secs * 1000,
                short_max_ms = args.intra_short_max_secs * 1000,
                cap_max_pauses = args.intra_max_pauses_cap,
                intra_log = intra_log_local
            )
        if intra_log_local:
            pause_log["intra_file_pauses"].append({"file": Path(f).name, "pauses": intra_log_local})

        # decide whether to insert an inter-file pause before this file
        if rng.random() < (args.inter_insert_prob / 100.0):
            attempts = 0
            while True:
                if rng.random() < (args.inter_long_prob / 100.0):
                    inter_ms = rng.randint(args.inter_long_min_secs * 1000, args.inter_long_max_secs * 1000)
                else:
                    inter_ms = rng.randint(1_000, args.inter_short_max_secs * 1000)

                if inter_ms not in global_pause_set or attempts > 200:
                    global_pause_set.add(inter_ms)
                    break
                attempts += 1
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

            buffer_ms = rng.randint(args.post_buffer_min_secs * 1000, args.post_buffer_max_secs * 1000)
            time_cursor = max_t + buffer_ms

            pause_log["post_file_buffers"].append({"file": Path(f).name, "buffer_ms": buffer_ms})
        else:
            play_times[f] = 0

    parts = [part_from_filename(f) + f"[{round((play_times[f] or 0)/60000)}m] " for f in final_files]
    total_minutes = round(sum(play_times.values()) / 60000)
    merged_fname = "".join(parts).rstrip() + f"_v{version_num}[{total_minutes}m].json"

    return merged_fname, merged, final_files, pause_log, excluded, total_minutes

def main():
    args = parse_args()
    in_dir, out_dir = Path(args.input_dir), Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    base_seed = args.seed if args.seed is not None else random.randrange(2**31)
    processed = get_previously_processed_files(out_dir/"merged_bundle.zip")
    global_pauses, zip_items = set(), []

    for gi, grp in enumerate(find_groups(in_dir)):
        files = find_json_files(grp)
        if not files:
            continue
        if not args.force and all(Path(f).name in processed for f in files):
            continue
        log = {"group": grp.name, "versions": []}
        for v in range(1, args.versions + 1):
            fname, merged, finals, pauses, excl, total = generate_version(
                files, base_seed + gi*1000 + v, global_pauses, v, args.exclude_count, args
            )
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

    zip_path = out_dir / "merged_bundle.zip"
    with ZipFile(zip_path, "w") as zf:
        for group_name, file_path in zip_items:
            zf.write(file_path, arcname=f"{group_name}/{file_path.name}")

    print("DONE.")

if __name__ == "__main__":
    main()
