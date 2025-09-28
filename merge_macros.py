#!/usr/bin/env python3
"""
merge_macros.py

Always-applies-both-pause-rules version (compact UI):

- Hardcoded defaults: input='input', output='output', seed=12345 (override-able via CLI)
- CLI-exposed:
    --versions N
    --force
    --between-min <time>   (e.g. 10s, 1m)
    --between-max <time>
    --within-min <time>    (e.g. 1m)
    --within-max <time>
    --min-pauses <int>
    --max-pauses <int>
    --seed (optional override)
- First file in each merged sequence is exempt from:
    * intra-file pauses
    * any inter-file pause that would occur before it
- New filename: {TOTALm}_v{VERSION}_<parts>.json
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

# Hardcoded defaults (you asked these be defaults)
DEFAULT_INPUT_DIR = "input"
DEFAULT_OUTPUT_DIR = "output"
DEFAULT_SEED = 12345

# ---------- helpers ----------
def parse_time_str_to_seconds(s: str):
    if s is None:
        raise ValueError("time string is None")
    s = str(s).strip().lower()
    if not s:
        raise ValueError("empty time string")
    if re.match(r'^\d+:\d{1,2}$', s):
        parts = s.split(':')
        mins = int(parts[0]); secs = int(parts[1])
        if secs >= 60:
            raise ValueError(f"seconds part must be < 60 in '{s}'")
        return mins * 60 + secs
    m = re.match(r'^(?:(\d+)m)?\s*(?:(\d+)s)?$', s)
    if m:
        mm = int(m.group(1)) if m.group(1) else 0
        ss = int(m.group(2)) if m.group(2) else 0
        return mm * 60 + ss
    if re.match(r'^\d+(\.\d+)?s?$', s):
        s2 = s[:-1] if s.endswith('s') else s
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

def part_from_filename(fname: str):
    stem = Path(fname).stem
    return ''.join(ch for ch in stem if ch.isalnum())

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

# ---------- generation ----------
def generate_version(files, rng, seed_for_intra, version_num, args, global_pause_set):
    if not files:
        return None, [], [], {}, [], 0

    excluded = []   # compact UI does not expose excludes
    included = [f for f in files if f not in excluded]

    # duplicates & extras (keep similar behavior)
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

    between_min_ms = args.between_min_secs * 1000
    between_max_ms = args.between_max_secs * 1000
    within_min_ms = args.within_min_secs * 1000
    within_max_ms = args.within_max_secs * 1000

    min_pauses = args.min_pauses
    max_pauses = args.max_pauses

    for idx, f in enumerate(final_files):
        evs_raw = load_json(Path(f)) or []
        evs = normalize_json(evs_raw)
        intra_log_local = []

        # INTRA-FILE PAUSES: always apply except for first file (exempt)
        if idx != 0 and evs and len(evs) > 1:
            n_gaps = len(evs) - 1
            chosen_count = rng.randint(min_pauses, max_pauses)
            chosen_count = min(chosen_count, n_gaps)
            # use per-file deterministic RNG for which gaps and pause lengths
            intra_rng = random.Random((hash(f) & 0xffffffff) ^ (seed_for_intra + version_num))
            chosen_gaps = intra_rng.sample(range(n_gaps), chosen_count)
            for gap_idx in sorted(chosen_gaps):
                pause_ms = intra_rng.randint(within_min_ms, within_max_ms)
                for j in range(gap_idx+1, len(evs)):
                    evs[j]["Time"] = int(evs[j].get("Time", 0)) + pause_ms
                intra_log_local.append({"after_event_index": gap_idx, "pause_ms": pause_ms})
        if intra_log_local:
            pause_log["intra_file_pauses"].append({"file": Path(f).name, "pauses": intra_log_local})

        # INTER-FILE PAUSES: always apply between files (i.e., pause before this file), except before first file
        if idx != 0:
            attempts = 0
            while True:
                inter_ms = rng.randint(between_min_ms, between_max_ms)
                if inter_ms not in global_pause_set or attempts > 200:
                    global_pause_set.add(inter_ms)
                    break
                attempts += 1
            time_cursor += inter_ms
            pause_log["inter_file_pauses"].append({"before_file": Path(f).name, "pause_ms": inter_ms, "is_before_index": idx})

        # apply shifts for this file's events
        shifted = apply_shifts(evs, time_cursor)
        merged.extend(shifted)
        if shifted:
            max_t = max(int(e.get("Time", 0)) for e in shifted)
            min_t = min(int(e.get("Time", 0)) for e in shifted)
            play_times[f] = max_t - min_t

            # randomized post-file buffer (10-30s) deterministic via rng
            buffer_ms = rng.randint(10_000, 30_000)
            time_cursor = max_t + buffer_ms
            pause_log["post_file_buffers"].append({"file": Path(f).name, "buffer_ms": buffer_ms})
        else:
            play_times[f] = 0

    total_minutes = round(sum(play_times.values()) / 60000) if play_times else 0
    parts = [part_from_filename(f) + f"[{round((play_times.get(f,0))/60000)}m] " for f in final_files]
    parts_joined = "".join(parts).rstrip()
    merged_fname = f"{total_minutes}m_v{version_num}_" + parts_joined + ".json"

    return merged_fname, merged, final_files, pause_log, excluded, total_minutes

# ---------- CLI ----------
def parse_args():
    p = argparse.ArgumentParser(description="Merge macros - compact always-both-pauses mode")
    p.add_argument("--versions", type=int, default=1, help="How many versions per group")
    p.add_argument("--force", action="store_true", help="Force reprocessing even if outputs exist")
    p.add_argument("--between-min", required=True, help="Between-files pause min (e.g. '10s' or '1m')")
    p.add_argument("--between-max", required=True, help="Between-files pause max")
    p.add_argument("--within-min", required=True, help="Within-file pause min (e.g. '1m')")
    p.add_argument("--within-max", required=True, help="Within-file pause max")
    p.add_argument("--min-pauses", type=int, default=2, help="Min pauses per file (used for within-file rule)")
    p.add_argument("--max-pauses", type=int, default=9, help="Max pauses per file")
    p.add_argument("--input-dir", default=DEFAULT_INPUT_DIR, help=argparse.SUPPRESS)
    p.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help=argparse.SUPPRESS)
    p.add_argument("--seed", type=int, default=DEFAULT_SEED, help=argparse.SUPPRESS)
    return p.parse_args()

def main():
    args = parse_args()
    try:
        args.between_min_secs = parse_time_str_to_seconds(args.between_min)
        args.between_max_secs = parse_time_str_to_seconds(args.between_max)
        args.within_min_secs = parse_time_str_to_seconds(args.within_min)
        args.within_max_secs = parse_time_str_to_seconds(args.within_max)
    except Exception as e:
        print("ERROR parsing time inputs:", e, file=sys.stderr)
        sys.exit(2)

    if args.between_min_secs > args.between_max_secs:
        print("ERROR: between-min > between-max", file=sys.stderr); sys.exit(2)
    if args.within_min_secs > args.within_max_secs:
        print("ERROR: within-min > within-max", file=sys.stderr); sys.exit(2)
    if args.min_pauses < 0 or args.max_pauses < 0 or args.min_pauses > args.max_pauses:
        print("ERROR: invalid min/max pauses", file=sys.stderr); sys.exit(2)

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    base_seed = args.seed
    processed = get_previously_processed_files(output_dir/"merged_bundle.zip")
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
