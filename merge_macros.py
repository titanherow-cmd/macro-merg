#!/usr/bin/env python3
"""
merge_macros.py

Simplified script matching the compact UI:

- Hardcoded defaults:
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
    """
    Accepts:
      - '1.30'  -> 1 minute 30 seconds
      - '1:30'  -> 1 minute 30 seconds
      - '1m30s' -> 1 minute 30 seconds
      - '90s' or '90' -> 90 seconds
      - '2m'   -> 120 seconds
    Returns integer seconds.
    """
    if s is None:
        raise ValueError("time string is None")
    s0 = str(s).strip().lower()
    if not s0:
        raise ValueError("empty time string")

    # dot format M.SS (e.g. 1.30)
    mdot = re.match(r'^(\d+)\.(\d{1,2})$', s0)
    if mdot:
        mins = int(mdot.group(1))
        secs = int(mdot.group(2))
        if secs >= 60:
            # allow "1.75" meaning 1 min 75s? no — require secs < 60
            raise ValueError(f"seconds part must be <60 in '{s}'")
        return mins * 60 + secs

    # colon format M:SS
    mcol = re.match(r'^(\d+):(\d{1,2})$', s0)
    if mcol:
        mins = int(mcol.group(1)); secs = int(mcol.group(2))
        if secs >= 60:
            raise ValueError(f"seconds part must be <60 in '{s}'")
        return mins * 60 + secs

    # combined m and s like '2m47s' or '1m' or '30s'
    m = re.match(r'^(?:(\d+)m)?\s*(?:(\d+)s)?$', s0)
    if m and (m.group(1) or m.group(2)):
        mm = int(m.group(1)) if m.group(1) else 0
        ss = int(m.group(2)) if m.group(2) else 0
        return mm * 60 + ss

    # numeric like '90' or '90s'
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
def generate_version(files, rng, seed_for_intra, version_num, args, global_pause_set):
    if not files:
        return None, [], [], {}, [], 0

    excluded = []
    included = [f for f in files if f not in excluded]

    # duplicates & extras (keep behavior similar to earlier)
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

    # convert seconds to ms; minimum duration is 1 second (1000 ms)
    within_max_ms = max(1000, args.within_max_secs * 1000)
    between_max_ms = max(1000, args.between_max_secs * 1000)
    min_pauses = max(1, args.within_max_pauses)  # we interpret the UI "max pauses" as upper bound; we'll pick 1..max
    max_pauses = max(1, args.within_max_pauses)
    between_max_p = max(1, args.between_max_pauses)

    for idx, f in enumerate(final_files):
        evs_raw = load_json(Path(f)) or []
        evs = normalize_json(evs_raw)
        intra_log_local = []

        # INTRA-FILE: apply for all except first file (first file exempt)
        if idx != 0 and evs and len(evs) > 1:
            n_gaps = len(evs) - 1
            # choose number of pauses between 1 and within_max_pauses (cap by available gaps)
            # Use a file-specific deterministic RNG for placement & lengths
            intra_rng = random.Random((hash(f) & 0xffffffff) ^ (seed_for_intra + version_num))
            chosen_count = intra_rng.randint(1, args.within_max_pauses)
            chosen_count = min(chosen_count, n_gaps)
            if chosen_count > 0:
                chosen_gaps = intra_rng.sample(range(n_gaps), chosen_count)
                for gap_idx in sorted(chosen_gaps):
                    pause_ms = intra_rng.randint(1000, within_max_ms)
                    for j in range(gap_idx+1, len(evs)):
                        evs[j]["Time"] = int(evs[j].get("Time", 0)) + pause_ms
                    intra_log_local.append({"after_event_index": gap_idx, "pause_ms": pause_ms})
        if intra_log_local:
            pause_log["intra_file_pauses"].append({"file": Path(f).name, "pauses": intra_log_local})

        # INTER-FILE: before this file (except before first file)
        if idx != 0:
            # pick number of pauses between 1 and between_max_pauses
            k = rng.randint(1, args.between_max_pauses)
            # insert k pauses sequentially
            total_inter_ms = 0
            inter_list = []
            for i in range(k):
                inter_ms = rng.randint(1000, between_max_ms)
                total_inter_ms += inter_ms
                inter_list.append(inter_ms)
            time_cursor += total_inter_ms
            pause_log["inter_file_pauses"].append({"before_file": Path(f).name, "pause_ms_list": inter_list, "is_before_index": idx})

        # apply shifts and append events
        shifted = apply_shifts(evs, time_cursor)
        merged.extend(shifted)
        if shifted:
            max_t = max(int(e.get("Time", 0)) for e in shifted)
            min_t = min(int(e.get("Time", 0)) for e in shifted)
            play_times[f] = max_t - min_t

            # post-file buffer (kept 10-30s)
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
    p = argparse.ArgumentParser(description="Merge macros - compact UI (within & between pauses).")
    p.add_argument("--versions", type=int, default=6, help="How many versions per group")
    # force passed as dropdown in workflow; here it's a boolean flag if present
    p.add_argument("--force", action="store_true", help="Force reprocessing even if outputs exist")
    p.add_argument("--within-max-time", required=True, help="Within-file max pause time (e.g. 1.30, 1m30s, 1:30)")
    p.add_argument("--within-max-pauses", type=int, required=True, help="Max number of pauses to add inside each file (we pick 1..max)")
    p.add_argument("--between-max-time", required=True, help="Between-files max pause time (e.g. 10s, 1m)")
    p.add_argument("--between-max-pauses", type=int, required=True, help="Max number of pauses to insert between files (usually 1)")
    # hidden overrides
    p.add_argument("--input-dir", default=DEFAULT_INPUT_DIR, help=argparse.SUPPRESS)
    p.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help=argparse.SUPPRESS)
    p.add_argument("--seed", type=int, default=DEFAULT_SEED, help=argparse.SUPPRESS)
    return p.parse_args()

def main():
    args = parse_args()
    # parse times (into seconds)
    try:
        args.within_max_secs = parse_time_str_to_seconds(args.within_max_time)
        args.between_max_secs = parse_time_str_to_seconds(args.between_max_time)
    except Exception as e:
        print("ERROR parsing time inputs:", e, file=sys.stderr)
        sys.exit(2)

    if args.within_max_secs < 1 or args.between_max_secs < 1:
        print("ERROR: max times must represent at least 1 second", file=sys.stderr); sys.exit(2)
    if args.within_max_pauses < 1 or args.between_max_pauses < 1:
        print("ERROR: max pauses must be >= 1", file=sys.stderr); sys.exit(2)

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

    # create zip
    zip_path = output_dir / "merged_bundle.zip"
    with ZipFile(zip_path, "w") as zf:
        for group_name, file_path in zip_items:
            zf.write(file_path, arcname=f"{group_name}/{file_path.name}")

    print("DONE. Outputs in:", output_dir)

if __name__ == "__main__":
    main()
