#!/usr/bin/env python3
"""
merge_macros.py

Simplified, hardcoded defaults for easy UI:

- input_dir (hardcoded): 'input'
- output_dir (hardcoded): 'output'
- seed (hardcoded): 12345

UI-exposed options (via CLI / workflow):
- --versions N
- --force (flag)
- --pause-target between|within
- --pause-min <human time, e.g. '10s' or '1m'>
- --pause-max <human time>
- --min-pauses <int> (used only for 'within' mode)
- --max-pauses <int> (used only for 'within' mode)

Behavior:
- If pause-target == 'between': insert a pause between each adjacent file (EXCEPT before first file).
  Each pause length is uniform random between pause-min and pause-max.
- If pause-target == 'within': for each file (EXCEPT first file), choose N between min-pauses and max-pauses
  (capped by available gaps), pick N distinct gaps and insert pauses there; each pause length sampled between pause-min and pause-max.
- Deterministic via hardcoded seed (12345). Use --seed to override if desired.
- Produces per-group logs and merged_bundle.zip in output_dir.
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

# ---------- Hardcoded defaults ----------
HARDCODED_INPUT_DIR = "input"
HARDCODED_OUTPUT_DIR = "output"
HARDCODED_SEED = 12345

# ---------- Helpers ----------
def parse_time_str_to_seconds(s: str):
    """Parse flexible time like '90', '90s', '1m', '2m47s', '1:30' into integer seconds."""
    if s is None:
        raise ValueError("time string is None")
    s = str(s).strip().lower()
    if not s:
        raise ValueError("empty time string")

    # mm:ss format
    if re.match(r'^\d+:\d{1,2}$', s):
        parts = s.split(':')
        mins = int(parts[0])
        secs = int(parts[1])
        if secs >= 60:
            raise ValueError(f"seconds part must be < 60 in '{s}'")
        return mins * 60 + secs

    # combined like '2m47s', '1m', '30s'
    m = re.match(r'^(?:(\d+)m)?\s*(?:(\d+)s)?$', s)
    if m:
        mm = int(m.group(1)) if m.group(1) else 0
        ss = int(m.group(2)) if m.group(2) else 0
        return mm * 60 + ss

    # numeric like '90' or '90.0' optionally with trailing s
    if re.match(r'^\d+(\.\d+)?s?$', s):
        s2 = s[:-1] if s.endswith('s') else s
        val = float(s2)
        return int(round(val))

    raise ValueError(f"Could not parse time value '{s}'")

def parse_args():
    p = argparse.ArgumentParser(description="Merge macro JSON files with simple pause controls (minimal UI).")
    p.add_argument("--versions", type=int, default=1, help="How many versions to generate per group (UI exposes this).")
    p.add_argument("--force", action="store_true", help="Force reprocessing even if outputs exist (UI checkbox).")
    p.add_argument("--pause-target", choices=("between","within"), required=True,
                   help="Where to insert pauses: 'between' entire files or 'within' each file.")
    p.add_argument("--pause-min", required=True, help="Shortest pause (e.g. 10s, 1m, 1:30).")
    p.add_argument("--pause-max", required=True, help="Longest pause (e.g. 4m53s).")
    p.add_argument("--min-pauses", type=int, default=2, help="Min pauses per file (used only for 'within').")
    p.add_argument("--max-pauses", type=int, default=9, help="Max pauses per file (used only for 'within').")
    # optional overrides (kept but not exposed in simple UI)
    p.add_argument("--input-dir", default=HARDCODED_INPUT_DIR, help=argparse.SUPPRESS)
    p.add_argument("--output-dir", default=HARDCODED_OUTPUT_DIR, help=argparse.SUPPRESS)
    p.add_argument("--seed", type=int, default=HARDCODED_SEED, help=argparse.SUPPRESS)
    return p.parse_args()

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

# ---------- Core merging ----------
def generate_version(files, rng, seed_for_intra, version_num, args, global_pause_set):
    """
    rng: a random.Random instance for inter-file and other choices (deterministic)
    seed_for_intra: base seed used to create per-file intra RNGs
    args: parsed CLI args
    global_pause_set: set for uniqueness of inter pauses (keeps older behavior)
    """
    if not files:
        return None, [], [], {}, [], 0

    m = len(files)
    # Exclude count behavior from original is not exposed in simple UI; keep it 0 (no excludes)
    exclude_count = 0
    excluded = []
    included = [f for f in files if f not in excluded]

    # duplicates & extras (keep original-ish behavior: duplicate up to 2 files)
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

    pause_min_ms = args.pause_min_secs * 1000
    pause_max_ms = args.pause_max_secs * 1000

    for idx, f in enumerate(final_files):
        evs_raw = load_json(Path(f)) or []
        evs = normalize_json(evs_raw)
        intra_log_local = []

        # --- INTRA-FILE PAUSES (only for 'within' mode AND NOT for the first file) ---
        if args.pause_target == "within" and idx != 0 and evs:
            # deterministic intra-file RNG per file/version
            intra_rng = random.Random((hash(f) & 0xffffffff) ^ (seed_for_intra + version_num))
            # decide count between min and max, but cap by available gaps
            n_gaps = max(0, len(evs) - 1)
            if n_gaps > 0:
                chosen_count = intra_rng.randint(args.min_pauses, args.max_pauses)
                chosen_count = min(chosen_count, n_gaps)
                # pick chosen_count distinct gaps
                chosen_gaps = intra_rng.sample(range(n_gaps), chosen_count)
                for gap_idx in sorted(chosen_gaps):
                    pause_ms = intra_rng.randint(pause_min_ms, pause_max_ms)
                    for j in range(gap_idx+1, len(evs)):
                        evs[j]["Time"] = int(evs[j].get("Time", 0)) + pause_ms
                    intra_log_local.append({"after_event_index": gap_idx, "pause_ms": pause_ms})
        if intra_log_local:
            pause_log["intra_file_pauses"].append({"file": Path(f).name, "pauses": intra_log_local})

        # --- INTER-FILE PAUSES (only for 'between' mode AND NOT before the first file) ---
        if args.pause_target == "between" and idx != 0:
            # choose a pause for the gap between previous and this file
            attempts = 0
            while True:
                inter_ms = rng.randint(pause_min_ms, pause_max_ms)
                if inter_ms not in global_pause_set or attempts > 200:
                    global_pause_set.add(inter_ms)
                    break
                attempts += 1
            time_cursor += inter_ms
            pause_log["inter_file_pauses"].append({"after_file": Path(f).name, "pause_ms": inter_ms, "is_before_index": idx})

        # Apply shifts and append events
        shifted = apply_shifts(evs, time_cursor)
        merged.extend(shifted)
        if shifted:
            # compute play time for this file
            max_t = max(int(e.get("Time", 0)) for e in shifted)
            min_t = min(int(e.get("Time", 0)) for e in shifted)
            play_times[f] = max_t - min_t

            # small randomized post-file buffer (10-30s) to separate files slightly; deterministic via rng
            buffer_ms = rng.randint(10_000, 30_000)
            time_cursor = max_t + buffer_ms
            pause_log["post_file_buffers"].append({"file": Path(f).name, "buffer_ms": buffer_ms})
        else:
            play_times[f] = 0

    parts = [part_from_filename(f) + f"[{round((play_times.get(f,0))/60000)}m] " for f in final_files]
    total_minutes = round(sum(play_times.values()) / 60000) if play_times else 0
    merged_fname = "".join(parts).rstrip() + f"_v{version_num}[{total_minutes}m].json"

    return merged_fname, merged, final_files, pause_log, excluded, total_minutes

def main():
    args = parse_args()

    # parse pause times
    try:
        args.pause_min_secs = parse_time_str_to_seconds(args.pause_min)
        args.pause_max_secs = parse_time_str_to_seconds(args.pause_max)
    except ValueError as ve:
        print(f"ERROR parsing pause times: {ve}", file=sys.stderr)
        sys.exit(2)

    if args.pause_min_secs > args.pause_max_secs:
        print("ERROR: pause-min must be <= pause-max", file=sys.stderr)
        sys.exit(2)

    # hardcoded values (but still allow override via CLI if someone wants)
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    seed = args.seed

    output_dir.mkdir(parents=True, exist_ok=True)

    base_seed = seed
    rng = random.Random(base_seed)
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
            # use rng for most choices; give each version a derived RNG
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

    # create ZIP
    zip_path = output_dir / "merged_bundle.zip"
    with ZipFile(zip_path, "w") as zf:
        for group_name, file_path in zip_items:
            zf.write(file_path, arcname=f"{group_name}/{file_path.name}")

    print("DONE. Outputs in:", output_dir)

if __name__ == "__main__":
    main()
