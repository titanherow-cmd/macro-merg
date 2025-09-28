#!/usr/bin/env python3
"""
merge_macros.py

Compact, updated script keyed to a small workflow UI:
- Hardcoded defaults: input='input', output='output', seed=12345
- CLI-exposed (from compact workflow):
    --versions N
    --force
    --pause-target between|within
    --pause-range "MIN-MAX"  (e.g. "10s-4m53s" or "1m-2m47s")
    --pauses-count "MIN-MAX" (e.g. "2-9")  (used only with pause-target=within)
    --extra <string> (optional, ignored by script; for future/advanced overrides)
- First file (index 0) is exempt from intra-file pauses and from any inter-file pause BEFORE it.
- Filenames use the new format: {TOTALm}_v{VERSION}_<parts>.json
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

# Hardcoded defaults (per your request)
DEFAULT_INPUT_DIR = "input"
DEFAULT_OUTPUT_DIR = "output"
DEFAULT_SEED = 12345

# ----------------- helpers -----------------
def parse_time_str_to_seconds(s: str):
    """Parse '90', '90s', '1m', '2m47s', '1:30' into integer seconds."""
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
    # combined like '2m47s' or '1m' or '30s'
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

def parse_range_str_to_seconds(range_str: str):
    """Parse 'MIN-MAX' where MIN and MAX are human time strings; return (min_secs, max_secs)."""
    if '-' not in range_str:
        raise ValueError("range must be in MIN-MAX format, e.g. '10s-4m53s'")
    left, right = range_str.split('-', 1)
    min_s = parse_time_str_to_seconds(left.strip())
    max_s = parse_time_str_to_seconds(right.strip())
    if min_s > max_s:
        raise ValueError(f"range min ({min_s}s) > max ({max_s}s)")
    return min_s, max_s

def parse_count_range(count_str: str):
    """Parse 'MIN-MAX' where MIN and MAX are integers. Return (min_count, max_count)."""
    if '-' not in count_str:
        raise ValueError("pauses count must be in MIN-MAX format, e.g. '2-9'")
    left, right = count_str.split('-', 1)
    mi = int(left.strip())
    ma = int(right.strip())
    if mi < 0 or ma < 0 or mi > ma:
        raise ValueError("invalid pauses count range")
    return mi, ma

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

# ----------------- core merging -----------------
def generate_version(files, rng, seed_for_intra, version_num, args, global_pause_set):
    """
    rng: random.Random for inter-file decisions, post buffers, filename ordering, duplicates, etc.
    seed_for_intra: integer used to derive per-file intra RNG.
    """
    if not files:
        return None, [], [], {}, [], 0

    # In this compact UI we do not expose excludes; keep none.
    excluded = []
    included = [f for f in files if f not in excluded]

    # keep duplicate behavior similar to before
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

    # convert parsed seconds to ms
    pause_min_ms = args.pause_min_secs * 1000
    pause_max_ms = args.pause_max_secs * 1000
    min_pauses = getattr(args, "min_pauses", 0)
    max_pauses = getattr(args, "max_pauses", 0)

    for idx, f in enumerate(final_files):
        evs_raw = load_json(Path(f)) or []
        evs = normalize_json(evs_raw)
        intra_log_local = []

        # --- INTRA-FILE PAUSES: only for 'within' and NOT for the first file ---
        if args.pause_target == "within" and idx != 0 and evs:
            n_gaps = max(0, len(evs) - 1)
            if n_gaps > 0:
                # deterministic intra RNG per file/version
                intra_rng = random.Random((hash(f) & 0xffffffff) ^ (seed_for_intra + version_num))
                chosen_count = intra_rng.randint(min_pauses, max_pauses)
                chosen_count = min(chosen_count, n_gaps)
                chosen_gaps = intra_rng.sample(range(n_gaps), chosen_count)
                for gap_idx in sorted(chosen_gaps):
                    pause_ms = intra_rng.randint(pause_min_ms, pause_max_ms)
                    for j in range(gap_idx+1, len(evs)):
                        evs[j]["Time"] = int(evs[j].get("Time", 0)) + pause_ms
                    intra_log_local.append({"after_event_index": gap_idx, "pause_ms": pause_ms})
        if intra_log_local:
            pause_log["intra_file_pauses"].append({"file": Path(f).name, "pauses": intra_log_local})

        # --- INTER-FILE PAUSES: only for 'between' and NOT before the first file ---
        if args.pause_target == "between" and idx != 0:
            attempts = 0
            while True:
                inter_ms = rng.randint(pause_min_ms, pause_max_ms)
                if inter_ms not in global_pause_set or attempts > 200:
                    global_pause_set.add(inter_ms)
                    break
                attempts += 1
            time_cursor += inter_ms
            pause_log["inter_file_pauses"].append({"after_file": Path(f).name, "pause_ms": inter_ms, "is_before_index": idx})

        # --- apply shifts ---
        shifted = apply_shifts(evs, time_cursor)
        merged.extend(shifted)
        if shifted:
            max_t = max(int(e.get("Time", 0)) for e in shifted)
            min_t = min(int(e.get("Time", 0)) for e in shifted)
            play_times[f] = max_t - min_t

            # post-file buffer (kept 10-30s deterministic)
            buffer_ms = rng.randint(10_000, 30_000)
            time_cursor = max_t + buffer_ms
            pause_log["post_file_buffers"].append({"file": Path(f).name, "buffer_ms": buffer_ms})
        else:
            play_times[f] = 0

    # build filename: {TOTALm}_v{version}_ + parts
    total_minutes = round(sum(play_times.values()) / 60000) if play_times else 0
    parts = [part_from_filename(f) + f"[{round((play_times.get(f,0))/60000)}m] " for f in final_files]
    # join parts and strip trailing space
    parts_joined = "".join(parts).rstrip()
    merged_fname = f"{total_minutes}m_v{version_num}_" + parts_joined + ".json"

    return merged_fname, merged, final_files, pause_log, excluded, total_minutes

# ----------------- entrypoint -----------------
def parse_args():
    p = argparse.ArgumentParser(description="Merge macros (compact UI).")
    p.add_argument("--versions", type=int, default=1, help="How many versions per group")
    p.add_argument("--force", action="store_true", help="Force reprocessing even if outputs exist")
    p.add_argument("--pause-target", choices=("between", "within"), required=True,
                   help="Where to insert pauses: between files or within each file")
    p.add_argument("--pause-range", required=True,
                   help="Pause length range MIN-MAX in formats like '10s-4m53s' or '1m-2m47s'")
    p.add_argument("--pauses-count", required=False, default="2-9",
                   help="Min-Max pauses per file (only used for 'within' mode), e.g. '2-9'")
    p.add_argument("--extra", required=False, default="", help=argparse.SUPPRESS)  # optional advanced overrides (ignored)
    # allow overriding defaults if needed
    p.add_argument("--input-dir", default=DEFAULT_INPUT_DIR, help=argparse.SUPPRESS)
    p.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help=argparse.SUPPRESS)
    p.add_argument("--seed", type=int, default=DEFAULT_SEED, help=argparse.SUPPRESS)
    return p.parse_args()

def main():
    args = parse_args()

    # parse pause-range
    try:
        min_s, max_s = parse_range_str_to_seconds(args.pause_range)
    except Exception as e:
        print("ERROR parsing pause-range:", e, file=sys.stderr)
        sys.exit(2)
    args.pause_min_secs = min_s
    args.pause_max_secs = max_s

    # parse pauses-count
    try:
        min_count, max_count = parse_count_range(args.pauses_count)
    except Exception as e:
        print("ERROR parsing pauses-count:", e, file=sys.stderr)
        sys.exit(2)
    args.min_pauses = min_count
    args.max_pauses = max_count

    if args.min_pauses > args.max_pauses:
        print("ERROR: min_pauses > max_pauses", file=sys.stderr)
        sys.exit(2)

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    seed = args.seed

    output_dir.mkdir(parents=True, exist_ok=True)

    base_seed = seed
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
            # per-version RNG deterministically derived
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
