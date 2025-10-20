#!/usr/bin/env python3
"""
merge_macros.py

Designed to work with the simplified UI (six fields). Behavior:
 - Defaults: input-dir="originals", output-dir="output"
 - Expects args:
     --versions N
     --within-max-time '1m30s' (string parsed)
     --within-max-pauses N
     --between-max-time '4m53s'
     --between-max-pauses N  (ignored, here for compatibility)
     --exclude-count N
 - Intra-file pauses always enabled.
 - Elastic min behaviour:
     * DEFAULT_INTRA_MIN = 4s, DEFAULT_INTER_MIN = 30s
     * If UI max < default_min → set min = 0 and use max = UI_max
 - Inter-file pauses added AFTER each file (i.e. pause between files).
 - Total merged time in filename computed from merged events (includes pauses).
"""

from pathlib import Path
import argparse
import json
import glob
import random
from copy import deepcopy
from zipfile import ZipFile
import re
import sys

# Hardcoded defaults (seconds)
DEFAULT_INTRA_MIN_SEC = 4        # 4s
DEFAULT_INTRA_MAX_SEC = 2*60 + 47  # 167s (2m47s)
DEFAULT_INTER_MIN_SEC = 30      # 30s
DEFAULT_INTER_MAX_SEC = 4*60 + 53  # 293s (4m53s)

def parse_time_to_seconds(s: str) -> int:
    """Parse various time formats to seconds.
    Accepts: '1m30s', '1:30', '1.30' (meaning 1m30s), '90s', '90' (seconds), '2m'.
    Returns integer seconds (>=0). Raises ValueError if cannot parse.
    """
    if s is None:
        raise ValueError("Empty time string")
    s = str(s).strip()
    if not s:
        raise ValueError("Empty time string")
    # mm:ss
    m = re.match(r'^(\d+):(\d{1,2})$', s)
    if m:
        mins = int(m.group(1)); secs = int(m.group(2))
        return mins*60 + secs
    # m.ss (treat as minutes.seconds, e.g. 1.30 => 1m30s)
    m = re.match(r'^(\d+)\.(\d{1,2})$', s)
    if m:
        mins = int(m.group(1)); secs = int(m.group(2))
        return mins*60 + secs
    # with letters like 1m30s, 90s, 2m
    m = re.match(r'^(?:(\d+)m)?(?:(\d+)s)?$', s)
    if m and (m.group(1) or m.group(2)):
        mins = int(m.group(1)) if m.group(1) else 0
        secs = int(m.group(2)) if m.group(2) else 0
        return mins*60 + secs
    # pure integer (seconds)
    if re.match(r'^\d+$', s):
        return int(s)
    raise ValueError(f"Cannot parse time value: {s}")

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input-dir", default="originals", help="Parent directory containing group subfolders (default: originals)")
    p.add_argument("--output-dir", default="output", help="Where to write output files/zip (default: output)")
    p.add_argument("--versions", type=int, default=5, help="Number of versions per group")
    p.add_argument("--within-max-time", default=f"{DEFAULT_INTRA_MAX_SEC//60}m{DEFAULT_INTRA_MAX_SEC%60}s",
                   help="Within-file max pause time (string)")
    p.add_argument("--within-max-pauses", type=int, default=4, help="Max pauses inside each file")
    p.add_argument("--between-max-time", default=f"{DEFAULT_INTER_MAX_SEC//60}m{DEFAULT_INTER_MAX_SEC%60}s",
                   help="Between-files max pause time (string)")
    p.add_argument("--between-max-pauses", type=int, default=1, help="Max pauses between files (ignored; for compatibility)")
    p.add_argument("--exclude-count", type=int, default=1, help="Max files to randomly exclude per version")
    return p.parse_args()

def load_json(path: Path):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as e:
        print(f"WARNING: cannot parse {path}: {e}", file=sys.stderr)
        return None

def normalize_json(data):
    if isinstance(data, dict):
        for k in ("events","items","entries","records","actions","eventsList","events_array"):
            if k in data and isinstance(data[k], list):
                return data[k]
        if "Time" in data:
            return [data]
        return []
    if isinstance(data, list):
        return data
    return []

def find_groups(input_dir: Path):
    if not input_dir.exists() or not input_dir.is_dir():
        return []
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
            except Exception:
                t = 0
        ne = dict(e)
        ne["Time"] = t + int(shift_ms)
        shifted.append(ne)
    return shifted

def insert_intra_pauses_fixed(events, rng, num_pauses, min_seconds, max_seconds):
    """Insert num_pauses random pauses inside events. min_seconds..max_seconds (seconds)."""
    if not events or num_pauses <= 0:
        return deepcopy(events), []
    evs = deepcopy(events)
    n = len(evs)
    if n < 2:
        return evs, []
    max_gaps = n - 1
    k = min(num_pauses, max_gaps)
    # choose k distinct gaps
    chosen = rng.sample(range(max_gaps), k) if k > 0 else []
    chosen_sorted = sorted(chosen)
    pauses_info = []
    for gap_idx in chosen_sorted:
        # pick pause between min..max (seconds)
        pause_s = rng.randint(min_seconds, max_seconds)
        pause_ms = pause_s * 1000
        for j in range(gap_idx+1, n):
            evs[j]["Time"] = int(evs[j].get("Time", 0)) + pause_ms
        pauses_info.append({"after_event_index": gap_idx, "pause_ms": pause_ms})
    return evs, pauses_info

def compute_total_minutes_from_events(events):
    if not events:
        return 0
    times = [int(e.get("Time", 0)) for e in events]
    min_t = min(times); max_t = max(times)
    total_minutes = round((max_t - min_t) / 60000)
    return total_minutes

def safe_sample(pop, k, rng):
    """Return up to k unique items from pop safely."""
    if not pop or k <= 0:
        return []
    if k >= len(pop):
        return list(pop)
    return rng.sample(pop, k=k)

def generate_version_for_group(files, rng, version_num, exclude_count,
                               within_min_s, within_max_s, within_max_pauses,
                               between_min_s, between_max_s):
    """Produce one merged version for one group's files."""
    if not files:
        return None, [], [], [], 0

    m = len(files)
    exclude_count = max(0, min(exclude_count, m-1))
    excluded = safe_sample(files, exclude_count, rng)
    included = [f for f in files if f not in excluded]

    # simple duplication: duplicate up to 2 files if available
    dup_files = []
    if included:
        dup_count = min(2, len(included))
        if len(included) == 1:
            dup_files = [included[0]] if dup_count >= 1 else []
        else:
            dup_files = safe_sample(included, dup_count, rng)
    final_files = included + dup_files

    # optionally insert extra copies (safe)
    extra_files = []
    if included:
        choice_k = rng.choice([1,2]) if len(included) >= 1 else 0
        extra_files = safe_sample(included, choice_k, rng)
    for ef in extra_files:
        pos = rng.randrange(len(final_files)+1)
        if pos > 0 and final_files[pos-1] == ef:
            pos = min(pos+1, len(final_files))
        final_files.insert(min(pos, len(final_files)), ef)

    rng.shuffle(final_files)

    merged = []
    inter_pauses = []
    intra_pauses = []
    time_cursor = 0
    play_times = {}

    for idx, f in enumerate(final_files):
        evs = normalize_json(load_json(Path(f)) or [])
        # decide number of intra pauses for this file: random between 0..within_max_pauses
        num_intra = rng.randint(0, max(0, within_max_pauses))
        evs_with_intra, intra_info = insert_intra_pauses_fixed(evs, rng, num_intra, within_min_s, within_max_s)
        if intra_info:
            intra_pauses.append({"file": Path(f).name, "pauses": intra_info})

        # shift events by current cursor and append
        shifted = apply_shifts(evs_with_intra, time_cursor)
        merged.extend(shifted)
        if shifted:
            max_t = max(int(e.get("Time", 0)) for e in shifted)
            min_t = min(int(e.get("Time", 0)) for e in shifted)
            play_times[f] = max_t - min_t
            time_cursor = max_t + 30
        else:
            play_times[f] = 0

        # AFTER this file, add an inter-file pause (if not last file)
        if idx < len(final_files) - 1:
            # choose a single pause between between_min_s..between_max_s
            pause_s = rng.randint(between_min_s, between_max_s)
            pause_ms = pause_s * 1000
            inter_pauses.append({"after_file": Path(f).name, "pause_ms": pause_ms, "after_index": idx})
            time_cursor += pause_ms

    # parts label string
    parts = [f"{''.join(ch for ch in Path(f).stem if ch.isalnum())}[{round((play_times.get(f,0) or 0)/60000)}m] " for f in final_files]
    total_minutes = compute_total_minutes_from_events(merged)
    merged_fname = "".join(parts).rstrip() + f"_v{version_num}[{total_minutes}m].json"
    return merged_fname, merged, final_files, {"inter_file_pauses": inter_pauses, "intra_file_pauses": intra_pauses}, excluded, total_minutes

def main():
    args = parse_args()

    # Parse max times from UI
    try:
        within_max_s = parse_time_to_seconds(args.within_max_time)
    except Exception as e:
        print(f"ERROR parsing within max time: {e}", file=sys.stderr); sys.exit(2)
    try:
        between_max_s = parse_time_to_seconds(args.between_max_time)
    except Exception as e:
        print(f"ERROR parsing between max time: {e}", file=sys.stderr); sys.exit(2)

    # Elastic adjustment: if UI max < hardcoded default min, set min to 0
    within_min_s = DEFAULT_INTRA_MIN_SEC if within_max_s >= DEFAULT_INTRA_MIN_SEC else 0
    between_min_s = DEFAULT_INTER_MIN_SEC if between_max_s >= DEFAULT_INTER_MIN_SEC else 0

    # enforce at least 0
    within_min_s = max(0, within_min_s)
    between_min_s = max(0, between_min_s)

    # other params
    within_max_pauses = max(0, int(args.within_max_pauses))
    between_max_pauses = max(0, int(args.between_max_pauses))  # ignored in logic, kept for compatibility
    exclude_count = max(0, int(args.exclude_count))
    versions = max(1, int(args.versions))

    input_root = Path(args.input_dir)
    output_root = Path(args.output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    rng = random.Random()  # non-deterministic; UI no seed provided
    zip_items = []

    groups = find_groups(input_root)
    if not groups:
        print(f"No group subfolders found in input dir: {input_root}", file=sys.stderr)

    for gi, grp in enumerate(groups):
        files = find_json_files(grp)
        if not files:
            continue
        for v in range(1, versions + 1):
            fname, merged, finals, pauses, excluded, total = generate_version_for_group(
                files, rng, v, exclude_count,
                within_min_s, within_max_s, within_max_pauses,
                between_min_s, between_max_s)
            if not fname:
                continue
            out_file = output_root / fname
            with open(out_file, "w", encoding="utf-8") as fh:
                json.dump(merged, fh, indent=2, ensure_ascii=False)
            zip_items.append((grp.name, out_file))

    # create zip at output/merged_bundle.zip with group subfolders
    zip_path = output_root / "merged_bundle.zip"
    with ZipFile(zip_path, "w") as zf:
        for group_name, file_path in zip_items:
            arcname = f"{group_name}/{file_path.name}"
            zf.write(file_path, arcname=arcname)

    print("DONE. Wrote:", zip_path)

if __name__ == "__main__":
    main()
