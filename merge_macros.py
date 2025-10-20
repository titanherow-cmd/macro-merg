#!/usr/bin/env python3
"""
merge_macros.py

Merges JSON macro files (nested groups under input-dir) into merged versions with
randomized intra-file and inter-file pauses. Produces merged JSON files in output-dir
and a merged_bundle.zip containing group folders.

Usage (example):
  python merge_macros.py --input-dir originals --output-dir output --versions 16 \
    --within-max-time "1m32s" --within-max-pauses 3 --between-max-time "2m37s" \
    --between-max-pauses 1 --exclude-count 5

Notes:
 - Intra-file pauses are always enabled (no toggle).
 - Elastic behavior: if provided max < hardcoded min, min becomes 0 and max is used.
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

# Hardcoded minimums (seconds)
DEFAULT_INTRA_MIN_SEC = 4     # 4 seconds
DEFAULT_INTER_MIN_SEC = 30    # 30 seconds

def parse_time_to_seconds(s: str) -> int:
    """Parse time inputs like '1m30s', '1:30', '1.30', '90s', '90' into seconds."""
    if s is None:
        raise ValueError("Empty time string")
    s = str(s).strip()
    if not s:
        raise ValueError("Empty time string")
    # mm:ss
    m = re.match(r'^(\d+):(\d{1,2})$', s)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    # m.ss (treat as minutes.seconds, e.g. 1.30 => 1m30s)
    m = re.match(r'^(\d+)\.(\d{1,2})$', s)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    # with letters like 1m30s, 90s, 2m
    m = re.match(r'^(?:(\d+)m)?(?:(\d+)s)?$', s)
    if m and (m.group(1) or m.group(2)):
        minutes = int(m.group(1)) if m.group(1) else 0
        seconds = int(m.group(2)) if m.group(2) else 0
        return minutes * 60 + seconds
    # pure integer (seconds)
    if re.match(r'^\d+$', s):
        return int(s)
    raise ValueError(f"Cannot parse time value: {s!r}")

def find_group_dirs(input_dir: Path):
    """Return immediate subdirectories inside input_dir (top-level groups)."""
    if not input_dir.exists() or not input_dir.is_dir():
        return []
    return [p for p in sorted(input_dir.iterdir()) if p.is_dir()]

def find_json_files_in_group(group_dir: Path):
    """Find json files recursively inside a group directory."""
    return sorted([p for p in group_dir.rglob("*.json") if p.is_file()])

def load_json_events(path: Path):
    """Load JSON and normalize into a list of events."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"WARNING: failed to read/parse {path}: {e}", file=sys.stderr)
        return []
    # If dict and contains list under common keys, return that list
    if isinstance(data, dict):
        for k in ("events","items","entries","records","actions","eventsList","events_array"):
            if k in data and isinstance(data[k], list):
                return deepcopy(data[k])
        # Single-event dict with Time -> wrap
        if "Time" in data:
            return [deepcopy(data)]
        return []
    if isinstance(data, list):
        return deepcopy(data)
    return []

def zero_base_events(events):
    """Shift events so the earliest event starts at time 0. Return events and duration (ms)."""
    if not events:
        return [], 0
    times = []
    for e in events:
        try:
            t = int(e.get("Time", 0))
        except Exception:
            try:
                t = int(float(e.get("Time", 0)))
            except Exception:
                t = 0
        times.append(t)
    min_t = min(times)
    shifted = []
    for e in events:
        ne = dict(e)
        try:
            t = int(ne.get("Time", 0))
        except Exception:
            try:
                t = int(float(ne.get("Time", 0)))
            except:
                t = 0
        ne["Time"] = t - min_t
        shifted.append(ne)
    duration_ms = max(int(e.get("Time", 0)) for e in shifted) if shifted else 0
    return shifted, duration_ms

def part_from_filename(fname: str):
    stem = Path(fname).stem
    return ''.join(ch for ch in stem if ch.isalnum())

def insert_intra_pauses(events, rng, max_pauses, min_s, max_s):
    """Insert up to max_pauses random pauses inside events.
    Returns modified events and list of inserted pauses info."""
    if not events or max_pauses <= 0:
        return deepcopy(events), []
    evs = deepcopy(events)
    n = len(evs)
    if n < 2:
        return evs, []
    # choose k between 0..max_pauses (inclusive) but no more than gaps
    k = rng.randint(0, min(max_pauses, n-1))
    if k == 0:
        return evs, []
    chosen = rng.sample(range(n-1), k)
    pauses = []
    for gap_idx in sorted(chosen):
        pause_s = rng.randint(min_s, max_s)
        pause_ms = pause_s * 1000
        for j in range(gap_idx+1, n):
            evs[j]["Time"] = int(evs[j].get("Time", 0)) + pause_ms
        pauses.append({"after_event_index": gap_idx, "pause_ms": pause_ms})
    return evs, pauses

def apply_shifts(events, shift_ms):
    shifted = []
    for e in events:
        ne = dict(e)
        try:
            t = int(ne.get("Time", 0))
        except Exception:
            try:
                t = int(float(ne.get("Time", 0)))
            except:
                t = 0
        ne["Time"] = t + int(shift_ms)
        shifted.append(ne)
    return shifted

def compute_total_minutes(merged_events):
    if not merged_events:
        return 0
    times = [int(e.get("Time", 0)) for e in merged_events]
    min_t = min(times); max_t = max(times)
    total_minutes = round((max_t - min_t) / 60000)
    return total_minutes

def safe_sample(population, k, rng):
    if not population or k <= 0:
        return []
    if k >= len(population):
        # return all except ensure at least 1 left if used for exclusion
        return list(population)
    return rng.sample(population, k=k)

def generate_version_for_group(files, rng, version_num,
                               exclude_count,
                               within_min_s, within_max_s, within_max_pauses,
                               between_min_s, between_max_s):
    """Return: (filename, merged_events, final_file_list, pause_info, excluded_list, total_minutes)"""
    if not files:
        return None, [], [], {}, [], 0

    m = len(files)
    # Limit exclude_count so at least 1 file remains
    ex_count = max(0, min(exclude_count, max(0, m-1)))
    excluded = safe_sample(files, ex_count, rng) if ex_count > 0 else []
    included = [f for f in files if f not in excluded]
    if not included:
        included = files.copy()

    # Duplication: optionally duplicate up to 2 files (safe)
    dup_files = []
    if included:
        dup_count = min(2, len(included))
        if dup_count > 0:
            if len(included) == 1:
                dup_files = [included[0]]
            else:
                dup_files = safe_sample(included, dup_count, rng)
    final_files = included + dup_files

    # Optionally insert extra copies (1 or 2), safe
    if included:
        try:
            extra_k = rng.choice([1,2])
            extra_files = safe_sample(included, extra_k, rng)
            for ef in extra_files:
                pos = rng.randrange(len(final_files)+1)
                if pos > 0 and final_files[pos-1] == ef:
                    pos = min(pos+1, len(final_files))
                final_files.insert(min(pos, len(final_files)), ef)
        except Exception:
            pass

    rng.shuffle(final_files)

    merged = []
    pause_info = {"inter_file_pauses": [], "intra_file_pauses": []}
    time_cursor = 0
    play_times = {}

    for idx, fpath in enumerate(final_files):
        evs = load_json_events(Path(fpath))
        zb_evs, duration_ms = zero_base_events(evs)

        # Intra-file pauses (always enabled)
        intra_evs, intra_details = insert_intra_pauses(zb_evs, rng, within_max_pauses, within_min_s, within_max_s)
        if intra_details:
            pause_info["intra_file_pauses"].append({"file": Path(fpath).name, "pauses": intra_details})

        # Shift by cursor and append
        shifted = apply_shifts(intra_evs, time_cursor)
        merged.extend(shifted)

        if shifted:
            file_max = max(int(e.get("Time", 0)) for e in shifted)
            file_min = min(int(e.get("Time", 0)) for e in shifted)
            play_times[str(fpath)] = file_max - file_min
            time_cursor = file_max + 30  # tiny gap
        else:
            play_times[str(fpath)] = 0

        # AFTER file: inter-file pause (between files)
        if idx < len(final_files) - 1:
            pause_s = rng.randint(between_min_s, between_max_s)
            pause_ms = pause_s * 1000
            time_cursor += pause_ms
            pause_info["inter_file_pauses"].append({"after_file": Path(fpath).name, "pause_ms": pause_ms, "after_index": idx})

    total_minutes = compute_total_minutes(merged)
    # Build filename: M_{total}m= <parts...>
    parts = []
    for f in final_files:
        dur_ms = play_times.get(str(f), 0)
        parts.append(f"{part_from_filename(str(Path(f).name))}[{round(dur_ms/60000)}m]")
    # join with '-' and ensure no illegal filename chars
    base_name = f"M_{total_minutes}m= " + " - ".join(parts)
    safe_name = ''.join(ch for ch in base_name if ch not in '/\\:*?"<>|')
    merged_fname = f"{safe_name}.json"
    return merged_fname, merged, [str(p) for p in final_files], pause_info, [str(p) for p in excluded], total_minutes

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-dir", required=False, default="originals")
    ap.add_argument("--output-dir", required=False, default="output")
    ap.add_argument("--versions", type=int, default=16)
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--exclude-count", type=int, default=5)
    ap.add_argument("--within-max-time", type=str, default="1m32s")
    ap.add_argument("--within-max-pauses", type=int, default=3)
    ap.add_argument("--between-max-time", type=str, default="2m37s")
    ap.add_argument("--between-max-pauses", type=int, default=1)
    args = ap.parse_args()

    rng = random.Random(args.seed) if args.seed is not None else random.Random()

    input_root = Path(args.input_dir)
    output_root = Path(args.output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    # find group directories (top-level folders under input_root)
    groups = find_group_dirs(input_root)
    if not groups:
        print(f"No group subfolders found under: {input_root}")
        return

    # parse times and apply elastic min logic
    try:
        within_max_s = parse_time_to_seconds(args.within_max_time)
    except Exception as e:
        print(f"ERROR parsing within max time: {e}", file=sys.stderr); return
    try:
        between_max_s = parse_time_to_seconds(args.between_max_time)
    except Exception as e:
        print(f"ERROR parsing between max time: {e}", file=sys.stderr); return

    within_min_s = DEFAULT_INTRA_MIN_SEC if within_max_s >= DEFAULT_INTRA_MIN_SEC else 0
    between_min_s = DEFAULT_INTER_MIN_SEC if between_max_s >= DEFAULT_INTER_MIN_SEC else 0

    all_zip_entries = []

    for gi, grp in enumerate(groups):
        files = find_json_files_in_group(grp)
        if not files:
            print(f"Skipping group {grp} (no json files found).")
            continue

        for v in range(1, max(1, args.versions) + 1):
            merged_fname, merged_events, finals, pauses, excluded, total_minutes = generate_version_for_group(
                files, rng, v,
                args.exclude_count,
                within_min_s, within_max_s, args.within_max_pauses,
                between_min_s, between_max_s
            )
            if not merged_fname:
                continue
            out_path = output_root / merged_fname
            # write merged json (as top-level array)
            try:
                out_path.write_text(json.dumps(merged_events, indent=2, ensure_ascii=False), encoding="utf-8")
                print(f"WROTE: {out_path}")
                all_zip_entries.append((grp.name, out_path))
            except Exception as e:
                print(f"ERROR writing {out_path}: {e}", file=sys.stderr)

    # create zip (grouped)
    zip_path = output_root / "merged_bundle.zip"
    with ZipFile(zip_path, "w") as zf:
        for group_name, file_path in all_zip_entries:
            arcname = f"{group_name}/{file_path.name}"
            try:
                zf.write(file_path, arcname=arcname)
            except Exception as e:
                print(f"WARNING: could not add {file_path} to zip: {e}", file=sys.stderr)

    print("DONE. Created zip:", zip_path)

if __name__ == "__main__":
    main()
