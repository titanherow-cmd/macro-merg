#!/usr/bin/env python3
"""
merge_macros.py

Backward-compatible CLI: accepts both legacy flags and newer names.
Added alias: --inter-file-count -> between max pauses (kept for compatibility).

Usage examples:
  python3 merge_macros.py --input-dir originals --output-dir output --versions 16 \
    --within-max-time "1m32s" --within-max-pauses 3 --between-max-time "2m37s" \
    --between-max-pauses 1 --exclude-count 5

  or (legacy flags):
  python3 merge_macros.py --intra-file-max "1m32s" --intra-file-max-pauses 3 \
    --inter-file-max "2m37s" --inter-file-count 1 --exclude-count 5
"""

from pathlib import Path
import argparse
import json
import random
import re
import sys
from copy import deepcopy
from zipfile import ZipFile

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
    m = re.match(r'^(\d+):(\d{1,2})$', s)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    m = re.match(r'^(\d+)\.(\d{1,2})$', s)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    m = re.match(r'^(?:(\d+)m)?(?:(\d+)s)?$', s)
    if m and (m.group(1) or m.group(2)):
        minutes = int(m.group(1)) if m.group(1) else 0
        seconds = int(m.group(2)) if m.group(2) else 0
        return minutes * 60 + seconds
    if re.match(r'^\d+$', s):
        return int(s)
    raise ValueError(f"Cannot parse time value: {s!r}")

def find_group_dirs(input_dir: Path):
    if not input_dir.exists() or not input_dir.is_dir():
        return []
    return [p for p in sorted(input_dir.iterdir()) if p.is_dir()]

def find_json_files_in_group(group_dir: Path):
    return sorted([p for p in group_dir.rglob("*.json") if p.is_file()])

def load_json_events(path: Path):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"WARNING: failed to read/parse {path}: {e}", file=sys.stderr)
        return []
    if isinstance(data, dict):
        for k in ("events","items","entries","records","actions","eventsList","events_array"):
            if k in data and isinstance(data[k], list):
                return deepcopy(data[k])
        if "Time" in data:
            return [deepcopy(data)]
        return []
    if isinstance(data, list):
        return deepcopy(data)
    return []

def zero_base_events(events):
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
    if not events or max_pauses <= 0:
        return deepcopy(events), []
    evs = deepcopy(events)
    n = len(evs)
    if n < 2:
        return evs, []
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
    return round((max_t - min_t) / 60000)

def safe_sample(population, k, rng):
    if not population or k <= 0:
        return []
    if k >= len(population):
        return list(population)
    return rng.sample(population, k=k)

def generate_version_for_group(files, rng, version_num,
                               exclude_count,
                               within_min_s, within_max_s, within_max_pauses,
                               between_min_s, between_max_s):
    if not files:
        return None, [], [], {}, [], 0

    m = len(files)
    ex_count = max(0, min(exclude_count, max(0, m-1)))
    excluded = safe_sample(files, ex_count, rng) if ex_count > 0 else []
    included = [f for f in files if f not in excluded]
    if not included:
        included = files.copy()

    dup_files = []
    if included:
        dup_count = min(2, len(included))
        if dup_count > 0:
            if len(included) == 1:
                dup_files = [included[0]]
            else:
                dup_files = safe_sample(included, dup_count, rng)
    final_files = included + dup_files

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

        intra_evs, intra_details = insert_intra_pauses(zb_evs, rng, within_max_pauses, within_min_s, within_max_s)
        if intra_details:
            pause_info["intra_file_pauses"].append({"file": Path(fpath).name, "pauses": intra_details})

        shifted = apply_shifts(intra_evs, time_cursor)
        merged.extend(shifted)

        if shifted:
            file_max = max(int(e.get("Time", 0)) for e in shifted)
            file_min = min(int(e.get("Time", 0)) for e in shifted)
            play_times[str(fpath)] = file_max - file_min
            time_cursor = file_max + 30
        else:
            play_times[str(fpath)] = 0

        if idx < len(final_files) - 1:
            pause_s = rng.randint(between_min_s, between_max_s)
            pause_ms = pause_s * 1000
            time_cursor += pause_ms
            pause_info["inter_file_pauses"].append({"after_file": Path(fpath).name, "pause_ms": pause_ms, "after_index": idx})

    total_minutes = compute_total_minutes(merged)
    parts = []
    for f in final_files:
        dur_ms = play_times.get(str(f), 0)
        parts.append(f"{part_from_filename(str(Path(f).name))}[{round(dur_ms/60000)}m]")
    base_name = f"M_{total_minutes}m= " + " - ".join(parts)
    safe_name = ''.join(ch for ch in base_name if ch not in '/\\:*?"<>|')
    merged_fname = f"{safe_name}.json"
    return merged_fname, merged, [str(p) for p in final_files], pause_info, [str(p) for p in excluded], total_minutes

def build_arg_parser():
    p = argparse.ArgumentParser()
    p.add_argument("--input-dir", required=False, default="originals")
    p.add_argument("--output-dir", required=False, default="output")
    p.add_argument("--versions", type=int, default=16)
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--exclude-count", type=int, default=5)

    # New/current names:
    p.add_argument("--within-max-time", type=str, default="1m32s", help="max intra-file pause (string)")
    p.add_argument("--within-max-pauses", type=int, default=3, help="max pauses inside each file")
    p.add_argument("--between-max-time", type=str, default="2m37s", help="max between-file pause (string)")
    p.add_argument("--between-max-pauses", type=int, default=1, help="max pauses between files (kept for compatibility)")

    # Legacy/old aliases (map to same dest names)
    p.add_argument("--intra-file-max", type=str, dest="within_max_time", help=argparse.SUPPRESS)
    p.add_argument("--intra-file-max-pauses", type=int, dest="within_max_pauses", help=argparse.SUPPRESS)
    p.add_argument("--inter-file-max", type=str, dest="between_max_time", help=argparse.SUPPRESS)
    p.add_argument("--inter-file-max-pauses", type=int, dest="between_max_pauses", help=argparse.SUPPRESS)

    # Added alias requested: --inter-file-count -> between_max_pauses
    p.add_argument("--inter-file-count", type=int, dest="between_max_pauses", help=argparse.SUPPRESS)

    return p

def main():
    parser = build_arg_parser()
    args = parser.parse_args()

    rng = random.Random(args.seed) if args.seed is not None else random.Random()

    input_root = Path(args.input_dir)
    output_root = Path(args.output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    groups = find_group_dirs(input_root)
    if not groups:
        print(f"No group subfolders found under: {input_root}", file=sys.stderr)
        return

    try:
        within_max_s = parse_time_to_seconds(getattr(args, "within_max_time"))
    except Exception as e:
        print(f"ERROR parsing within max time: {e}", file=sys.stderr); return
    try:
        between_max_s = parse_time_to_seconds(getattr(args, "between_max_time"))
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
                within_min_s, within_max_s, getattr(args, "within_max_pauses"),
                between_min_s, between_max_s
            )
            if not merged_fname:
                continue
            out_path = output_root / merged_fname
            try:
                out_path.write_text(json.dumps(merged_events, indent=2, ensure_ascii=False), encoding="utf-8")
                print(f"WROTE: {out_path}")
                all_zip_entries.append((grp.name, out_path))
            except Exception as e:
                print(f"ERROR writing {out_path}: {e}", file=sys.stderr)

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
