#!/usr/bin/env python3
"""
merge_macros.py

Per-folder merger with elastic pause logic and correct filename timing including pauses.
"""

from pathlib import Path
import argparse
import json
import random
import re
import sys
from copy import deepcopy
from zipfile import ZipFile
import os
import math

# Hardcoded minimums (seconds)
DEFAULT_INTRA_MIN_SEC = 4     # 4 seconds
DEFAULT_INTER_MIN_SEC = 30    # 30 seconds

# Counter file (fallback if BUNDLE_SEQ not provided)
COUNTER_PATH = Path(".github/merge_bundle_counter.txt")

def parse_time_to_seconds(s: str) -> int:
    if s is None:
        raise ValueError("Empty time string")
    s = str(s).strip()
    if not s:
        raise ValueError("Empty time string")
    # mm:ss
    m = re.match(r'^(\d+):(\d{1,2})$', s)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    # m.ss
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

def read_counter_file():
    try:
        if COUNTER_PATH.exists():
            txt = COUNTER_PATH.read_text(encoding="utf-8").strip()
            return int(txt) if txt else 0
    except Exception:
        pass
    return 0

def write_counter_file(n: int):
    try:
        COUNTER_PATH.parent.mkdir(parents=True, exist_ok=True)
        COUNTER_PATH.write_text(str(n), encoding="utf-8")
    except Exception as e:
        print(f"WARNING: could not write counter file: {e}", file=sys.stderr)

def find_direct_parent_dirs_with_json(input_root: Path):
    if not input_root.exists() or not input_root.is_dir():
        return []
    parents = {p.parent.resolve() for p in input_root.rglob("*.json") if p.is_file()}
    return sorted(parents)

def find_json_files_in_dir(dirpath: Path):
    return sorted([p for p in dirpath.glob("*.json") if p.is_file()])

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
            except:
                t = 0
        times.append(t)
    min_t = min(times)
    shifted = []
    for e in events:
        ne = deepcopy(e)
        try:
            t = int(ne.get("Time", 0))
        except Exception:
            try:
                t = int(float(ne.get("Time", 0)))
            except:
                t = 0
        ne["Time"] = t - min_t
        shifted.append(ne)
    duration_ms = max(int(e.get("Time",0)) for e in shifted) if shifted else 0
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
    pauses_info = []
    for gap_idx in sorted(chosen):
        pause_s = rng.randint(min_s, max_s)
        pause_ms = pause_s * 1000
        for j in range(gap_idx+1, n):
            evs[j]["Time"] = int(evs[j].get("Time",0)) + pause_ms
        pauses_info.append({"after_event_index": gap_idx, "pause_ms": pause_ms})
    return evs, pauses_info

def apply_shifts(events, shift_ms):
    shifted = []
    for e in events:
        ne = deepcopy(e)           # preserve all keys
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

def compute_total_minutes_from_ms(total_ms: int):
    return math.ceil(total_ms / 60000) if total_ms > 0 else 0

def safe_sample(population, k, rng):
    if not population or k <= 0:
        return []
    if k >= len(population):
        return list(population)
    return rng.sample(population, k=k)

def generate_version_for_folder(files, rng, version_num,
                                exclude_count,
                                within_min_s, within_max_s, within_max_pauses,
                                between_min_s, between_max_s):
    """
    For each file in the folder, compute:
      - event duration (includes intra pauses)
      - inter pause after it (0 for last file)
    Per-file displayed minutes = ceil((event_duration + inter_pause_after)/60000)
    Total time is computed from merged events (which include intra+inter).
    """
    if not files:
        return None, [], [], {"inter_file_pauses":[], "intra_file_pauses":[]}, [], 0

    m = len(files)
    ex_count = max(0, min(exclude_count, max(0, m-1)))
    excluded = safe_sample(files, ex_count, rng) if ex_count > 0 else []
    included = [f for f in files if f not in excluded]
    if not included:
        included = files.copy()

    # Duplications / extras (keeps prior behavior)
    dup_files = []
    if included:
        dup_count = min(2, len(included))
        if dup_count > 0:
            dup_files = safe_sample(included, dup_count, rng) if len(included)>1 else [included[0]]
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
    per_file_event_ms = {}
    per_file_inter_ms = {}

    for idx, fpath in enumerate(final_files):
        evs = load_json_events(Path(fpath))
        zb_evs, duration_ms = zero_base_events(evs)

        # INTRA-file pauses (always enabled)
        intra_evs, intra_details = insert_intra_pauses(zb_evs, rng, within_max_pauses, within_min_s, within_max_s)
        if intra_details:
            pause_info["intra_file_pauses"].append({"file": Path(fpath).name, "pauses": intra_details})

        # Shift by cursor and append
        shifted = apply_shifts(intra_evs, time_cursor)
        merged.extend(shifted)

        # compute file event duration (including intra)
        if shifted:
            file_max = max(int(e.get("Time",0)) for e in shifted)
            file_min = min(int(e.get("Time",0)) for e in shifted)
            event_ms = file_max - file_min
            per_file_event_ms[str(fpath)] = event_ms
            # advance cursor to file end + tiny gap
            time_cursor = file_max + 30
        else:
            per_file_event_ms[str(fpath)] = 0

        # inter-file pause (after file), only if not last
        if idx < len(final_files) - 1:
            pause_s = rng.randint(between_min_s, between_max_s)
            pause_ms = pause_s * 1000
            time_cursor += pause_ms
            per_file_inter_ms[str(fpath)] = pause_ms
            pause_info["inter_file_pauses"].append({"after_file": Path(fpath).name, "pause_ms": pause_ms, "after_index": idx})
        else:
            per_file_inter_ms[str(fpath)] = 0

    # total minutes from merged events (merged already contains intra and inter gaps)
    if merged:
        times = [int(e.get("Time",0)) for e in merged]
        total_ms = max(times) - min(times)
    else:
        total_ms = 0
    total_minutes = compute_total_minutes_from_ms(total_ms)

    # Build file name parts using per-file (event + following inter) durations
    parts = []
    for f in final_files:
        event_ms = per_file_event_ms.get(str(f), 0)
        inter_ms = per_file_inter_ms.get(str(f), 0)
        combined_ms = event_ms + inter_ms
        minutes = compute_total_minutes_from_ms(combined_ms)
        parts.append(f"{part_from_filename(Path(f).name)}[{minutes}m]")

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

    p.add_argument("--within-max-time", type=str, default="1m32s")
    p.add_argument("--within-max-pauses", type=int, default=3)
    p.add_argument("--between-max-time", type=str, default="2m37s")
    p.add_argument("--between-max-pauses", type=int, default=1)

    # legacy aliases
    p.add_argument("--intra-file-max", type=str, dest="within_max_time", help=argparse.SUPPRESS)
    p.add_argument("--intra-file-max-pauses", type=int, dest="within_max_pauses", help=argparse.SUPPRESS)
    p.add_argument("--inter-file-max", type=str, dest="between_max_time", help=argparse.SUPPRESS)
    p.add_argument("--inter-file-max-pauses", type=int, dest="between_max_pauses", help=argparse.SUPPRESS)
    p.add_argument("--inter-file-count", type=int, dest="between_max_pauses", help=argparse.SUPPRESS)

    return p

def main():
    parser = build_arg_parser()
    args = parser.parse_args()

    rng = random.Random(args.seed) if args.seed is not None else random.Random()

    input_root = Path(args.input_dir)
    output_parent = Path(args.output_dir)
    output_parent.mkdir(parents=True, exist_ok=True)

    # Prefer BUNDLE_SEQ from env (workflow writes it to GITHUB_ENV). Fallback to counter file.
    bundle_seq_env = os.environ.get("BUNDLE_SEQ", "").strip()
    if bundle_seq_env:
        try:
            counter = int(bundle_seq_env)
        except:
            counter = read_counter_file() or 1
    else:
        # fallback: increment local counter file (so manual runs still get numbered)
        prev = read_counter_file()
        counter = prev + 1 if prev >= 0 else 1
        write_counter_file(counter)

    output_base_name = f"merged files_v{counter}"
    output_root = output_parent / output_base_name
    output_root.mkdir(parents=True, exist_ok=True)

    folder_dirs = find_direct_parent_dirs_with_json(input_root)
    if not folder_dirs:
        print(f"No json files found under {input_root}", file=sys.stderr)
        return

    # parse UI times and apply elastic minima
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

    for folder in folder_dirs:
        files = find_json_files_in_dir(folder)
        if not files:
            continue

        try:
            rel_folder = folder.relative_to(input_root)
        except Exception:
            rel_folder = Path(folder.name)

        out_folder_for_group = output_root / rel_folder
        out_folder_for_group.mkdir(parents=True, exist_ok=True)

        for v in range(1, max(1, args.versions) + 1):
            merged_fname, merged_events, finals, pauses, excluded, total_minutes = generate_version_for_folder(
                files, rng, v,
                args.exclude_count,
                within_min_s, within_max_s, getattr(args, "within_max_pauses"),
                between_min_s, between_max_s
            )
            if not merged_fname:
                continue
            out_path = out_folder_for_group / merged_fname
            try:
                out_path.write_text(json.dumps(merged_events, indent=2, ensure_ascii=False), encoding="utf-8")
                print(f"WROTE: {out_path}")
                all_zip_entries.append(out_path)
            except Exception as e:
                print(f"ERROR writing {out_path}: {e}", file=sys.stderr)

    # create zip (named under output_parent)
    zip_path = output_parent / f"{output_base_name}.zip"
    with ZipFile(zip_path, "w") as zf:
        for fpath in all_zip_entries:
            try:
                arcname = str(fpath.relative_to(output_parent))
            except Exception:
                arcname = f"{output_base_name}/{fpath.name}"
            zf.write(fpath, arcname=arcname)

    print("DONE. Created zip:", zip_path)

if __name__ == "__main__":
    main()
