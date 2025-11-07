#!/usr/bin/env python3
"""
merge_macros.py

Behavior summary (key points):
- Recursively finds .json files inside each group (direct children of --input-dir).
- New CLI inputs expect max times in **seconds**:
    --within-max-time-secs  (default 33)  -> pauses between files (your requested naming)
    --between-max-time-secs (default 18)  -> pauses inside files between actions
- Pause ranges are always 0 .. max_seconds (inclusive).
- No repeat of pause positions or pause durations until exhaustion, then repeats allowed.
- Appended file for MOBILE (default "close reopen mobile screensharelink.json") is exempt from intra pauses and from pause after it.
- Writes merged files to --output-dir/<group>/ and creates a zip named merged_bundle_<N>.zip in --output-dir.
- Maintains persistent counter at .github/merge_bundle_counter.txt (workflow may commit it back).
"""

from pathlib import Path
import argparse
import json
import random
from copy import deepcopy
from zipfile import ZipFile
import sys
import re

COUNTER_PATH = Path(".github/merge_bundle_counter.txt")
DEFAULT_APPEND_FILENAME = "close reopen mobile screensharelink.json"

# ---------------- utilities ----------------
def seconds_to_ms(s: int) -> int:
    return int(s) * 1000

def minutes_from_ms(ms: int) -> int:
    return round(ms / 60000)

def read_counter() -> int:
    try:
        if COUNTER_PATH.exists():
            txt = COUNTER_PATH.read_text(encoding="utf-8").strip()
            if txt:
                return int(txt)
    except Exception:
        pass
    return 0

def write_counter(n: int):
    try:
        COUNTER_PATH.parent.mkdir(parents=True, exist_ok=True)
        COUNTER_PATH.write_text(str(n), encoding="utf-8")
    except Exception as e:
        print(f"WARNING: cannot write counter file: {e}", file=sys.stderr)

# ---------------- json helpers ----------------
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
        return [data]
    if isinstance(data, list):
        return data
    return []

# ---------------- fs helpers ----------------
def find_groups(input_dir: Path):
    if not input_dir.exists():
        return []
    return [p for p in sorted(input_dir.iterdir()) if p.is_dir()]

def find_json_files_recursive(group_path: Path, output_dir: Path = None):
    files = []
    for p in sorted(group_path.rglob("*.json")):
        # skip files inside output_dir to avoid re-processing outputs
        if output_dir is not None:
            try:
                if output_dir.resolve() in p.resolve().parents:
                    continue
            except Exception:
                pass
        files.append(str(p.resolve()))
    return files

# ---------------- time & pauses ----------------
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

def insert_intra_pauses_no_repeat(events, rng, max_pauses, max_secs, intra_log):
    """
    Insert up to max_pauses pauses between events.
    Pause durations are chosen from 0..max_secs seconds.
    Positions (gaps between event indices) are chosen without repeat.
    Durations are chosen without repeat where possible; if exhausted, repeats allowed.
    """
    if not events or max_pauses <= 0 or max_secs < 0:
        return deepcopy(events)

    evs = deepcopy(events)
    n = len(evs)
    if n < 2:
        return evs

    # number of possible gaps:
    max_gaps = n - 1
    k = rng.randint(0, min(max_pauses, max_gaps))
    if k == 0:
        return evs

    # choose gap indices without repeat
    gap_indices = list(range(0, max_gaps))
    rng.shuffle(gap_indices)
    chosen_gaps = sorted(gap_indices[:k])

    # durations pool: seconds from 0..max_secs inclusive
    durations_pool = list(range(0, max_secs + 1))
    rng.shuffle(durations_pool)
    durations_chosen = []
    # take first k unique durations if possible, else sample with replacement
    if len(durations_pool) >= k:
        durations_chosen = durations_pool[:k]
    else:
        # not enough unique durations -> pick all unique then sample more with replacement
        durations_chosen = durations_pool[:]
        while len(durations_chosen) < k:
            durations_chosen.append(rng.choice(durations_pool) if durations_pool else 0)

    # now apply pauses in order of chosen gaps
    for gap_idx, dur_secs in zip(chosen_gaps, durations_chosen):
        pause_ms = dur_secs * 1000
        for j in range(gap_idx + 1, n):
            evs[j]["Time"] = int(evs[j].get("Time", 0)) + pause_ms
        intra_log.append({"after_event_index": gap_idx, "pause_ms": pause_ms})

    return evs

# ---------------- filename helpers ----------------
def part_from_filename(fname: str):
    stem = Path(fname).stem
    return ''.join(ch for ch in stem if ch.isalnum())

# ---------------- generate version ----------------
def generate_version(group_name: str, files, seed, global_pause_set, version_num, exclude_count,
                     intra_enabled, intra_max_pauses, within_max_secs,
                     between_max_secs, append_file_path: Path):
    rng = random.Random(seed)
    if not files:
        return None, [], [], {}, [], 0

    m = len(files)
    exclude_count = max(0, min(exclude_count, max(0, m-1)))
    excluded = rng.sample(files, k=exclude_count) if exclude_count and m>0 else []
    included = [f for f in files if f not in excluded]

    # duplicates (up to 2)
    dup_candidates = included or files
    dup_count = min(2, len(dup_candidates))
    dup_files = rng.sample(dup_candidates, k=dup_count) if dup_candidates else []

    final_files = included + dup_files

    # insert extra copies (1 or 2)
    if included:
        extra_k = rng.choice([1,2]) if len(included) >= 1 else 0
        extra_k = min(extra_k, len(included))
        extra_files = rng.sample(included, k=extra_k) if extra_k>0 else []
        for ef in extra_files:
            pos = rng.randrange(len(final_files)+1)
            if pos > 0 and final_files[pos-1] == ef:
                pos = min(len(final_files), pos+1)
            final_files.insert(min(pos, len(final_files)), ef)

    rng.shuffle(final_files)

    # For MOBILE group, insert the append_file near middle and at end (if exists)
    appended_abs = str(append_file_path.resolve()) if append_file_path and append_file_path.exists() else None
    if group_name.lower() == "mobile" and appended_abs:
        mid_pos = len(final_files)//2
        final_files.insert(mid_pos, appended_abs)
        final_files.append(appended_abs)

    merged = []
    pause_log = {"inter_file_pauses":[], "intra_file_pauses":[]}
    time_cursor = 0
    play_times = {}  # per-file total ms including pause after it

    # prepare inter-file durations pool (boundaries between files)
    # We'll choose durations per boundary ensuring uniqueness until exhaustion
    num_boundaries = max(0, len(final_files) - 1)
    inter_durations_pool = list(range(0, between_max_secs + 1))
    rng.shuffle(inter_durations_pool)
    # If not enough unique durations to cover boundaries, we'll re-use randomly where necessary
    inter_durations_iter = []
    if len(inter_durations_pool) >= num_boundaries:
        inter_durations_iter = inter_durations_pool[:num_boundaries]
    else:
        inter_durations_iter = inter_durations_pool[:]
        while len(inter_durations_iter) < num_boundaries:
            inter_durations_iter.append(rng.choice(inter_durations_pool) if inter_durations_pool else 0)

    # iterate through files and build merged events
    for idx, f in enumerate(final_files):
        evs_raw = load_json(Path(f))
        evs = normalize_json(evs_raw) if evs_raw is not None else []
        intra_log_local = []

        is_appended = (appended_abs is not None and str(Path(f).resolve()) == appended_abs)

        # insert intra pauses inside file unless appended file (appended exempt)
        if intra_enabled and evs and (not is_appended):
            evs = insert_intra_pauses_no_repeat(evs, rng, intra_max_pauses, within_max_secs, intra_log_local)
        if intra_log_local:
            pause_log["intra_file_pauses"].append({"file": Path(f).name, "pauses": intra_log_local})

        # apply shifts for this file
        shifted = apply_shifts(evs, time_cursor)
        merged.extend(shifted)

        if shifted:
            file_max_t = max(int(e.get("Time", 0)) for e in shifted)
            file_min_t = min(int(e.get("Time", 0)) for e in shifted)
            event_duration_ms = file_max_t - file_min_t
            last_time = file_max_t
        else:
            event_duration_ms = 0
            last_time = time_cursor

        # decide inter pause AFTER current file (unless last)
        inter_ms = 0
        is_last = (idx == len(final_files)-1)
        if not is_last:
            # appended file is exempt from having a pause after it
            if is_appended:
                inter_ms = 0
            else:
                # pick next duration from inter_durations_iter
                dur_secs = inter_durations_iter[idx] if idx < len(inter_durations_iter) else rng.choice(inter_durations_pool) if inter_durations_pool else 0
                inter_ms = int(dur_secs) * 1000
                pause_log["inter_file_pauses"].append({"after_file": Path(f).name, "pause_ms": inter_ms, "is_before_index": idx+1})

        # store per-file play time = events + pause after
        play_times[f] = event_duration_ms + inter_ms

        # advance cursor to end of events + pause
        time_cursor = last_time + inter_ms
        # tiny gap
        time_cursor += 30

    # Build filename parts (per-file play time in minutes rounded)
    parts = [part_from_filename(Path(f).name) + f"[{minutes_from_ms(play_times.get(f,0))}m] " for f in final_files]

    total_minutes = 0
    if merged:
        times = [int(e.get("Time",0)) for e in merged]
        total_ms = max(times) - min(times)
        total_minutes = minutes_from_ms(total_ms)

    merged_fname = "".join(parts).rstrip() + f"_v{version_num}[{total_minutes}m].json"
    return merged_fname, merged, final_files, pause_log, excluded, total_minutes

# ---------------- CLI & main ----------------
def build_arg_parser():
    p = argparse.ArgumentParser(description="Merge macro JSONs per-group (recursively).")
    p.add_argument("--input-dir", required=True, help="Parent directory containing group subfolders")
    p.add_argument("--output-dir", required=True, help="Directory to write merged files and ZIP")
    p.add_argument("--versions", type=int, default=5, help="Number of versions per group")
    p.add_argument("--seed", type=int, default=None, help="Base random seed (optional)")
    p.add_argument("--force", action="store_true", help="Force processing groups even if previously processed")
    p.add_argument("--exclude-count", type=int, default=1, help="How many files to randomly exclude per version")
    p.add_argument("--intra-file-enabled", action="store_true", help="Enable intra-file random pauses")
    p.add_argument("--intra-file-max", type=int, default=4, help="Max intra-file pauses per file")
    # NEW: times entered in seconds (integers)
    p.add_argument("--within-max-time-secs", type=int, default=33, help="Max pause inside-file in seconds (0..N)")
    p.add_argument("--within-max-pauses", type=int, default=3, help="Max number of pauses inside each file")
    p.add_argument("--between-max-time-secs", type=int, default=18, help="Max pause between files in seconds (0..N)")
    p.add_argument("--between-max-pauses", type=int, default=1, help="(compat) Max pauses between files (ignored)")
    p.add_argument("--append-file", type=str, default=DEFAULT_APPEND_FILENAME,
                   help="Path to file to append into MOBILE merges (default looks in repo root)")
    return p

def main():
    args = build_arg_parser().parse_args()

    in_dir = Path(args.input_dir)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    within_max_secs = max(0, int(args.within_max_time_secs))
    within_max_pauses = max(0, args.within_max_pauses)

    between_max_secs = max(0, int(args.between_max_time_secs))

    base_seed = args.seed if args.seed is not None else random.randrange(2**31)

    # read previous processed set from any existing zip in out_dir (best-effort)
    prev_processed = set()
    try:
        prev_zip = out_dir / "merged_bundle.zip"
        if prev_zip.exists():
            with ZipFile(prev_zip, "r") as zf:
                for name in zf.namelist():
                    if name.endswith(".json"):
                        prev_processed.add(Path(name).name)
    except Exception:
        prev_processed = set()

    groups = find_groups(in_dir)
    if not groups:
        print(f"No group directories found in {in_dir}", file=sys.stderr)
        return

    # locate append file (try provided path or repo root)
    append_file_path = Path(args.append_file)
    if not append_file_path.exists():
        candidate = Path.cwd() / args.append_file
        if candidate.exists():
            append_file_path = candidate
        else:
            append_file_path = None

    global_pauses = set()
    zip_items = []

    for gi, grp in enumerate(groups):
        files = find_json_files_recursive(grp, output_dir=out_dir)
        if not files:
            print(f"Skipping group '{grp.name}' — no JSON files found inside (recursively).")
            continue

        if not args.force and all(Path(f).name in prev_processed for f in files):
            print(f"Skipping group '{grp.name}' — files already processed and --force not set.")
            continue

        group_out_dir = out_dir / grp.name
        group_out_dir.mkdir(parents=True, exist_ok=True)

        for v in range(1, args.versions + 1):
            fname, merged, finals, pauses, excl, total = generate_version(
                grp.name, files, base_seed + gi*1000 + v, global_pauses, v,
                args.exclude_count, True, args.intra_file_max, within_max_secs,
                between_max_secs, append_file_path
            )
            if not fname:
                continue
            out_file_path = group_out_dir / fname
            try:
                with open(out_file_path, "w", encoding="utf-8") as fh:
                    json.dump(merged, fh, indent=2, ensure_ascii=False)
                zip_items.append((grp.name, out_file_path))
                print(f"Wrote {out_file_path} (total_minutes={total})")
            except Exception as e:
                print(f"ERROR writing {out_file_path}: {e}", file=sys.stderr)

    # increment counter and write zip
    counter = read_counter() + 1
    write_counter(counter)
    bundle_name = f"merged_bundle_{counter}"
    zip_path = out_dir / f"{bundle_name}.zip"
    try:
        with ZipFile(zip_path, "w") as zf:
            for group_name, file_path in zip_items:
                arcname = f"{bundle_name}/{group_name}/{file_path.name}"
                zf.write(file_path, arcname=arcname)
        print(f"Created ZIP: {zip_path.resolve()}")
        # print any found zip(s)
        for p in sorted(Path(out_dir).rglob("merged_bundle*.zip")):
            print("DEBUG: Found zip:", p.resolve())
    except Exception as e:
        print(f"ERROR creating ZIP {zip_path}: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
