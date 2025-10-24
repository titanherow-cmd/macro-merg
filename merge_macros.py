#!/usr/bin/env python3
"""
merge_macros.py

Patched merge script (full) — copy/paste into repo root.

Key behaviors / fixes included here:
 - Recursively finds `.json` under each group folder (child directories of --input-dir).
 - For groups named "MOBILE" (case-insensitive), *appends* a special file
   (default: "close reopen mobile screensharelink.json" in repo root) **near the middle**
   of the merged sequence and **again at the end** of the merged sequence.
 - The appended file is always exempt from:
     * intra-file pauses (no random pauses inserted inside it),
     * having an inter-file pause AFTER it (the next file starts immediately after it).
 - Inter-file pauses and intra-file pauses otherwise apply as configured by CLI args.
 - Per-file durations used in filenames include that file's event duration *plus* the pause
   that follows it (if any). Total merged duration in filename reflects the full span of merged events.
 - Persistent bundle counter is written to `.github/merge_bundle_counter.txt` (workflow may commit it back).
 - Writes merged per-group outputs to `--output-dir/<group>/` and creates
   a ZIP named `merged_bundle_<N>.zip` with a top-level folder `merged_bundle_<N>/...`.

Usage (example):
  python3 merge_macros.py --input-dir originals --output-dir merged_output \
    --versions 16 --within-max-time '1m32s' --within-max-pauses 3 \
    --between-max-time '2m37s' --between-max-pauses 1 --exclude-count 5

"""

from pathlib import Path
import argparse
import json
import random
from copy import deepcopy
from zipfile import ZipFile
import sys
import re
import math

COUNTER_PATH = Path(".github/merge_bundle_counter.txt")
DEFAULT_APPEND_FILENAME = "close reopen mobile screensharelink.json"

# ---------- small helpers --------------------------------------------------
def parse_time_to_ms(s: str) -> int:
    """Parse time strings to milliseconds. Supports '1m32s', '92s', '1:32', '1m', '45', '1.5m'."""
    if not s:
        return 0
    s = str(s).strip().lower()
    # mm:ss
    m = re.match(r'^(\d+):(\d+)$', s)
    if m:
        return (int(m.group(1)) * 60 + int(m.group(2))) * 1000
    total_seconds = 0.0
    m_min = re.search(r'([0-9]+(?:\.[0-9]+)?)\s*m', s)
    if m_min:
        total_seconds += float(m_min.group(1)) * 60.0
    m_sec = re.search(r'([0-9]+(?:\.[0-9]+)?)\s*s', s)
    if m_sec:
        total_seconds += float(m_sec.group(1))
    if total_seconds == 0.0:
        m_digits = re.match(r'^[0-9]+(?:\.[0-9]+)?$', s)
        if m_digits:
            total_seconds = float(s)
    try:
        return int(total_seconds * 1000)
    except Exception:
        return 0

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

# ---------- JSON helpers --------------------------------------------------
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

# ---------- filesystem helpers ---------------------------------------------
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

# ---------- time/shift & pause insertion ----------------------------------
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

def insert_intra_pauses_ms(events, rng, max_pauses, min_ms, max_ms, intra_log):
    """Insert up to k pauses between events, updating times after each inserted gap.
       Pause durations uniformly sampled between effective min and max (elastic if max < min)."""
    if not events or max_pauses <= 0 or max_ms <= 0:
        return deepcopy(events)
    evs = deepcopy(events)
    n = len(evs)
    if n < 2:
        return evs
    k = rng.randint(0, min(max_pauses, n-1))
    if k == 0:
        return evs
    chosen = rng.sample(range(n-1), k)
    for gap_idx in sorted(chosen):
        eff_min = max(0, min_ms)
        eff_max = max(eff_min, max_ms)
        pause_ms = rng.randint(eff_min, eff_max)
        for j in range(gap_idx+1, n):
            evs[j]["Time"] = int(evs[j].get("Time", 0)) + pause_ms
        intra_log.append({"after_event_index": gap_idx, "pause_ms": pause_ms})
    return evs

# ---------- filename helper -----------------------------------------------
def part_from_filename(fname: str):
    stem = Path(fname).stem
    return ''.join(ch for ch in stem if ch.isalnum())

# ---------- version generation ---------------------------------------------
def generate_version(group_name: str, files, seed, global_pause_set, version_num, exclude_count,
                     intra_enabled, intra_max_pauses, within_min_ms, within_max_ms,
                     between_min_ms, between_max_ms, append_file_path: Path):
    """
    Compose a merged version for the given group.
    Returns (merged_filename, merged_events_list, final_files_order, pause_log, excluded_list, total_minutes)
    """
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

    # optionally insert extra copies (1 or 2) non-adjacently
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

    # For MOBILE group, insert the append_file near middle and also at end (if file exists)
    appended_abs = str(append_file_path.resolve()) if append_file_path and append_file_path.exists() else None
    if group_name.lower() == "mobile" and appended_abs:
        # avoid inserting if it's already present in list (we still want duplicates as explicit inserted items)
        # Insert near middle:
        mid_pos = len(final_files)//2
        final_files.insert(mid_pos, appended_abs)
        # Append at end
        final_files.append(appended_abs)

    merged = []
    pause_log = {"inter_file_pauses":[], "intra_file_pauses":[]}
    time_cursor = 0
    play_times = {}  # per-file total ms including pause after it

    for idx, f in enumerate(final_files):
        evs_raw = load_json(Path(f))
        evs = normalize_json(evs_raw) if evs_raw is not None else []
        intra_log_local = []

        is_appended = (appended_abs is not None and str(Path(f).resolve()) == appended_abs)

        # insert intra pauses UNLESS this is the appended file (appended file exempt from intra pauses)
        if intra_enabled and evs and (not is_appended):
            intra_rng = random.Random((hash(f) & 0xffffffff) ^ (seed + version_num))
            evs = insert_intra_pauses_ms(evs, intra_rng, intra_max_pauses, within_min_ms, within_max_ms, intra_log_local)
        if intra_log_local:
            pause_log["intra_file_pauses"].append({"file": Path(f).name, "pauses": intra_log_local})

        # shift events by current cursor
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

        # decide inter-file pause AFTER current file
        inter_ms = 0
        is_last = (idx == len(final_files)-1)
        if not is_last:
            # appended file is exempt from having a pause after it
            if is_appended:
                inter_ms = 0
            else:
                # choose inter pause uniformly between effective min and max (elastic)
                eff_min = max(0, min(between_min_ms, between_max_ms))
                eff_max = max(0, max(between_min_ms, between_max_ms))
                if eff_max == 0:
                    # fallback range (as historical): first file shorter, others longer
                    inter_ms = rng.randint(120000, 180000) if idx==0 else rng.randint(120000, 780000)
                else:
                    inter_ms = rng.randint(eff_min, eff_max)
                # optional uniqueness guard
                if inter_ms not in global_pause_set:
                    global_pause_set.add(inter_ms)
                pause_log["inter_file_pauses"].append({"after_file": Path(f).name, "pause_ms": inter_ms, "is_before_index": idx+1})

        # save per-file total play time (duration + pause after)
        play_times[f] = event_duration_ms + inter_ms

        # advance cursor: end of events plus the chosen pause (the pause effectively shifts next events)
        time_cursor = last_time + inter_ms
        # small gap after cursor (to avoid overlapping events)
        time_cursor += 30

    # build filename parts with minutes including pause after each file
    parts = [part_from_filename(Path(f).name) + f"[{minutes_from_ms(play_times.get(f,0))}m] " for f in final_files]
    total_minutes = 0
    if merged:
        times = [int(e.get("Time",0)) for e in merged]
        total_ms = max(times) - min(times)
        # Note: inter-file pauses move subsequent events forward, so total_ms includes inter pauses between events.
        total_minutes = minutes_from_ms(total_ms)
    merged_fname = "".join(parts).rstrip() + f"_v{version_num}[{total_minutes}m].json"
    return merged_fname, merged, final_files, pause_log, excluded, total_minutes

# ---------- CLI parsing & main ---------------------------------------------
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
    p.add_argument("--within-max-time", type=str, default="", help="Max pause time inside file (e.g. '1m32s')")
    p.add_argument("--within-max-pauses", type=int, default=4, help="Max pauses inside each file")
    p.add_argument("--between-max-time", type=str, default="", help="Max pause time between files (e.g. '4m53s')")
    p.add_argument("--between-max-pauses", type=int, default=1, help="(compat) Max pauses between files")
    p.add_argument("--append-file", type=str, default=DEFAULT_APPEND_FILENAME,
                   help="Path to file to append into MOBILE merges (default: at repo root)")
    return p

def main():
    args = build_arg_parser().parse_args()

    in_dir = Path(args.input_dir)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # parse time inputs
    within_max_ms = parse_time_to_ms(args.within_max_time)
    # default intra-file minimum: 4s unless user max below 4s then elastic from 0
    within_min_ms = 4000 if within_max_ms >= 4000 else 0
    within_max_pauses = max(0, args.within_max_pauses)

    between_max_ms = parse_time_to_ms(args.between_max_time)
    # default inter-file minimum: 30s unless user max < 30s then elastic from 0
    between_min_ms = 30000 if between_max_ms >= 30000 else 0

    base_seed = args.seed if args.seed is not None else random.randrange(2**31)
    prev_processed = set()
    # reading previous zip if exists - deprecated but harmless
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

    # find append file
    append_file_path = Path(args.append_file)
    if not append_file_path.exists():
        # try repo root
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

        group_out = out_dir / grp.name
        group_out.mkdir(parents=True, exist_ok=True)

        for v in range(1, args.versions + 1):
            fname, merged, finals, pauses, excl, total = generate_version(
                grp.name, files, base_seed + gi*1000 + v, global_pauses, v,
                args.exclude_count, True, args.intra_file_max, within_min_ms, within_max_ms,
                between_min_ms, between_max_ms, append_file_path
            )
            if not fname:
                continue
            out_file_path = group_out / fname
            try:
                with open(out_file_path, "w", encoding="utf-8") as fh:
                    json.dump(merged, fh, indent=2, ensure_ascii=False)
                zip_items.append((grp.name, out_file_path))
                print(f"Wrote {out_file_path} (total_minutes={total})")
            except Exception as e:
                print(f"ERROR writing {out_file_path}: {e}", file=sys.stderr)

    # increment persistent counter and write ZIP named merged_bundle_{N}.zip
    counter = read_counter() + 1
    write_counter(counter)
    bundle_name = f"merged_bundle_{counter}"
    zip_path = out_dir / f"{bundle_name}.zip"
    try:
        with ZipFile(zip_path, "w") as zf:
            for group_name, file_path in zip_items:
                arcname = f"{bundle_name}/{group_name}/{file_path.name}"
                zf.write(file_path, arcname=arcname)
        print(f"Created ZIP: {zip_path}")
    except Exception as e:
        print(f"ERROR creating ZIP {zip_path}: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
