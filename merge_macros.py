#!/usr/bin/env python3
"""
merge_macros.py — patched to accept within/between time CLI args.

Features:
 - Recursive JSON discovery per-group (fix for fused_bundle inside groups).
 - New CLI options accepted:
     --within-max-time (e.g. '1m32s' or '92s')
     --within-max-pauses (int)
     --between-max-time (e.g. '2m37s')
     --between-max-pauses (int)  # for UI compatibility (not heavily used)
 - Time parsing supports 'XmYs', 'Ns', 'M:S', 'Mm' and plain seconds.
 - Intra-file pauses now choose pause durations in ms between a min and the provided max.
 - Inter-file pauses use min_ms=30000 (30s) and max = parsed between-max-time (if lower than min, min becomes 0 to stay elastic).
"""

from pathlib import Path
import argparse
import json
import random
from copy import deepcopy
from zipfile import ZipFile
import sys
import re

# ---------- helpers ---------------------------------------------------------
def parse_time_to_ms(s: str) -> int:
    """Parse human-friendly time string to milliseconds.
    Accepts: '1m32s', '92s', '1:32', '1m', '45', '1.5m' (decimal minutes).
    Returns 0 on parse failure.
    """
    if not s:
        return 0
    s = str(s).strip().lower()
    # mm:ss style
    m = re.match(r'^(\d+):(\d+)$', s)
    if m:
        minutes = int(m.group(1))
        seconds = int(m.group(2))
        return (minutes * 60 + seconds) * 1000
    # XmYs or Xm or Ys
    total_seconds = 0.0
    # find minutes
    m = re.search(r'([0-9]+(?:\.[0-9]+)?)\s*m', s)
    if m:
        total_seconds += float(m.group(1)) * 60.0
    # find seconds
    m2 = re.search(r'([0-9]+(?:\.[0-9]+)?)\s*s', s)
    if m2:
        total_seconds += float(m2.group(1))
    # if string is only digits (maybe seconds)
    if total_seconds == 0.0:
        m3 = re.match(r'^[0-9]+(?:\.[0-9]+)?$', s)
        if m3:
            total_seconds = float(s)
    # fallback for '1.5m' without m or s: handled above by 'm'
    try:
        return int(total_seconds * 1000)
    except Exception:
        return 0

def minutes_from_ms(ms: int) -> int:
    return round(ms / 60000)

# ---------- json helpers ---------------------------------------------------
def load_json(path: Path):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as e:
        print(f"WARNING: cannot parse {path}: {e}", file=sys.stderr)
        return None

def normalize_json(data):
    if isinstance(data, dict) and "events" in data and isinstance(data["events"], list):
        return data["events"]
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in ("items","entries","records","actions","eventsList","events_array"):
            if k in data and isinstance(data[k], list):
                return data[k]
        if "Time" in data:
            return [data]
        return [data]
    return []

# ---------- filesystem helpers ---------------------------------------------
def find_groups(input_dir: Path):
    if not input_dir.exists():
        return []
    return [p for p in sorted(input_dir.iterdir()) if p.is_dir()]

def find_json_files(group_path: Path, output_dir: Path = None):
    files = []
    for p in sorted(group_path.rglob("*.json")):
        if output_dir is not None:
            try:
                # avoid including files that are inside the output directory (if user points output under input)
                if output_dir.resolve() in p.resolve().parents:
                    continue
            except Exception:
                pass
        files.append(str(p.resolve()))
    return files

# ---------- time & pause logic ---------------------------------------------
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
        new = dict(e)
        new["Time"] = t + int(shift_ms)
        shifted.append(new)
    return shifted

def insert_intra_pauses_ms(events, rng, max_pauses, min_ms, max_ms, intra_log):
    """Insert up to max_pauses pauses; pause durations chosen uniformly between min_ms and max_ms (inclusive).
       Pauses inserted at random gap positions (not at very end)."""
    if not events or max_pauses <= 0 or max_ms <= 0:
        return deepcopy(events)
    evs = deepcopy(events)
    n = len(evs)
    if n < 2:
        return evs
    k = rng.randint(0, min(max_pauses, n-1))
    if k == 0:
        return evs
    # choose distinct gap indices
    chosen = rng.sample(range(n-1), k)
    for gap_idx in sorted(chosen):
        # ensure min_ms <= max_ms, be elastic
        effective_min = max(0, min_ms)
        effective_max = max(effective_min, max_ms)
        pause_ms = rng.randint(effective_min, effective_max)
        for j in range(gap_idx+1, n):
            evs[j]["Time"] = int(evs[j].get("Time", 0)) + pause_ms
        intra_log.append({"after_event_index": gap_idx, "pause_ms": pause_ms})
    return evs

# ---------- filename helpers -----------------------------------------------
from pathlib import Path as _P
def part_from_filename(fname: str):
    stem = _P(fname).stem
    return ''.join(ch for ch in stem if ch.isalnum())

# ---------- processed detection --------------------------------------------
from zipfile import ZipFile
def get_previously_processed_files(zip_path: Path):
    processed = set()
    if zip_path.exists():
        try:
            with ZipFile(zip_path, "r") as zf:
                for name in zf.namelist():
                    if name.endswith(".json"):
                        processed.add(Path(name).name)
        except Exception:
            pass
    return processed

# ---------- generate version ------------------------------------------------
def generate_version(files, seed, global_pause_set, version_num, exclude_count,
                     intra_enabled, intra_max_pauses, within_min_ms, within_max_ms,
                     between_min_ms, between_max_ms):
    rng = random.Random(seed)
    if not files:
        return None, [], [], {}, [], 0

    m = len(files)
    exclude_count = max(0, min(exclude_count, max(0, m-1)))
    excluded = rng.sample(files, k=exclude_count) if exclude_count and m>0 else []
    included = [f for f in files if f not in excluded]

    # pick up to 2 duplicates
    dup_candidates = included or files
    dup_count = min(2, len(dup_candidates))
    dup_files = rng.sample(dup_candidates, k=dup_count) if dup_candidates else []

    final_files = included + dup_files

    # optional extra copies (1 or 2)
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

    merged = []
    pause_log = {"inter_file_pauses":[], "intra_file_pauses":[]}
    time_cursor = 0
    play_times = {}

    for idx, f in enumerate(final_files):
        evs_raw = load_json(Path(f))
        evs = normalize_json(evs_raw) if evs_raw is not None else []
        intra_log_local = []
        if intra_enabled and evs:
            # deterministic per-file intra RNG
            intra_rng = random.Random((hash(f) & 0xffffffff) ^ (seed + version_num))
            evs = insert_intra_pauses_ms(evs, intra_rng, intra_max_pauses, within_min_ms, within_max_ms, intra_log_local)
        if intra_log_local:
            pause_log["intra_file_pauses"].append({"file": Path(f).name, "pauses": intra_log_local})

        # Inter-file pause: apply AFTER the file (so before next file starts we will add)
        # But original code added pause before appending this file's events; keep the net behaviour consistent:
        # We'll follow: decide pause between this file and the next (except after last).
        shifted = apply_shifts(evs, time_cursor)
        merged.extend(shifted)

        if shifted:
            max_t = max(int(e.get("Time", 0)) for e in shifted)
            min_t = min(int(e.get("Time", 0)) for e in shifted)
            play_times[f] = max_t - min_t
            time_cursor = max_t  # cursor now at end of this file (ms)
        else:
            play_times[f] = 0

        if idx < len(final_files) - 1:
            # choose an inter pause duration between between_min_ms and between_max_ms (elastic if max < min)
            eff_min = max(0, min(between_min_ms, between_max_ms))
            eff_max = max(0, max(between_min_ms, between_max_ms))
            # if eff_min==0 and eff_max==0 fallback to default 120k..780k
            if eff_max == 0:
                inter_ms = rng.randint(120000, 780000) if idx > 0 else rng.randint(120000, 180000)
            else:
                inter_ms = rng.randint(eff_min, eff_max)
            # ensure uniqueness if required
            if inter_ms not in global_pause_set:
                global_pause_set.add(inter_ms)
            time_cursor += inter_ms
            pause_log["inter_file_pauses"].append({"after_file": Path(f).name, "pause_ms": inter_ms, "is_before_index": idx+1})
        # small gap
        time_cursor += 30

    parts = [part_from_filename(Path(f).name) + f"[{minutes_from_ms(play_times[f])}m] " for f in final_files]
    total_minutes = max(0, round(sum(play_times.values()) / 60000))
    merged_fname = "".join(parts).rstrip() + f"_v{version_num}[{total_minutes}m].json"
    return merged_fname, merged, final_files, pause_log, excluded, total_minutes

# ---------- arg parsing ----------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input-dir", required=True, help="Parent directory containing group subfolders")
    p.add_argument("--output-dir", required=True, help="Directory to write merged files and ZIP")
    p.add_argument("--versions", type=int, default=5, help="Number of versions per group")
    p.add_argument("--seed", type=int, default=None, help="Base random seed (optional)")
    p.add_argument("--force", action="store_true", help="Force processing groups even if previously processed")
    p.add_argument("--exclude-count", type=int, default=1, help="How many files to randomly exclude per version")
    # new UI arguments (accepted and parsed)
    p.add_argument("--within-max-time", type=str, default="", help="Max pause time inside file (e.g. '1m32s')")
    p.add_argument("--within-max-pauses", type=int, default=4, help="Max number of pauses inside each file")
    p.add_argument("--between-max-time", type=str, default="", help="Max pause time between files (e.g. '4m53s')")
    p.add_argument("--between-max-pauses", type=int, default=1, help="(compat) Max pauses between files")
    return p.parse_args()

# ---------- main -----------------------------------------------------------
def main():
    args = parse_args()
    in_dir = Path(args.input_dir)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # parse time args to milliseconds
    within_max_ms = parse_time_to_ms(args.within_max_time)
    # For intra pauses, pick a min default (4s) if within_max_ms >= 4000 else 0 (elastic)
    within_min_ms = 4000 if within_max_ms >= 4000 else 0
    within_max_pauses = max(0, args.within_max_pauses)

    between_max_ms = parse_time_to_ms(args.between_max_time)
    # default min for inter pauses = 30000 ms (30s). If user chose smaller max, make elastic and set min to 0
    between_min_ms = 30000 if between_max_ms >= 30000 else 0
    between_max_pauses = max(0, args.between_max_pauses)

    base_seed = args.seed if args.seed is not None else random.randrange(2**31)
    prev_processed = get_previously_processed_files(out_dir / "merged_bundle.zip")
    global_pauses = set()
    zip_items = []

    groups = find_groups(in_dir)
    if not groups:
        print(f"No group directories found in {in_dir}", file=sys.stderr)
        return

    for gi, grp in enumerate(groups):
        files = find_json_files(grp, output_dir=out_dir)
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
                files, base_seed + gi*1000 + v, global_pauses, v,
                args.exclude_count, True, within_max_pauses, within_min_ms, within_max_ms,
                between_min_ms, between_max_ms
            )

            if not fname:
                continue

            out_file_path = group_out_dir / fname
            try:
                with open(out_file_path, "w", encoding="utf-8") as fh:
                    json.dump(merged, fh, indent=2, ensure_ascii=False)
                zip_items.append((grp.name, out_file_path))
                print(f"Wrote merged version: {out_file_path} (total_minutes={total})")
            except Exception as e:
                print(f"ERROR writing {out_file_path}: {e}", file=sys.stderr)

    zip_path = out_dir / "merged_bundle.zip"
    try:
        with ZipFile(zip_path, "w") as zf:
            for group_name, file_path in zip_items:
                arcname = f"{group_name}/{file_path.name}"
                zf.write(file_path, arcname=arcname)
        print(f"Created ZIP: {zip_path}")
    except Exception as e:
        print(f"ERROR creating ZIP {zip_path}: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
