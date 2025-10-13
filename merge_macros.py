#!/usr/bin/env python3
"""
merge_macros.py

Finalized script implementing:
 - Unified after-file pause rule (per-file random min 30..57s, UI-configurable max).
 - Intra-file pauses (4s..within-max), configurable count.
 - Appended MOBILE-only file: prefers group folder then repo root; appended as-is (no pauses).
 - Preserves originals/ subtree inside ZIP.
 - Deterministic via --seed.
 - UI-compatible with compact GitHub Actions inputs (no post-buffer UI fields).
"""
from pathlib import Path
import argparse
import json
import glob
import random
from zipfile import ZipFile
import sys
import re
import math
import os

# Defaults
DEFAULT_INPUT_DIR = "originals"
DEFAULT_OUTPUT_DIR = "output"
DEFAULT_SEED = 12345
DEFAULT_EXCLUDE_MAX = 3
DEFAULT_WITHIN_MAX = "2m47s"    # intra pause default max
DEFAULT_BETWEEN_MAX = "4m53s"   # after-file unified pause default max
ATTACHED_FILENAME = "close reopen mobile screensharelink.json"  # filename to append for MOBILE groups

_time_re_dot = re.compile(r'^(\\d+)\\.(\\d{1,2})$')
_time_re_colon = re.compile(r'^(\\d+):(\\d{1,2})$')
_time_re_ms = re.compile(r'^(?:(\\d+)m)?\\s*(?:(\\d+)s)?$')

def parse_time_str_to_seconds(s: str) -> int:
    if s is None:
        raise ValueError("time string is None")
    s0 = str(s).strip().lower()
    if not s0:
        raise ValueError("empty time string")
    m = _time_re_dot.match(s0)
    if m:
        mins = int(m.group(1)); secs = int(m.group(2))
        if secs >= 60:
            raise ValueError(f"seconds part must be <60 in '{s}'")
        return mins * 60 + secs
    m = _time_re_colon.match(s0)
    if m:
        mins = int(m.group(1)); secs = int(m.group(2))
        if secs >= 60:
            raise ValueError(f"seconds part must be <60 in '{s}'")
        return mins * 60 + secs
    m = _time_re_ms.match(s0)
    if m and (m.group(1) or m.group(2)):
        mm = int(m.group(1)) if m.group(1) else 0
        ss = int(m.group(2)) if m.group(2) else 0
        return mm * 60 + ss
    if re.match(r'^\\d+(\\.\\d+)?s?$', s0):
        s2 = s0[:-1] if s0.endswith('s') else s0
        val = float(s2)
        return int(round(val))
    if re.match(r'^\\d+$', s0):
        return int(s0)
    raise ValueError(f"Could not parse time value '{s}'")

def index_to_letters(idx: int) -> str:
    if idx < 1:
        return "A"
    s = ""
    n = idx
    while n > 0:
        n -= 1
        s = chr(ord('A') + (n % 26)) + s
        n //= 26
    return s

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

def part_from_filename(fname: str) -> str:
    stem = Path(fname).stem
    return ''.join(ch for ch in stem if ch.isalnum())

def find_groups_recursive(input_dir: Path):
    groups = []
    for root, dirs, files in os.walk(input_dir):
        jsons = [f for f in files if f.lower().endswith(".json")]
        if jsons:
            groups.append(Path(root))
    groups = sorted(groups)
    return groups

def find_json_files(group_path: Path):
    return sorted(glob.glob(str(group_path / "*.json")))

def apply_shifts(events, shift_ms: int):
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

def generate_version(files, rng, seed_for_intra, version_num, args, group_path: Path):
    excluded = []
    if not files:
        return None, [], [], {}, [], 0

    # Random exclusion between 0..exclude_max (clamped)
    if len(files) <= 1:
        included = list(files)
        excluded = []
    else:
        max_excl = min(args.exclude_max, len(files)-1)
        excl_count = rng.randint(0, max_excl) if max_excl >= 0 else 0
        excluded = rng.sample(files, excl_count) if excl_count > 0 else []
        included = [f for f in files if f not in excluded]

    if not included and files:
        included = [files[0]]
        excluded = [f for f in files if f != files[0]]

    population = included or files
    dup_count = min(2, len(population))
    dup_files = rng.sample(population, dup_count) if dup_count > 0 else []
    final_files = included + dup_files

    if included:
        k_choice = rng.choice([1,2])
        k = min(k_choice, len(included))
        if k > 0:
            extra_files = rng.sample(included, k=k)
            for ef in extra_files:
                n = len(final_files)
                pos = None
                for attempt in range(n+2):
                    p = rng.randrange(0, n+1)
                    left_ok = (p-1 < 0) or (final_files[p-1] != ef)
                    right_ok = (p >= n) or (final_files[p] != ef)
                    if left_ok and right_ok:
                        pos = p
                        break
                if pos is None:
                    pos = min(rng.randrange(0, n+1), n)
                final_files.insert(min(pos, len(final_files)), ef)

    rng.shuffle(final_files)

    merged = []
    time_cursor = 0
    play_times = {}

    intra_sum = {}
    post_pause = {}

    within_max_ms = max(4000, args.within_max_secs * 1000)  # enforce at least 4s
    between_max_ms_config = max(1, args.between_max_secs) * 1000  # UI max in ms

    for idx, f in enumerate(final_files):
        evs_raw = load_json(Path(f)) or []
        evs = normalize_json(evs_raw)

        intra_sum[f] = 0
        post_pause[f] = 0

        # INTRA-FILE pauses for ALL files (including first)
        if evs and len(evs) > 1:
            n_gaps = len(evs) - 1
            intra_rng = random.Random((hash(f) & 0xffffffff) ^ (seed_for_intra + version_num))
            chosen_count = intra_rng.randint(1, min(args.within_max_pauses, n_gaps))
            if chosen_count > 0:
                chosen_gaps = intra_rng.sample(range(n_gaps), chosen_count)
                for gap_idx in sorted(chosen_gaps):
                    pause_ms = intra_rng.randint(4000, within_max_ms)
                    for j in range(gap_idx+1, len(evs)):
                        evs[j]["Time"] = int(evs[j].get("Time", 0)) + pause_ms
                    intra_sum[f] += pause_ms

        # append file events shifted by current time_cursor
        shifted = apply_shifts(evs, time_cursor)
        merged.extend(shifted)
        if shifted:
            max_t = max(int(e.get("Time", 0)) for e in shifted)
            min_t = min(int(e.get("Time", 0)) for e in shifted)
            duration_ms = max_t - min_t
            play_times[f] = duration_ms
        else:
            play_times[f] = 0
            max_t = time_cursor

        # AFTER-FILE unified pause: apply after every file (will not be added to appended MOBILE file later)
        min_rand_secs = rng.randint(30, 57)
        min_rand_ms = min_rand_secs * 1000
        effective_between_max_ms = between_max_ms_config if between_max_ms_config >= min_rand_ms else min_rand_ms
        pause_ms = rng.randint(min_rand_ms, effective_between_max_ms)
        post_pause[f] = pause_ms
        time_cursor = max_t + pause_ms

    # Append attached file for MOBILE groups only (prefer group folder then repo root)
    appended = None
    try:
        parts_lower = [p.lower() for p in group_path.parts]
    except Exception:
        parts_lower = []
    if "mobile" in parts_lower:
        cand_local = group_path / ATTACHED_FILENAME
        cand = cand_local if cand_local.exists() else Path(ATTACHED_FILENAME)
        if cand.exists():
            evs_raw = load_json(cand) or []
            evs = normalize_json(evs_raw)
            # append immediately at current time_cursor, no intra, no after pause
            shifted = apply_shifts(evs, time_cursor)
            merged.extend(shifted)
            if shifted:
                max_t = max(int(e.get("Time", 0)) for e in shifted)
                min_t = min(int(e.get("Time", 0)) for e in shifted)
                play_times[str(cand)] = max_t - min_t
                intra_sum[str(cand)] = 0
                post_pause[str(cand)] = 0
                time_cursor = max_t
                final_files.append(str(cand))
                appended = str(cand)

    # compute per-file effective times: duration + intra + post_pause (appended file has post_pause 0)
    per_file_effective_ms = {}
    for f in final_files:
        dur = play_times.get(f, 0)
        intra = intra_sum.get(f, 0)
        post = post_pause.get(f, 0)
        per_file_effective_ms[f] = dur + intra + post

    total_effective_ms = sum(per_file_effective_ms.values())
    total_minutes = math.ceil(total_effective_ms / 60000) if total_effective_ms > 0 else 0

    parts = []
    for f in final_files:
        minutes = math.ceil(per_file_effective_ms.get(f, 0) / 60000)
        parts.append(part_from_filename(f) + f"[+{minutes}m]")

    parts_joined = "- ".join(parts)
    letters = index_to_letters(version_num)
    merged_fname = f"{letters}_{total_minutes}m= {parts_joined}.json"

    return merged_fname, merged, final_files, {}, excluded, total_minutes

def parse_args():
    p = argparse.ArgumentParser(description="Merge macros - recursive group discovery under 'originals'.")
    p.add_argument("--versions", type=int, default=6, help="How many versions per group")
    p.add_argument("--force", action="store_true", help="Force reprocessing even if outputs exist")
    p.add_argument("--within-max-time", default=DEFAULT_WITHIN_MAX, help="Within-file max pause time (e.g. 2m47s)")
    p.add_argument("--within-max-pauses", type=int, default=5, help="Max number of intra-file pauses")
    p.add_argument("--between-max-time", default=DEFAULT_BETWEEN_MAX, help="After-file unified max pause time (e.g. 4m53s); per-file min is random 30..57s")
    p.add_argument("--between-max-pauses", type=int, default=1, help="(ignored) for UI compatibility")
    p.add_argument("--exclude-max", type=int, default=DEFAULT_EXCLUDE_MAX, help="Maximum number of files to randomly exclude per version (0..N-1)")
    p.add_argument("--input-dir", default=DEFAULT_INPUT_DIR, help="Top-level folder containing original groups (default: originals)")
    p.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Where to write outputs (default: ./output)")
    p.add_argument("--seed", type=int, default=DEFAULT_SEED, help=argparse.SUPPRESS)
    return p.parse_args()

def main():
    args = parse_args()
    try:
        args.within_max_secs = parse_time_str_to_seconds(args.within_max_time)
        args.between_max_secs = parse_time_str_to_seconds(args.between_max_time)
    except Exception as e:
        print("ERROR parsing time inputs:", e, file=sys.stderr)
        sys.exit(2)

    if args.within_max_secs < 4:
        print("WARNING: --within-max-time too small; raising to 4s.")
        args.within_max_secs = 4

    if args.between_max_secs < 1:
        print("WARNING: --between-max-time too small; raising to 1s.")
        args.between_max_secs = 1

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not input_dir.exists():
        placeholder = output_dir / "merged_bundle.zip"
        with ZipFile(placeholder, "w") as zf:
            zf.writestr("placeholder.txt", "No originals folder found.")
        print("Wrote placeholder zip:", placeholder)
        sys.exit(0)

    groups = find_groups_recursive(input_dir)
    base_seed = args.seed
    merged_outputs = []

    for gi, grp in enumerate(groups):
        files = find_json_files(grp)
        if not files:
            continue
        for v in range(1, args.versions + 1):
            version_rng = random.Random(base_seed + gi*1000 + v)
            fname, merged, finals, _, excl, total = generate_version(
                files, version_rng, base_seed, v, args, grp
            )
            if not fname:
                continue
            out_file_path = output_dir / fname
            with open(out_file_path, "w", encoding="utf-8") as fh:
                json.dump(merged, fh, indent=2, ensure_ascii=False)
            merged_outputs.append((grp, out_file_path))

    bundle_path = output_dir / "merged_bundle.zip"
    bundle_seq = os.environ.get("BUNDLE_SEQ", "").strip()
    top_folder = f"merged_bundle_{bundle_seq}" if bundle_seq else None

    if merged_outputs:
        with ZipFile(bundle_path, "w") as zf:
            for group_path, file_path in merged_outputs:
                if not file_path.exists() or file_path.suffix.lower() != ".json":
                    continue
                try:
                    rel_group = group_path.relative_to(input_dir)
                except Exception:
                    rel_group = Path(group_path.name)
                if top_folder:
                    arc = Path(top_folder) / rel_group / file_path.name
                else:
                    arc = rel_group / file_path.name
                zf.write(file_path, arcname=str(arc.as_posix()))
    else:
        with ZipFile(bundle_path, "w") as zf:
            zf.writestr("placeholder.txt", "No merged files were produced.")

    print("DONE. Outputs in:", str(output_dir.resolve()))
    print("Created:", bundle_path.name)

if __name__ == "__main__":
    main()
