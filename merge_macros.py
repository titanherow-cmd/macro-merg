#!/usr/bin/env python3

"""
merge_macros.py (Updated Pauses)
- Minimum pause times set to 0 seconds (both intra and inter)
- UI defaults: 18s for inter-file, 33s for intra-file
- Pause selection uses millisecond precision
- UI displays times in seconds
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

# --------- constants ---------
COUNTER_PATH = Path(".github/merge_bundle_counter.txt")
SPECIAL_FILENAME = "close reopen mobile screensharelink.json"
SPECIAL_KEYWORD = "screensharelink"

# --------- helpers ---------
def parse_time_to_seconds(s: str) -> int:
    if s is None:
        raise ValueError("Empty time string")
    s = str(s).strip()
    if not s:
        raise ValueError("Empty time string")
    
    # plain integer seconds
    if re.match(r'^\d+$', s):
        return int(s)
    
    # mm:ss
    m = re.match(r'^(\d+):(\d{1,2})$', s)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    
    # m.ss (minutes.seconds)
    m = re.match(r'^(\d+)\.(\d{1,2})$', s)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    
    # with letters like 1m30s, 90s, 2m
    m = re.match(r'^(?:(\d+)m)?(?:(\d+)s)?$', s)
    if m and (m.group(1) or m.group(2)):
        minutes = int(m.group(1)) if m.group(1) else 0
        seconds = int(m.group(2)) if m.group(2) else 0
        return minutes * 60 + seconds
    
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
    except Exception:
        pass

def find_all_dirs_with_json(input_root: Path):
    """Return sorted list of directories (any depth) containing at least one .json directly inside them."""
    if not input_root.exists() or not input_root.is_dir():
        return []
    
    found = set()
    for p in sorted(input_root.rglob("*")):
        if p.is_dir():
            try:
                has = any(child.is_file() and child.suffix.lower() == ".json" for child in p.iterdir())
            except Exception:
                has = False
            if has:
                found.add(p)
    
    try:
        if any(child.is_file() and child.suffix.lower() == ".json" for child in input_root.iterdir()):
            found.add(input_root)
    except Exception:
        pass
    
    return sorted(found)

def find_json_files_in_dir(dirpath: Path):
    """Return sorted list of json files directly in dirpath (non-recursive)."""
    try:
        return sorted([p for p in dirpath.glob("*.json") if p.is_file()])
    except Exception:
        return []

def load_json_events(path: Path):
    """Load JSON and normalize to list of events (preserve structure where possible)."""
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
    """
    Return events sorted, shifted so earliest Time is 0, and the event duration in ms.
    """
    if not events:
        return [], 0
    
    events_with_time = []
    for e in events:
        try:
            t = int(e.get("Time", 0))
        except Exception:
            try:
                t = int(float(e.get("Time", 0)))
            except:
                t = 0
        events_with_time.append((e, t))
    
    try:
        events_with_time.sort(key=lambda x: x[1])
    except Exception as e:
        print(f"WARNING: Could not sort events, proceeding without sorting. Error: {e}", file=sys.stderr)
    
    if not events_with_time:
        return [], 0
    
    min_t = events_with_time[0][1]
    shifted = []
    for (original_event, original_time) in events_with_time:
        ne = deepcopy(original_event)
        ne["Time"] = original_time - min_t
        shifted.append(ne)
    
    duration_ms = shifted[-1]["Time"] if shifted else 0
    return shifted, duration_ms

def part_from_filename(fname: str):
    """
    Token rule: up to 4 alphanumeric chars, prefer letters first then digits.
    """
    stem = Path(fname).stem
    letters = [ch for ch in stem if ch.isalpha()]
    digits = [ch for ch in stem if ch.isdigit()]
    
    token_chars = []
    for ch in letters:
        if len(token_chars) >= 4:
            break
        token_chars.append(ch.lower())
    
    if len(token_chars) < 4:
        for d in digits:
            if len(token_chars) >= 4:
                break
            token_chars.append(d)
    
    if not token_chars:
        alnum = [ch for ch in stem if ch.isalnum()]
        token_chars = [ch.lower() for ch in alnum[:4]]
    
    return ''.join(token_chars)

def insert_intra_pauses(events, rng, max_pauses, min_s, max_s):
    """Return events with inserted intra pauses and a list of the pauses (ms)."""
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
        # Random pause with millisecond precision
        pause_ms = rng.randint(min_s * 1000, max_s * 1000)
        
        # shift subsequent events
        for j in range(gap_idx+1, n):
            evs[j]["Time"] = int(evs[j].get("Time", 0)) + pause_ms
        
        pauses_info.append({"after_event_index": gap_idx, "pause_ms": pause_ms})
    
    return evs, pauses_info

def apply_shifts(events, shift_ms):
    """Preserve all keys but update Time by adding shift_ms."""
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
        ne["Time"] = t + int(shift_ms)
        shifted.append(ne)
    return shifted

def compute_minutes_from_ms(ms: int):
    return math.ceil(ms / 60000) if ms > 0 else 0

def safe_sample(population, k, rng):
    if not population or k <= 0:
        return []
    if k >= len(population):
        return list(population)
    return rng.sample(population, k=k)

def number_to_letters(n: int) -> str:
    """Convert 1->A, 2->B, ... 26->Z, 27->AA, ... Excel-style uppercase."""
    if n <= 0:
        return ""
    letters = ""
    while n > 0:
        n -= 1
        letters = chr(ord('A') + (n % 26)) + letters
        n //= 26
    return letters

# --------- find special file ---------
def locate_special_file_for_group(folder: Path, input_root: Path):
    """
    Robust locate of the special file for a group.
    """
    # 1) exact in the group folder
    try:
        cand = folder / SPECIAL_FILENAME
        if cand.exists():
            return cand.resolve()
    except Exception:
        pass
    
    # 2) exact under input_root
    try:
        cand2 = input_root / SPECIAL_FILENAME
        if cand2.exists():
            return cand2.resolve()
    except Exception:
        pass
    
    # 3) search repo root (cwd) for files that include SPECIAL_KEYWORD in name
    repo_root = Path.cwd()
    keyword = SPECIAL_KEYWORD.lower()
    try:
        for p in repo_root.iterdir():
            if p.is_file() and keyword in p.name.lower():
                return p.resolve()
    except Exception:
        pass
    
    # 4) fallback: search entire repo recursively
    try:
        for p in repo_root.rglob("*"):
            if p.is_file() and keyword in p.name.lower():
                return p.resolve()
    except Exception:
        pass
    
    return None

# --------- generate for a single folder ---------
def generate_version_for_folder(files, rng, version_num,
                                exclude_count,
                                within_min_s, within_max_s, within_max_pauses,
                                between_min_s, between_max_s,
                                folder_path: Path,
                                input_root: Path):
    """Merge provided list of files (all from same folder)."""
    if not files:
        return None, [], [], {"inter_file_pauses":[], "intra_file_pauses":[]}, [], 0
    
    m = len(files)
    ex_count = max(0, min(exclude_count, max(0, m-1)))
    excluded = safe_sample(files, ex_count, rng) if ex_count > 0 else []
    included = [f for f in files if f not in excluded]
    
    if not included:
        included = files.copy()
    
    # duplication
    dup_files = []
    if included:
        dup_count = min(2, len(included))
        if dup_count > 0:
            dup_files = safe_sample(included, dup_count, rng) if len(included) > 1 else [included[0]]
    
    final_files = included + dup_files
    
    # optional extra copies
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
    
    # Handle mobile special file
    special_path = None
    is_mobile_group = any("mobile" in part.lower() for part in folder_path.parts)
    
    if is_mobile_group:
        special_cand = locate_special_file_for_group(folder_path, input_root)
        if special_cand:
            special_path = special_cand
            final_files = [f for f in final_files if Path(f).resolve() != special_path]
            
            if final_files:
                mid_idx = len(final_files) // 2
                insert_pos = min(mid_idx + 1, len(final_files))
                final_files.insert(insert_pos, str(special_path))
            else:
                final_files.insert(0, str(special_path))
            
            final_files.append(str(special_path))
        else:
            print(f"INFO: mobile group {folder_path} - special '{SPECIAL_FILENAME}' not found in repo; skipping insertion.")
    
    merged = []
    pause_info = {"inter_file_pauses": [], "intra_file_pauses": []}
    time_cursor = 0
    per_file_event_ms = {}
    per_file_inter_ms = {}
    
    for idx, fpath in enumerate(final_files):
        fpath_obj = Path(fpath)
        is_special = special_path is not None and fpath_obj.resolve() == special_path.resolve()
        
        evs = load_json_events(fpath_obj)
        zb_evs, file_duration_ms = zero_base_events(evs)
        
        # For special file: no intra pauses
        if is_special:
            intra_evs = zb_evs
            intra_details = []
            per_file_event_ms[str(fpath_obj)] = file_duration_ms
        else:
            intra_evs, intra_details = insert_intra_pauses(zb_evs, rng, within_max_pauses, within_min_s, within_max_s)
            if intra_details:
                pause_info["intra_file_pauses"].append({"file": fpath_obj.name, "pauses": intra_details})
            
            total_event_ms = intra_evs[-1]["Time"] if intra_evs else 0
            per_file_event_ms[str(fpath_obj)] = total_event_ms
        
        shifted = apply_shifts(intra_evs, time_cursor)
        merged.extend(shifted)
        
        if shifted:
            file_max = shifted[-1]["Time"]
            time_cursor = file_max
        
        # Pause logic
        if idx < len(final_files) - 1:
            pause_ms = 0
            
            if is_special:
                SHORT_MID_BUFFER_MS = 1000
                pause_ms = SHORT_MID_BUFFER_MS
            else:
                # Random pause with millisecond precision (0 to between_max_s)
                pause_ms = rng.randint(between_min_s * 1000, between_max_s * 1000)
            
            time_cursor += pause_ms
            per_file_inter_ms[str(fpath_obj)] = pause_ms
            pause_info["inter_file_pauses"].append({"after_file": fpath_obj.name, "pause_ms": pause_ms, "after_index": idx})
        else:
            POST_APPEND_BUFFER_MS = 1000
            per_file_inter_ms[str(fpath_obj)] = POST_APPEND_BUFFER_MS
            time_cursor += POST_APPEND_BUFFER_MS
    
    total_ms = time_cursor if merged else 0
    total_minutes = compute_minutes_from_ms(total_ms)
    
    # build parts
    parts = []
    for f in final_files:
        event_ms = per_file_event_ms.get(str(f), 0)
        inter_ms = per_file_inter_ms.get(str(f), 0)
        combined_ms = event_ms + inter_ms
        minutes = compute_minutes_from_ms(combined_ms)
        parts.append(f"{part_from_filename(Path(f).name)}[{minutes}m]")
    
    letters = number_to_letters(version_num or 1)
    base_name = f"{letters}_{total_minutes}m= " + " - ".join(parts)
    safe_name = ''.join(ch for ch in base_name if ch not in '/\\:*?"<>|')
    merged_fname = f"{safe_name}.json"
    
    return merged_fname, merged, [str(p) for p in final_files], pause_info, [str(p) for p in excluded], total_minutes

# --------- CLI ---------
def build_arg_parser():
    p = argparse.ArgumentParser()
    p.add_argument("--input-dir", required=False, default="originals")
    p.add_argument("--output-dir", required=False, default="output")
    p.add_argument("--versions", type=int, default=16)
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--exclude-count", type=int, default=5)
    p.add_argument("--within-max-time", type=str, default="33")
    p.add_argument("--within-max-pauses", type=int, default=3)
    p.add_argument("--between-max-time", type=str, default="18")
    p.add_argument("--between-max-pauses", type=int, default=1)
    
    # legacy aliases
    p.add_argument("--intra-file-max", type=str, dest="within_max_time", help=argparse.SUPPRESS)
    p.add_argument("--intra-file-max-pauses", type=int, dest="within_max_pauses", help=argparse.SUPPRESS)
    p.add_argument("--inter-file-max", type=str, dest="between_max_time", help=argparse.SUPPRESS)
    p.add_argument("--inter-file-max-pauses", type=int, dest="between_max_pauses", help=argparse.SUPPRESS)
    p.add_argument("--inter-file-count", type=int, dest="between_max_pauses", help=argparse.SUPPRESS)
    
    return p

# --------- main ---------
def main():
    parser = build_arg_parser()
    args = parser.parse_args()
    
    rng = random.Random(args.seed) if args.seed is not None else random.Random()
    
    input_root = Path(args.input_dir)
    output_parent = Path(args.output_dir)
    output_parent.mkdir(parents=True, exist_ok=True)
    
    bundle_seq_env = os.environ.get("BUNDLE_SEQ", "").strip()
    if bundle_seq_env:
        try:
            counter = int(bundle_seq_env)
        except:
            counter = read_counter_file() or 1
    else:
        prev = read_counter_file()
        counter = prev + 1 if prev >= 0 else 1
        write_counter_file(counter)
    
    output_base_name = f"merged_bundle_{counter}"
    output_root = output_parent / output_base_name
    output_root.mkdir(parents=True, exist_ok=True)
    
    folder_dirs = find_all_dirs_with_json(input_root)
    
    if not folder_dirs:
        print(f"No json files found under {input_root}", file=sys.stderr)
        return
    
    try:
        within_max_s = parse_time_to_seconds(getattr(args, "within_max_time"))
    except Exception as e:
        print(f"ERROR parsing within max time: {e}", file=sys.stderr)
        return
    
    try:
        between_max_s = parse_time_to_seconds(getattr(args, "between_max_time"))
    except Exception as e:
        print(f"ERROR parsing between max time: {e}", file=sys.stderr)
        return
    
    # Minimum pause times are now 0
    within_min_s = 0
    between_min_s = 0
    
    all_written_paths = []
    
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
                between_min_s, between_max_s,
                folder, input_root
            )
            
            if not merged_fname:
                continue
            
            out_path = out_folder_for_group / merged_fname
            try:
                out_path.write_text(json.dumps(merged_events, indent=2, ensure_ascii=False), encoding="utf-8")
                print(f"WROTE: {out_path}")
                all_written_paths.append(out_path)
            except Exception as e:
                print(f"ERROR writing {out_path}: {e}", file=sys.stderr)
    
    # ZIP
    zip_path = output_parent / f"{output_base_name}.zip"
    with ZipFile(zip_path, "w") as zf:
        for fpath in all_written_paths:
            try:
                arcname = str(fpath.relative_to(output_parent))
            except Exception:
                arcname = f"{output_base_name}/{fpath.name}"
            zf.write(fpath, arcname=arcname)
    
    print("DONE. Created zip:", zip_path)

if __name__ == "__main__":
    main()
