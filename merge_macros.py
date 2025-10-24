#!/usr/bin/env python3
"""
merge_macros.py (Patched)
- Scans all subfolders under --input-dir for .json files and treats each directory that
  directly contains .json files as a separate group to merge.
- Mirrors folder tree under output/<merged_bundle_{N}>.
- For groups under 'mobile' (case-insensitive), inserts the special file
  'close reopen mobile screensharelink.json' (or filename variants containing 'screensharelink')
  once near the middle (after a file boundary) and once at the end.
- Special file gets no intra-pauses and no random inter-file pauses (only a 1s buffer).
- Per-file displayed time = event duration (including intra-file pauses) + inter-file pause after it.
- Total displayed time calculated from final time cursor.
- Elastic min rule for pause ranges: if UI max < hardcoded min, min becomes 0..UI_max.
- Preserves all event fields when shifting times.
- Filenames start with alphabetic version labels (A_, B_, ... AA_, ...).
- Part tokens use up to first 4 alphanumeric chars: prefer letters then digits (lowercased).
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
# ---------- constants ----------
DEFAULT_INTRA_MIN_SEC = 4     # 4 seconds
DEFAULT_INTER_MIN_SEC = 30    # 30 seconds
COUNTER_PATH = Path(".github/merge_bundle_counter.txt")
SPECIAL_FILENAME = "close reopen mobile screensharelink.json"
SPECIAL_KEYWORD = "screensharelink"  # used for tolerant matching
# ---------- helpers ----------
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
    # plain integer seconds
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
    except Exception:
        pass
def find_all_dirs_with_json(input_root: Path):
    """Return sorted list of directories (any depth) containing at least one .json directly inside them."""
    if not input_root.exists() or not input_root.is_dir():
        return []
    found = set()
    # walk directories and check direct children for .json files
    for p in sorted(input_root.rglob("*")):
        if p.is_dir():
            try:
                has = any(child.is_file() and child.suffix.lower() == ".json" for child in p.iterdir())
            except Exception:
                has = False
            if has:
                found.add(p)
    # also check input_root itself for jsons
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
    """Return events shifted so earliest Time is 0 and the event duration in ms."""
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
    duration_ms = max(int(e.get("Time", 0)) for e in shifted) if shifted else 0
    return shifted, duration_ms
def part_from_filename(fname: str):
    """
    New token rule: up to 4 alphanumeric chars, prefer letters first then digits.
    - Extract letters (a-z) from stem in order; take first up to 4.
    - If less than 4 letters, append digits from stem (in order) until up to 4 chars.
    - Lowercase result.
    - If no letters/digits, fallback to first up to 4 alnum chars of stem.
    """
    stem = Path(fname).stem
    # collect letters and digits in order
    letters = [ch for ch in stem if ch.isalpha()]
    digits = [ch for ch in stem if ch.isdigit()]
    token_chars = []
    # take up to 4 letters first
    for ch in letters:
        if len(token_chars) >= 4:
            break
        token_chars.append(ch.lower())
    # if fewer than 4, append digits
    if len(token_chars) < 4:
        for d in digits:
            if len(token_chars) >= 4:
                break
            token_chars.append(d)
    # if still empty, fall back to first up to 4 alnum chars (non-letter/digit unlikely)
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
        pause_s = rng.randint(min_s, max_s)
        pause_ms = pause_s * 1000
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
                # *** FIX 2: Corrected typo 'm' to '0' ***
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
# ---------- find special file ----------
def locate_special_file_for_group(folder: Path, input_root: Path):
    """
    Robust locate of the special file for a group.
    Priority:
      1) exact filename present directly under the group folder
      2) exact filename present under input_root (originals/)
      3) file under repository root (cwd) whose name contains SPECIAL_KEYWORD (case-insensitive)
      4) first match anywhere in repo whose name contains SPECIAL_KEYWORD (case-insensitive)
      5) None if not found
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
    # 3) search repo root (cwd) for files that include SPECIAL_KEYWORD in name (case-insensitive)
    repo_root = Path.cwd()
    keyword = SPECIAL_KEYWORD.lower()
    try:
        for p in repo_root.iterdir():
            if p.is_file() and keyword in p.name.lower():
                return p.resolve()
    except Exception:
        pass
    # 4) fallback: search entire repo recursively for the keyword in filename
    try:
        for p in repo_root.rglob("*"):
            if p.is_file() and keyword in p.name.lower():
                return p.resolve()
    except Exception:
        pass
    return None
# ---------- generate for a single folder ----------
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
    # duplication (keeps previous behavior)
    dup_files = []
    if included:
        dup_count = min(2, len(included))
        if dup_count > 0:
            dup_files = safe_sample(included, dup_count, rng) if len(included) > 1 else [included[0]]
    final_files = included + dup_files
    # optional extra copies inserted at random positions
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
    # If group is under mobile (case-insensitive), locate special file and insert twice:
    special_path = None
    is_mobile_group = any("mobile" in part.lower() for part in folder_path.parts)
    if is_mobile_group:
        special_cand = locate_special_file_for_group(folder_path, input_root)
        if special_cand:
            special_path = special_cand
            # remove any occurrences from final_files to avoid duplicates from the folder content
            final_files = [f for f in final_files if Path(f).resolve() != special_path]
            # insert near middle AFTER a file boundary:
            if final_files:
                mid_idx = len(final_files) // 2
                insert_pos = min(mid_idx + 1, len(final_files))
                final_files.insert(insert_pos, str(special_path))
            else:
                final_files.insert(0, str(special_path))
            # also append at end
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
        zb_evs, _ = zero_base_events(evs)
        # For special file: do NOT insert intra pauses (play as-is)
        if is_special:
            intra_evs = zb_evs
            intra_details = []
        else:
            intra_evs, intra_details = insert_intra_pauses(zb_evs, rng, within_max_pauses, within_min_s, within_max_s)
            if intra_details:
                pause_info["intra_file_pauses"].append({"file": fpath_obj.name, "pauses": intra_details})
        # shift by cursor and append
        shifted = apply_shifts(intra_evs, time_cursor)
        merged.extend(shifted)
        # compute event duration (including intra)
        if shifted:
            file_max = max(int(e.get("Time",0)) for e in shifted)
            file_min = min(int(e.get("Time",0)) for e in shifted)
            event_ms = file_max - file_min
            # *** FIX 1: Corrected typo 'event_.ms' to 'event_ms' ***
            per_file_event_ms[str(fpath_obj)] = event_ms
            time_cursor = file_max # Set cursor to the end of the last event
        else:
            per_file_event_ms[str(fpath_obj)] = 0
            
        # --- *** FIX 2: Reworked Pause Logic *** ---
        
        # Check if this is the very last file
        if idx < len(final_files) - 1:
            # This is NOT the last file
            pause_ms = 0
            # Check if it's the special file (in the middle)
            if is_special:
                # Apply a short, 1s hardcoded buffer to prevent collision
                # This makes it exempt from random pause rules
                SHORT_MID_BUFFER_MS = 1000
                pause_ms = SHORT_MID_BUFFER_MS
                print(f"INFO: Applying short {pause_ms}ms buffer after special file {fpath_obj.name}")
            else:
                # Apply a normal random pause
                low = between_min_s if between_max_s >= between_min_s else 0
                high = between_max_s
                if low > high:
                    low = 0
                pause_s = rng.randint(low, high)
                pause_ms = pause_s * 1000
            
            time_cursor += pause_ms # Advance cursor by the pause
            per_file_inter_ms[str(fpath_obj)] = pause_ms
            pause_info["inter_file_pauses"].append({"after_file": fpath_obj.name, "pause_ms": pause_ms, "after_index": idx})
        
        else:
            # This is the VERY last file
            # Apply the 1-second final buffer for the player
            POST_APPEND_BUFFER_MS = 1000
            per_file_inter_ms[str(fpath_obj)] = POST_APPEND_BUFFER_MS
            time_cursor += POST_APPEND_BUFFER_MS # Advance cursor by buffer
            
    # total ms from merged events
    if merged:
        # Total time is the final cursor position
        total_ms = time_cursor
    else:
        total_ms = 0
    total_minutes = compute_minutes_from_ms(total_ms)
    # build parts: each file's displayed minutes = ceil((event_ms + inter_ms)/60000)
    parts = []
    for f in final_files:
        event_ms = per_file_event_ms.get(str(f), 0)
        inter_ms = per_file_inter_ms.get(str(f), 0)
        combined_ms = event_ms + inter_ms
        minutes = compute_minutes_from_ms(combined_ms)
        parts.append(f"{part_from_filename(Path(f).name)}[{minutes}m]")
    # version label
    letters = number_to_letters(version_num or 1)
    base_name = f"{letters}_{total_minutes}m= " + " - ".join(parts)
    safe_name = ''.join(ch for ch in base_name if ch not in '/\\:*?"<>|')
    merged_fname = f"{safe_name}.json"
    return merged_fname, merged, [str(p) for p in final_files], pause_info, [str(p) for p in excluded], total_minutes
# ---------- CLI ----------
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
# ---------- main ----------
def main():
    parser = build_arg_parser()
    args = parser.parse_args()
    rng = random.Random(args.seed) if args.seed is not None else random.Random()
    input_root = Path(args.input_dir)
    output_parent = Path(args.output_dir)
    output_parent.mkdir(parents=True, exist_ok=True)
    # pick bundle sequence: prefer env BUNDLE_SEQ (workflow writes it), else increment local counter file
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
    # find directories that directly contain .json (non-recursive per directory)
    folder_dirs = find_all_dirs_with_json(input_root)
    if not folder_dirs:
        print(f"No json files found under {input_root}", file=sys.stderr)
        return
    # parse times and determine elastic minima
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
    # ZIP: ensure top-level entry is output_base_name (no leading "output/")
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

