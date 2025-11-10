#!/usr/bin/env python3
"""
merge_macros.py (Complete Working Version)
Environment-specific anti-detection for OSRS botting
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
from itertools import combinations

# Constants
COUNTER_PATH = Path(".github/merge_bundle_counter.txt")
SPECIAL_FILENAME = "close reopen mobile screensharelink.json"
SPECIAL_KEYWORD = "screensharelink"

def parse_time_to_seconds(s: str) -> int:
    if s is None:
        raise ValueError("Empty time string")
    s = str(s).strip()
    if not s:
        raise ValueError("Empty time string")
    if re.match(r'^\d+$', s):
        return int(s)
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
    try:
        return sorted([p for p in dirpath.glob("*.json") if p.is_file()])
    except Exception:
        return []

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
        print(f"WARNING: Could not sort events: {e}", file=sys.stderr)
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

def compute_minutes_from_ms(ms: int):
    return math.ceil(ms / 60000) if ms > 0 else 0

def number_to_letters(n: int) -> str:
    if n <= 0:
        return ""
    letters = ""
    while n > 0:
        n -= 1
        letters = chr(ord('A') + (n % 26)) + letters
        n //= 26
    return letters

def add_micro_pauses(events, rng, micropause_chance=0.15):
    paused = []
    for e in deepcopy(events):
        if rng.random() < micropause_chance:
            hesitation = rng.randint(50, 250)
            e['Time'] = int(e.get('Time', 0)) + hesitation
        paused.append(e)
    return paused

def add_reaction_variance(events, rng):
    varied = []
    for i, e in enumerate(deepcopy(events)):
        is_click = (e.get('Type') in ['Click', 'RightClick'] or 'button' in e or 'Button' in e)
        if is_click and i > 0 and rng.random() < 0.3:
            reaction_time = rng.randint(200, 600)
            e['Time'] = int(e.get('Time', 0)) + reaction_time
        varied.append(e)
    return varied

def add_mouse_jitter(events, rng, is_desktop=False):
    jittered = []
    jitter_range = [-1, 0, 1] if is_desktop else [-2, -1, 0, 1, 2]
    for e in deepcopy(events):
        is_click = (e.get('Type') in ['Click', 'LeftClick', 'RightClick'] or 'button' in e or 'Button' in e)
        if is_click and 'X' in e and 'Y' in e and e['X'] is not None and e['Y'] is not None:
            try:
                original_x = int(e['X'])
                original_y = int(e['Y'])
                offset_x = rng.choice(jitter_range)
                offset_y = rng.choice(jitter_range)
                e['X'] = original_x + offset_x
                e['Y'] = original_y + offset_y
            except:
                pass
        jittered.append(e)
    return jittered

def add_time_of_day_fatigue(events, rng):
    try:
        from datetime import datetime
        now = datetime.now()
        hour = now.hour
        is_weekend = now.weekday() >= 5
        if 23 <= hour or hour < 5:
            fatigue_min, fatigue_max = 1.15, 1.30
        elif 6 <= hour < 12:
            fatigue_min, fatigue_max = 0.95, 1.05
        elif 18 <= hour < 23:
            fatigue_min, fatigue_max = 1.0, 1.1
        else:
            fatigue_min, fatigue_max = 1.05, 1.15
        if is_weekend:
            fatigue_min += 0.05
            fatigue_max += 0.1
        fatigue_multiplier = rng.uniform(fatigue_min, fatigue_max)
        fatigued = []
        for e in deepcopy(events):
            original_time = int(e.get('Time', 0))
            e['Time'] = int(original_time * fatigue_multiplier)
            fatigued.append(e)
        return fatigued, 0.0
    except Exception:
        return events, 0.0

def insert_intra_pauses(events, rng, max_pauses, min_s, max_s):
    if not events or max_pauses <= 0:
        return deepcopy(events), []
    evs = deepcopy(events)
    n = len(evs)
    if n < 2:
        return evs, []
    k = rng.randint(1, min(max_pauses, n-1))
    chosen = rng.sample(range(n-1), k)
    pauses_info = []
    for gap_idx in sorted(chosen):
        pause_ms = rng.randint(min_s * 1000, max_s * 1000)
        for j in range(gap_idx+1, n):
            evs[j]["Time"] = int(evs[j].get("Time", 0)) + pause_ms
        pauses_info.append({"after_event_index": gap_idx, "pause_ms": pause_ms})
    return evs, pauses_info

def apply_shifts(events, shift_ms):
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

class NonRepeatingSelector:
    def __init__(self, rng):
        self.rng = rng
        self.used_combos = set()
    
    def select_files(self, files, exclude_count):
        if not files:
            return [], []
        n = len(files)
        max_exclude = min(exclude_count, max(0, n - 1))
        file_indices = list(range(n))
        all_possible = []
        for exclude_k in range(0, max_exclude + 1):
            for combo in combinations(file_indices, exclude_k):
                all_possible.append(frozenset(combo))
        available = [c for c in all_possible if c not in self.used_combos]
        if not available:
            self.used_combos.clear()
            available = all_possible
        chosen_exclude_indices = self.rng.choice(available)
        self.used_combos.add(chosen_exclude_indices)
        excluded = [files[i] for i in chosen_exclude_indices]
        included = [files[i] for i in file_indices if i not in chosen_exclude_indices]
        return included if included else files.copy(), excluded
    
    def shuffle_with_memory(self, items):
        if not items or len(items) <= 1:
            return items
        if len(items) > 8:
            shuffled = items.copy()
            self.rng.shuffle(shuffled)
            return shuffled
        items_tuple = tuple(items)
        from itertools import permutations as iter_perms
        all_perms = [perm for perm in iter_perms(items_tuple)]
        available = [p for p in all_perms if p not in self.used_combos]
        if not available:
            self.used_combos.clear()
            available = all_perms
        chosen = self.rng.choice(available)
        self.used_combos.add(chosen)
        return list(chosen)

def locate_special_file_for_group(folder: Path, input_root: Path):
    try:
        cand = folder / SPECIAL_FILENAME
        if cand.exists():
            return cand.resolve()
    except Exception:
        pass
    try:
        cand2 = input_root / SPECIAL_FILENAME
        if cand2.exists():
            return cand2.resolve()
    except Exception:
        pass
    repo_root = Path.cwd()
    keyword = SPECIAL_KEYWORD.lower()
    try:
        for p in repo_root.iterdir():
            if p.is_file() and keyword in p.name.lower():
                return p.resolve()
    except Exception:
        pass
    try:
        for p in repo_root.rglob("*"):
            if p.is_file() and keyword in p.name.lower():
                return p.resolve()
    except Exception:
        pass
    return None

def generate_version_for_folder(files, rng, version_num, exclude_count,
                                within_max_s, within_max_pauses, between_max_s,
                                folder_path: Path, input_root: Path,
                                selector: NonRepeatingSelector):
    if not files:
        return None, [], [], {"inter_file_pauses":[], "intra_file_pauses":[]}, [], 0
    
    included, excluded = selector.select_files(files, exclude_count)
    if not included:
        included = files.copy()
    
    # Check for "always first" and "always last" files
    always_first_file = None
    always_last_file = None
    for f in included:
        fname_lower = Path(f).name.lower()
        if fname_lower.startswith("always first"):
            always_first_file = f
        elif fname_lower.startswith("always last"):
            always_last_file = f
    
    included = [f for f in included if f not in [always_first_file, always_last_file]]
    
    use_always_first_this_version = False
    use_always_last_this_version = False
    if always_first_file and always_last_file:
        if version_num % 2 == 1:
            use_always_first_this_version = True
        else:
            use_always_last_this_version = True
    elif always_first_file:
        use_always_first_this_version = (version_num % 3 == 1)
    elif always_last_file:
        use_always_last_this_version = (version_num % 3 == 2)
    
    final_files = selector.shuffle_with_memory(included)
    
    if use_always_first_this_version and always_first_file:
        final_files.insert(0, always_first_file)
    if use_always_last_this_version and always_last_file:
        final_files.append(always_last_file)
    
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
        
        # Determine if desktop or mobile
        folder_path_lower = str(folder_path).lower()
        is_desktop = "deskt" in folder_path_lower
        
        if not is_special:
            zb_evs, _ = add_time_of_day_fatigue(zb_evs, rng)
            zb_evs = add_micro_pauses(zb_evs, rng)
            zb_evs = add_reaction_variance(zb_evs, rng)
            zb_evs = add_mouse_jitter(zb_evs, rng, is_desktop=is_desktop)
            zb_evs, file_duration_ms = zero_base_events(zb_evs)
        
        if is_special:
            intra_evs = zb_evs
            per_file_event_ms[str(fpath_obj)] = file_duration_ms
        else:
            intra_evs, intra_details = insert_intra_pauses(zb_evs, rng, within_max_pauses, 0, int(within_max_s))
            if intra_details:
                pause_info["intra_file_pauses"].append({"file": fpath_obj.name, "pauses": intra_details})
            per_file_event_ms[str(fpath_obj)] = intra_evs[-1]["Time"] if intra_evs else 0
        
        shifted = apply_shifts(intra_evs, time_cursor)
        merged.extend(shifted)
        
        if shifted:
            time_cursor = shifted[-1]["Time"]
        
        if idx < len(final_files) - 1:
            if is_special:
                pause_ms = 1000
            else:
                pause_ms = rng.randint(0, int(between_max_s * 1000))
            time_cursor += pause_ms
            per_file_inter_ms[str(fpath_obj)] = pause_ms
            pause_info["inter_file_pauses"].append({"after_file": fpath_obj.name, "pause_ms": pause_ms})
        else:
            per_file_inter_ms[str(fpath_obj)] = 1000
            time_cursor += 1000
    
    total_ms = time_cursor if merged else 0
    total_minutes = compute_minutes_from_ms(total_ms)
    
    parts = []
    for f in final_files:
        event_ms = per_file_event_ms.get(str(f), 0)
        inter_ms = per_file_inter_ms.get(str(f), 0)
        combined_ms = event_ms + inter_ms
        minutes = compute_minutes_from_ms(combined_ms)
        parts.append(f"{part_from_filename(Path(f).name)}[{minutes}m]")
    
    letters = number_to_letters(version_num or 1)
    if use_always_last_this_version and always_last_file:
        base_name = f"always last - {letters}_{total_minutes}m= " + " - ".join(parts)
    else:
        base_name = f"{letters}_{total_minutes}m= " + " - ".join(parts)
    safe_name = ''.join(ch for ch in base_name if ch not in '/\\:*?"<>|')
    merged_fname = f"{safe_name}.json"
    
    return merged_fname, merged, [str(p) for p in final_files], pause_info, [str(p) for p in excluded], total_minutes

def build_arg_parser():
    p = argparse.ArgumentParser()
    p.add_argument("--input-dir", required=False, default="originals")
    p.add_argument("--output-dir", required=False, default="output")
    p.add_argument("--versions", type=int, default=26)
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--exclude-count", type=int, default=10)
    p.add_argument("--within-max-time", type=str, default="33")
    p.add_argument("--within-max-pauses", type=int, default=2)
    p.add_argument("--between-max-time", type=str, default="18")
    return p

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
        
        selector = NonRepeatingSelector(rng)
        
        for v in range(1, max(1, args.versions) + 1):
            merged_fname, merged_events, finals, pauses, excluded, total_minutes = generate_version_for_folder(
                files, rng, v, args.exclude_count,
                within_max_s, getattr(args, "within_max_pauses"), between_max_s,
                folder, input_root, selector
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
