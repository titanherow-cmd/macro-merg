#!/usr/bin/env python3
"""merge_macros.py - OSRS Anti-Detection with AFK & Zone Awareness"""

from pathlib import Path
import argparse, json, random, re, sys, os, math
from copy import deepcopy
from zipfile import ZipFile
from itertools import combinations, permutations

COUNTER_PATH = Path(".github/merge_bundle_counter.txt")
SPECIAL_FILENAME = "close reopen mobile screensharelink.json"
SPECIAL_KEYWORD = "screensharelink"

def parse_time_to_seconds(s: str) -> int:
    if s is None or not str(s).strip():
        raise ValueError("Empty time string")
    s = str(s).strip()
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
    raise ValueError(f"Cannot parse time: {s!r}")

def read_counter_file():
    try:
        if COUNTER_PATH.exists():
            txt = COUNTER_PATH.read_text(encoding="utf-8").strip()
            return int(txt) if txt else 0
    except:
        pass
    return 0

def write_counter_file(n: int):
    try:
        COUNTER_PATH.parent.mkdir(parents=True, exist_ok=True)
        COUNTER_PATH.write_text(str(n), encoding="utf-8")
    except:
        pass

def load_exemption_config():
    config_file = Path.cwd() / "exemption_config.json"
    if config_file.exists():
        try:
            data = json.loads(config_file.read_text(encoding="utf-8"))
            return {
                "exempted_folders": set(data.get("exempted_folders", [])),
                "disable_intra_pauses": data.get("disable_intra_pauses", True),
                "disable_afk": data.get("disable_afk", True)
            }
        except Exception as e:
            print(f"WARNING: Failed to load exemptions: {e}", file=sys.stderr)
    return {"exempted_folders": set(), "disable_intra_pauses": False, "disable_afk": False}

def is_folder_exempted(folder_path: Path, exempted_folders: set) -> bool:
    folder_str = str(folder_path).lower().replace("\\", "/")
    for exempted in exempted_folders:
        if exempted.lower().replace("\\", "/") in folder_str:
            return True
    return False

def load_click_zones(folder_path: Path):
    search_paths = [folder_path / "click_zones.json", folder_path.parent / "click_zones.json", Path.cwd() / "click_zones.json"]
    for zone_file in search_paths:
        if zone_file.exists():
            try:
                data = json.loads(zone_file.read_text(encoding="utf-8"))
                return data.get("target_zones", []), data.get("excluded_zones", [])
            except Exception as e:
                print(f"WARNING: Failed to load {zone_file}: {e}", file=sys.stderr)
    return [], []

def is_click_in_zone(x: int, y: int, zone: dict) -> bool:
    try:
        return zone['x1'] <= x <= zone['x2'] and zone['y1'] <= y <= zone['y2']
    except:
        return False

def find_all_dirs_with_json(input_root: Path):
    if not input_root.exists() or not input_root.is_dir():
        return []
    found = set()
    for p in sorted(input_root.rglob("*")):
        if p.is_dir():
            try:
                has = any(child.is_file() and child.suffix.lower() == ".json" for child in p.iterdir())
                if has:
                    found.add(p)
            except:
                pass
    return sorted(found)

def find_json_files_in_dir(dirpath: Path):
    try:
        return sorted([p for p in dirpath.glob("*.json") if p.is_file() and not p.name.startswith("click_zones")])
    except:
        return []

def load_json_events(path: Path):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"WARNING: Failed to read {path}: {e}", file=sys.stderr)
        return []
    if isinstance(data, dict):
        for k in ("events", "items", "entries", "records", "actions"):
            if k in data and isinstance(data[k], list):
                return deepcopy(data[k])
        if "Time" in data:
            return [deepcopy(data)]
        return []
    return deepcopy(data) if isinstance(data, list) else []

def zero_base_events(events):
    if not events:
        return [], 0
    events_with_time = []
    for e in events:
        try:
            t = int(e.get("Time", 0))
        except:
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
    for (e, t) in events_with_time:
        ne = deepcopy(e)
        ne["Time"] = t - min_t
        shifted.append(ne)
    duration_ms = shifted[-1]["Time"] if shifted else 0
    return shifted, duration_ms

def part_from_filename(fname: str):
    stem = Path(fname).stem
    letters = [ch.lower() for ch in stem if ch.isalpha()][:4]
    digits = [ch for ch in stem if ch.isdigit()][:4-len(letters)]
    return ''.join(letters + digits) or ''.join([ch.lower() for ch in stem if ch.isalnum()][:4])

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

def add_desktop_mouse_paths(events, rng):
    enhanced, last_x, last_y = [], None, None
    
    for e in deepcopy(events):
        is_click = e.get('Type') in ['Click', 'LeftClick', 'RightClick']
        is_drag = e.get('Type') in ['Drag', 'DragStart', 'DragEnd', 'MouseDrag']
        
        if is_click and not is_drag and 'X' in e and 'Y' in e and e['X'] is not None and e['Y'] is not None:
            try:
                target_x, target_y, click_time = int(e['X']), int(e['Y']), int(e.get('Time', 0))
                
                if last_x is not None and last_y is not None:
                    distance = ((target_x - last_x)**2 + (target_y - last_y)**2)**0.5
                    
                    if distance > 30:
                        num_points = rng.randint(3, 5)
                        movement_duration = int(100 + distance * 0.25)
                        movement_duration = min(movement_duration, 400)
                        
                        for i in range(1, num_points + 1):
                            t = i / (num_points + 1)
                            t_smooth = t * t * (3 - 2 * t)
                            
                            inter_x = int(last_x + (target_x - last_x) * t_smooth + rng.randint(-3, 3))
                            inter_y = int(last_y + (target_y - last_y) * t_smooth + rng.randint(-3, 3))
                            point_time = click_time - movement_duration + int(movement_duration * t_smooth)
                            
                            enhanced.append({'Time': max(0, point_time), 'Type': 'MouseMove', 'X': inter_x, 'Y': inter_y})
                
                last_x, last_y = target_x, target_y
            except Exception as ex:
                print(f"Warning: Mouse path error: {ex}", file=sys.stderr)
        
        enhanced.append(e)
        
        if e.get('Type') == 'MouseMove' and 'X' in e and 'Y' in e:
            try:
                last_x, last_y = int(e['X']), int(e['Y'])
            except:
                pass
        elif is_drag and 'X' in e and 'Y' in e:
            try:
                last_x, last_y = int(e['X']), int(e['Y'])
            except:
                pass
    
    return enhanced

def add_micro_pauses(events, rng, micropause_chance=0.15):
    result = []
    for e in events:
        new_e = deepcopy(e)
        if rng.random() < micropause_chance:
            new_e['Time'] = int(e.get('Time', 0)) + rng.randint(50, 250)
        else:
            new_e['Time'] = int(e.get('Time', 0))
        result.append(new_e)
    return result

def add_reaction_variance(events, rng):
    varied = []
    for i, e in enumerate(events):
        new_e = deepcopy(e)
        is_click = e.get('Type') in ['Click', 'RightClick'] or 'button' in e or 'Button' in e
        if is_click and i > 0 and rng.random() < 0.3:
            new_e['Time'] = int(e.get('Time', 0)) + rng.randint(200, 600)
        varied.append(new_e)
    return varied

def add_mouse_jitter(events, rng, is_desktop=False, target_zones=None, excluded_zones=None):
    if target_zones is None:
        target_zones = []
    if excluded_zones is None:
        excluded_zones = []
    jittered, jitter_range = [], [-1, 0, 1]
    for e in events:
        new_e = deepcopy(e)
        is_click = e.get('Type') in ['Click', 'LeftClick', 'RightClick'] or 'button' in e or 'Button' in e
        if is_click and 'X' in e and 'Y' in e and e['X'] is not None and e['Y'] is not None:
            try:
                original_x, original_y = int(e['X']), int(e['Y'])
                in_excluded = any(is_click_in_zone(original_x, original_y, zone) for zone in excluded_zones)
                if not in_excluded and (any(is_click_in_zone(original_x, original_y, zone) for zone in target_zones) or not target_zones):
                    new_e['X'] = original_x + rng.choice(jitter_range)
                    new_e['Y'] = original_y + rng.choice(jitter_range)
            except:
                pass
        jittered.append(new_e)
    return jittered

def add_time_of_day_fatigue(events, rng, is_exempted=False, max_pause_ms=0):
    if not events:
        return events, 0.0
    
    if is_exempted:
        return deepcopy(events), 0.0
    
    if rng.random() < 0.20:
        return deepcopy(events), 0.0
    
    evs = deepcopy(events)
    n = len(evs)
    
    if n < 2:
        return evs, 0.0
    
    num_pauses = rng.randint(0, 3)
    
    if num_pauses == 0:
        return evs, 0.0
    
    pause_locations = rng.sample(range(n - 1), min(num_pauses, n - 1))
    
    for gap_idx in sorted(pause_locations, reverse=True):
        pause_ms = rng.randint(0, 72000)
        
        for j in range(gap_idx + 1, n):
            evs[j]["Time"] = int(evs[j].get("Time", 0)) + pause_ms
    
    return evs, 0.0

def insert_intra_pauses(events, rng, is_exempted=False, max_pause_s=33, max_num_pauses=3):
    if not events:
        return deepcopy(events), []
    
    evs = deepcopy(events)
    n = len(evs)
    
    if n < 2:
        return evs, []
    
    if not is_exempted:
        return evs, []
    
    num_pauses = rng.randint(0, max_num_pauses)
    
    if num_pauses == 0:
        return evs, []
    
    chosen = rng.sample(range(n-1), min(num_pauses, n-1))
    pauses_info = []
    
    for gap_idx in sorted(chosen):
        pause_ms = rng.randint(0, int(max_pause_s * 1000))
        
        for j in range(gap_idx+1, n):
            evs[j]["Time"] = int(evs[j].get("Time", 0)) + pause_ms
        
        pauses_info.append({"after_event_index": gap_idx, "pause_ms": pause_ms})
    
    return evs, pauses_info

def add_afk_pause(events, rng):
    if not events:
        return deepcopy(events)
    evs = deepcopy(events)
    if rng.random() < 0.7:
        afk_seconds = rng.randint(60, 300)
    else:
        afk_seconds = rng.randint(300, 1200)
    afk_ms = afk_seconds * 1000
    insert_idx = rng.randint(len(evs) // 4, 3 * len(evs) // 4) if len(evs) > 1 else 0
    for j in range(insert_idx, len(evs)):
        evs[j]["Time"] = int(evs[j].get("Time", 0)) + afk_ms
    return evs

def apply_shifts(events, shift_ms):
    result = []
    for e in events:
        new_e = deepcopy(e)
        new_e['Time'] = int(e.get('Time', 0)) + int(shift_ms)
        result.append(new_e)
    return result

class NonRepeatingSelector:
    def __init__(self, rng):
        self.rng = rng
        self.used_combos = set()
        self.special_files_used = set()

    def select_files(self, files, exclude_count):
        if not files:
            return [], []
        n = len(files)
        file_indices = list(range(n))
        max_exclude = min(exclude_count, max(0, n - 1))
        all_possible = [frozenset(combo) for exclude_k in range(0, max_exclude + 1) for combo in combinations(file_indices, exclude_k)]
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
        all_perms = list(permutations(items))
        available = [p for p in all_perms if p not in self.used_combos]
        if not available:
            self.used_combos.clear()
            available = all_perms
        chosen = self.rng.choice(available)
        self.used_combos.add(chosen)
        return list(chosen)

    def mark_special_used(self, fname):
        self.special_files_used.add(fname)

    def is_special_used(self, fname):
        return fname in self.special_files_used

def locate_special_file(folder: Path, input_root: Path):
    for cand in [folder / SPECIAL_FILENAME, input_root / SPECIAL_FILENAME]:
        if cand.exists():
            return cand.resolve()
    keyword = SPECIAL_KEYWORD.lower()
    for p in Path.cwd().rglob("*"):
        if p.is_file() and keyword in p.name.lower():
            return p.resolve()
    return None

def generate_version_for_folder(files, rng, version_num, exclude_count, within_max_s, within_max_pauses, between_max_s, folder_path: Path, input_root: Path, selector, exemption_config: dict = None):
    if not files:
        return None, [], [], {"inter_file_pauses": [], "intra_file_pauses": []}, [], 0
    always_first_file = next((f for f in files if Path(f).name.lower().startswith("always first")), None)
    always_last_file = next((f for f in files if Path(f).name.lower().startswith("always last")), None)
    regular_files = [f for f in files if f not in [always_first_file, always_last_file]]
    if not regular_files:
        return None, [], [], {"inter_file_pauses": [], "intra_file_pauses": []}, [], 0
    included, excluded = selector.select_files(regular_files, exclude_count)
    if not included:
        included = regular_files.copy()

    use_special_file = None
    always_first_exists = always_first_file is not None
    always_last_exists = always_last_file is not None
    
    if always_first_exists and always_last_exists:
        if version_num == 1 and not selector.is_special_used(str(always_first_file)):
            use_special_file = always_first_file
            selector.mark_special_used(str(always_first_file))
        elif version_num == 2 and not selector.is_special_used(str(always_last_file)):
            use_special_file = always_last_file
            selector.mark_special_used(str(always_last_file))
    elif always_first_exists and not always_last_exists:
        if version_num == 1 and not selector.is_special_used(str(always_first_file)):
            use_special_file = always_first_file
            selector.mark_special_used(str(always_first_file))
    elif always_last_exists and not always_first_exists:
        if version_num == 1 and not selector.is_special_used(str(always_last_file)):
            use_special_file = always_last_file
            selector.mark_special_used(str(always_last_file))

    final_files = selector.shuffle_with_memory(included)
    if use_special_file == always_first_file:
        final_files.insert(0, always_first_file)
    elif use_special_file == always_last_file:
        final_files.append(always_last_file)

    special_path = locate_special_file(folder_path, input_root)
    is_mobile_group = any("mobile" in part.lower() for part in folder_path.parts)
    if is_mobile_group and special_path is not None:
        final_files = [f for f in final_files if f is not None and Path(f).resolve() != special_path.resolve()]
        if final_files:
            mid_idx = len(final_files) // 2
            final_files.insert(min(mid_idx + 1, len(final_files)), str(special_path))
        else:
            final_files.insert(0, str(special_path))
        final_files.append(str(special_path))
    final_files = [f for f in final_files if f is not None]
    target_zones, excluded_zones = load_click_zones(folder_path)
    merged, pause_info, time_cursor = [], {"inter_file_pauses": [], "intra_file_pauses": []}, 0
    per_file_event_ms, per_file_inter_ms = {}, {}

    for idx, fpath in enumerate(final_files):
        if fpath is None:
            continue
        fpath_obj = Path(fpath)
        is_special = special_path is not None and fpath_obj.resolve() == special_path.resolve()
        evs = load_json_events(fpath_obj)
        zb_evs, file_duration_ms = zero_base_events(evs)

        if not is_special:
            is_desktop = "deskt" in str(folder_path).lower()
            exemption_config = exemption_config or {"exempted_folders": set(), "disable_intra_pauses": False, "disable_afk": False}
            is_exempted = exemption_config["exempted_folders"] and is_folder_exempted(folder_path, exemption_config["exempted_folders"])
            
            if not is_desktop:
                zb_evs = add_micro_pauses(zb_evs, rng)
                zb_evs = add_reaction_variance(zb_evs, rng)
                zb_evs = add_mouse_jitter(zb_evs, rng, is_desktop=False, target_zones=target_zones, excluded_zones=excluded_zones)
                zb_evs, _ = add_time_of_day_fatigue(zb_evs, rng, is_exempted=is_exempted, max_pause_ms=0)
            else:
                zb_evs = add_desktop_mouse_paths(zb_evs, rng)
                zb_evs = add_micro_pauses(zb_evs, rng)
                zb_evs = add_reaction_variance(zb_evs, rng)
                zb_evs = add_mouse_jitter(zb_evs, rng, is_desktop=True, target_zones=target_zones, excluded_zones=excluded_zones)
                zb_evs, _ = add_time_of_day_fatigue(zb_evs, rng, is_exempted=is_exempted, max_pause_ms=0)
            
            zb_evs, file_duration_ms = zero_base_events(zb_evs)
            
            if is_exempted:
                if not exemption_config.get("disable_intra_pauses", False):
                    intra_evs, _ = insert_intra_pauses(zb_evs, rng, is_exempted=True, max_pause_s=within_max_s, max_num_pauses=within_max_pauses)
                else:
                    intra_evs = zb_evs
                
                if not exemption_config.get("disable_afk", False) and rng.random() < 0.5:
                    intra_evs = add_afk_pause(intra_evs, rng)
            else:
                intra_evs = zb_evs
                if rng.random() < 0.5:
                    intra_evs = add_afk_pause(intra_evs, rng)
        else:
            intra_evs = zb_evs

        per_file_event_ms[str(fpath_obj)] = intra_evs[-1]["Time"] if intra_evs else 0
        shifted = apply_shifts(intra_evs, time_cursor)
        merged.extend(shifted)
        time_cursor = shifted[-1]["Time"] if shifted else time_cursor

        if idx < len(final_files) - 1:
            exemption_config = exemption_config or {"exempted_folders": set(), "disable_intra_pauses": False, "disable_afk": False}
            is_exempted = exemption_config["exempted_folders"] and is_folder_exempted(folder_path, exemption_config["exempted_folders"])
            if is_exempted:
                pause_ms = rng.randint(0, int(between_max_s * 1000))
            else:
                pause_ms = rng.randint(1000, 12000)
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
        if f is None:
            continue
        fname = Path(f).name.lower()
        if fname.startswith("always first"):
            part_name = "first"
        elif fname.startswith("always last"):
            part_name = "last"
        else:
            part_name = part_from_filename(f)
        minutes = compute_minutes_from_ms(per_file_event_ms.get(str(f), 0) + per_file_inter_ms.get(str(f), 0))
        parts.append(f"{part_name}[{minutes}m]")

    letters = number_to_letters(version_num or 1)
    tag = ""
    if use_special_file == always_first_file and always_first_file is not None:
        tag = "FIRST"
    elif use_special_file == always_last_file and always_last_file is not None:
        tag = "LAST"
    
    tag_prefix = f"{tag} - " if tag else ""
    base_name = f"{tag_prefix}{letters}_{total_minutes}m= {' - '.join(parts)}"
    safe_name = ''.join(ch for ch in base_name if ch not in '/\\:*?"<>|')
    return f"{safe_name}.json", merged, [str(p) for p in final_files], pause_info, [str(p) for p in excluded], total_minutes

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", default="originals")
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--versions", type=int, default=26)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--exclude-count", type=int, default=10)
    parser.add_argument("--within-max-time", default="33")
    parser.add_argument("--within-max-pauses", type=int, default=2)
    parser.add_argument("--between-max-time", default="18")
    args = parser.parse_args()
    rng = random.Random(args.seed) if args.seed is not None else random.Random()
    input_root, output_parent = Path(args.input_dir), Path(args.output_dir)
    output_parent.mkdir(parents=True, exist_ok=True)
    counter = int(os.environ.get("BUNDLE_SEQ", "").strip() or read_counter_file() or 1)
    if not os.environ.get("BUNDLE_SEQ"):
        write_counter_file(counter + 1)
    output_base_name, output_root = f"merged_bundle_{counter}", output_parent / f"merged_bundle_{counter}"
    output_root.mkdir(parents=True, exist_ok=True)
    folder_dirs = find_all_dirs_with_json(input_root)
    if not folder_dirs:
        print(f"No JSON files found in {input_root}", file=sys.stderr)
        return
    try:
        within_max_s = parse_time_to_seconds(args.within_max_time)
        between_max_s = parse_time_to_seconds(args.between_max_time)
    except Exception as e:
        print(f"ERROR parsing time: {e}", file=sys.stderr)
        return
    all_written_paths = []
    exemption_config = load_exemption_config()
    for folder in folder_dirs:
        files = find_json_files_in_dir(folder)
        if not files:
            continue
        try:
            rel_folder = folder.relative_to(input_root)
        except:
            rel_folder = Path(folder.name)
        out_folder_for_group = output_root / rel_folder
        out_folder_for_group.mkdir(parents=True, exist_ok=True)
        selector = NonRepeatingSelector(rng)
        for v in range(1, max(1, args.versions) + 1):
            merged_fname, merged_events, finals, pauses, excluded, total_minutes = generate_version_for_folder(files, rng, v, args.exclude_count, within_max_s, args.within_max_pauses, between_max_s, folder, input_root, selector, exemption_config)
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
            except:
                arcname = f"{output_base_name}/{fpath.name}"
            zf.write(fpath, arcname=arcname)
    print(f"DONE. Created: {zip_path}")

if __name__ == "__main__":
    main()
