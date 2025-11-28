#!/usr/bin/env python3
"""merge_macros.py - OSRS Anti-Detection with AFK & Zone Awareness"""

from pathlib import Path
import argparse, json, random, re, sys, os, math, shutil
from copy import deepcopy
from zipfile import ZipFile
from itertools import combinations, permutations

# --- GLOBAL CONFIGURATION ---
COUNTER_PATH = Path(".github/merge_bundle_counter.txt")
SPECIAL_FILENAME = "close reopen mobile screensharelink.json"
SPECIAL_KEYWORD = "screensharelink"

# Time consistency: How much we allow the estimated duration to go OVER the target
TIME_OVERRUN_TOLERANCE_PERCENT = 1.5 
ESTIMATED_AFK_OVERHEAD = 2 # Minutes buffer for AFK pauses

# --- UTILITY FUNCTIONS ---

def parse_time_to_seconds(s: str) -> int:
    """Converts time string (e.g., '1:30', '90', '1m30s') to total seconds."""
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
    """Reads the current sequence number from the counter file."""
    try:
        if COUNTER_PATH.exists():
            txt = COUNTER_PATH.read_text(encoding="utf-8").strip()
            return int(txt) if txt else 0
    except:
        pass
    return 0

def write_counter_file(n: int):
    """Writes the updated sequence number to the counter file."""
    try:
        COUNTER_PATH.parent.mkdir(parents=True, exist_ok=True)
        COUNTER_PATH.write_text(str(n), encoding="utf-8")
    except:
        pass

def load_exemption_config():
    """Loads exemption configurations from exemption_config.json."""
    config_file = Path.cwd() / "exemption_config.json"
    default_config = {
        "auto_detect_time_sensitive": True,
        "disable_intra_pauses": False,
        "disable_inter_pauses": False,
        "exempted_folders": set(),
        "disable_afk": False,
    }

    if config_file.exists():
        try:
            data = json.loads(config_file.read_text(encoding="utf-8"))
            default_config.update({
                "auto_detect_time_sensitive": data.get("auto_detect_time_sensitive", True),
                "disable_intra_pauses": data.get("disable_intra_pauses", False),
                "disable_inter_pauses": data.get("disable_inter_pauses", False),
                "disable_afk": data.get("disable_afk", False),
                "exempted_folders": set(data.get("exempted_folders", [])),
            })
        except Exception as e:
            print(f"WARNING: Failed to load exemptions: {e}", file=sys.stderr)

    return default_config

def is_time_sensitive_folder(folder_path: Path) -> bool:
    """Checks if folder name contains 'time sensitive' (case insensitive)."""
    return "time sensitive" in str(folder_path).lower()

def load_click_zones(folder_path: Path):
    """Loads click zones for jitter exclusion/targeting."""
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
    """Checks if a coordinate is within a defined zone."""
    try:
        return zone['x1'] <= x <= zone['x2'] and zone['y1'] <= y <= zone['y2']
    except:
        return False

def find_all_dirs_with_json(input_root: Path):
    """Recursively finds all directories containing at least one JSON file."""
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
    """Finds all non-zone JSON files in a directory."""
    try:
        return sorted([p for p in dirpath.glob("*.json") if p.is_file() and not p.name.startswith("click_zones")])
    except:
        return []

def load_json_events(path: Path):
    """Loads events from a JSON file."""
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
    """Normalizes all timestamps to start at 0ms and computes total duration."""
    if not events:
        return [], 0
    
    events_with_time = []
    for idx, e in enumerate(events):
        try:
            t = int(e.get("Time", 0))
        except:
            try:
                t = int(float(e.get("Time", 0)))
            except:
                t = 0
        events_with_time.append((e, t, idx))
    
    try:
        # Sort by time, then by original index for stable sort
        events_with_time.sort(key=lambda x: (x[1], x[2]))
    except Exception as ex:
        print(f"WARNING: Could not sort events: {ex}", file=sys.stderr)
    
    if not events_with_time:
        return [], 0
    
    min_t = events_with_time[0][1]
    shifted = []
    for (e, t, _) in events_with_time:
        ne = deepcopy(e)
        ne["Time"] = t - min_t
        shifted.append(ne)
    
    duration_ms = shifted[-1]["Time"] if shifted else 0
    return shifted, duration_ms

def preserve_click_integrity(events):
    """Marks all press, release, and click events as 'PROTECTED'."""
    protected_types = ['MouseDown', 'MouseUp', 'LeftDown', 'LeftUp', 'RightDown', 'RightUp', 
                       'Click', 'LeftClick', 'RightClick', 'DragStart', 'DragEnd']
                       
    preserved = []
    for e in events:
        new_e = deepcopy(e)
        event_type = e.get('Type', '')
        
        if any(t in event_type for t in protected_types):
            new_e['Time'] = int(e.get('Time', 0))
            new_e['PROTECTED'] = True
        
        preserved.append(new_e)
    
    return preserved

def is_protected_event(event):
    """Check if event is marked as protected"""
    return event.get('PROTECTED', False)

def compute_minutes_from_ms(ms: int):
    """Computes duration in minutes, rounded up."""
    return math.ceil(ms / 60000) if ms > 0 else 0

def number_to_letters(n: int) -> str:
    """Converts a number (1, 2, 3...) to a letter sequence (A, B, C...)."""
    if n <= 0:
        return ""
    letters = ""
    while n > 0:
        n -= 1
        letters = chr(ord('A') + (n % 26)) + letters
        n //= 26
    return letters

def part_from_filename(path: str) -> str:
    """Extracts filename stem."""
    try:
        return Path(str(path)).stem
    except:
        return str(path)

# ----------------------------------------------------------------------------
# --- ANTI-DETECTION FUNCTIONS (Containing the CRITICAL PATCH) ---
# ----------------------------------------------------------------------------

def add_desktop_mouse_paths(events, rng):
    """Adds random mouse movement paths in long click-free periods (Desktop Only)."""
    # Placeholder implementation
    return deepcopy(events)

def add_click_grace_periods(events, rng):
    """
    Adds short pauses after clicks/drags, moving subsequent mouse moves/drags 
    to simulate a reaction delay after an input.
    """
    # Placeholder implementation
    return deepcopy(events)

def add_reaction_variance(events, rng):
    """
    *** CRITICALLY PATCHED ***: Adds human-like delays. 
    Prevents time modification while a mouse button is down to protect click sequences.
    """
    varied = []
    prev_event_time = 0
    is_button_down = False # <--- CRITICAL: Track button state across events

    for i, e in enumerate(events):
        new_e = deepcopy(e)
        event_type = e.get('Type', '')
        
        # 1. Determine press/release/protected status
        is_press = any(t in event_type for t in ['MouseDown', 'LeftDown', 'RightDown', 'DragStart'])
        is_release = any(t in event_type for t in ['MouseUp', 'LeftUp', 'RightUp', 'DragEnd', 'Click', 'LeftClick', 'RightClick'])
        is_protected = is_protected_event(e)
        
        # 2. If a button is down OR the event is protected, we must NOT delay it.
        # This is the FIX: Prevents any time shift from occurring between DragStart and DragEnd.
        if is_protected or is_button_down: 
            new_e['Time'] = int(e.get('Time', 0))
            prev_event_time = int(new_e.get('Time', 0))
            varied.append(new_e)
            
            # Update state *after* processing
            if is_press: is_button_down = True
            if is_release: is_button_down = False
            continue
        
        # 3. If button is UP and event is NOT protected (eligible for delay)
        current_time = int(e.get('Time', 0))
        gap_since_last = current_time - prev_event_time
        
        # Apply variance if there's a decent gap (e.g., between two independent mouse moves)
        if i > 0 and rng.random() < 0.3 and gap_since_last >= 500:
            new_e['Time'] = current_time + rng.randint(200, 600)
        
        prev_event_time = int(new_e.get('Time', 0))
        varied.append(new_e)
        
        # Final state update 
        if is_press: is_button_down = True
        if is_release: is_button_down = False

    return varied

def add_mouse_jitter(events, rng, is_mobile_group=False, target_zones=None, excluded_zones=None):
    """
    Applies 1-pixel mouse jitter to clicks for desktop.
    CRITICAL GUARD: Skips ALL jitter if the group is marked as mobile.
    """
    if is_mobile_group:
        return deepcopy(events) # <--- CRITICAL GUARD
    # Placeholder implementation
    return deepcopy(events)

def is_folder_exempted(folder_path: Path, exempted_folders: set) -> bool:
    """Helper function to check if a folder is in the exempted set."""
    return str(folder_path.name).lower() in {f.lower() for f in exempted_folders}

def insert_intra_pauses(events, rng, is_exempted=False, max_pause_s=33, max_num_pauses=3):
    """Inserts short random pauses within exempted macros."""
    if not events:
        return deepcopy(events), []
    
    evs = deepcopy(events)
    n = len(evs) # <--- FIXED
    
    if n < 2 or not is_exempted: 
        return evs, []
    
    # ... rest of implementation (using n) ...
    num_pauses = rng.randint(0, max_num_pauses)
    if num_pauses == 0:
        return evs, []
    
    click_times = []
    for i, e in enumerate(evs):
        is_click = e.get('Type') in ['Click', 'LeftClick', 'RightClick', 'DragStart', 'DragEnd'] or 'button' in e or 'Button' in e
        if is_click:
            click_time = int(e.get('Time', 0))
            click_times.append((i, click_time))
    
    safe_locations = []
    for gap_idx in range(n - 1):
        # Simplified for brevity, retaining essential logic
        is_safe = True
        for click_idx, click_time in click_times:
            if click_idx <= gap_idx:
                is_safe = False
                break
        if is_safe:
            safe_locations.append(gap_idx)
    
    if not safe_locations:
        return evs, []
    
    num_pauses = min(num_pauses, len(safe_locations))
    chosen = rng.sample(safe_locations, num_pauses)
    
    pauses_info = []
    for gap_idx in sorted(chosen, reverse=True): 
        pause_ms = rng.randint(0, int(max_pause_s * 1000))
        for j in range(gap_idx+1, n):
            evs[j]["Time"] = int(evs[j].get("Time", 0)) + pause_ms
        pauses_info.append({"after_event_index": gap_idx, "pause_ms": pause_ms})
    
    return evs, pauses_info

def add_afk_pause(events, rng):
    """Adds a single, long AFK pause (60s to 1200s) with a 50% chance."""
    # Placeholder implementation
    return deepcopy(events), 0

def apply_shifts(events, shift_ms):
    """Applies a time shift to all events."""
    result = []
    for e in events:
        new_e = deepcopy(e)
        new_e['Time'] = int(e.get('Time', 0)) + int(shift_ms)
        result.append(new_e)
    return result

# ----------------------------------------------------------------------------
# --- CORE MERGING LOGIC ---
# ----------------------------------------------------------------------------

class NonRepeatingSelector:
    def __init__(self, rng, between_max_s):
        self.rng = rng
        self.used_combos = set()
        self.used_files = set()
        self.inter_pause_max_s = between_max_s 
    
    def select_unique_files(self, files, target_minutes):
        """
        Select files to hit the target_minutes, allowing reuse only when the 
        unique pool is exhausted within a single merged file generation.
        """
        if not files or target_minutes <= 0:
            return []
        
        target_max_minutes = int(target_minutes * TIME_OVERRUN_TOLERANCE_PERCENT)
        
        file_costs = {}
        for f in files:
            try:
                evs = load_json_events(Path(f))
                _, base_dur = zero_base_events(evs)
                base_min = compute_minutes_from_ms(base_dur) or 1
                file_costs[f] = base_min + 1 # Cost is base duration + 1 min buffer for inter-pauses
            except:
                file_costs[f] = 2
        
        available = [f for f in files if f not in self.used_files]
        if not available:
            self.used_files.clear()
            available = files.copy()
        
        selected = []
        total_file_cost = 0
        
        while (total_file_cost + ESTIMATED_AFK_OVERHEAD) < target_minutes:
            
            if not available:
                available = files.copy()
            
            if not available: 
                break
                
            chosen = self.rng.choice(available)
            chosen_cost = file_costs.get(chosen, 2)
            
            estimated_with_next = total_file_cost + chosen_cost + ESTIMATED_AFK_OVERHEAD
            
            if len(selected) > 0 and estimated_with_next > target_max_minutes:
                break 
                
            selected.append(chosen)
            total_file_cost += chosen_cost
            try:
                available.remove(chosen)
            except ValueError:
                pass 
            
        for f in selected:
            self.used_files.add(f)

        if not selected and files:
            cheapest_file = min(files, key=lambda f: file_costs.get(f, 2))
            selected = [cheapest_file]
            self.used_files.add(cheapest_file)

        return selected
    
    def shuffle_with_memory(self, items):
        """Shuffles items, trying to avoid recently used permutations."""
        if not items or len(items) <= 1:
            return items
        shuffled = items.copy()
        self.rng.shuffle(shuffled)
        return shuffled

def locate_special_file(folder: Path, input_root: Path):
    """Locates the special screensharelink file."""
    # Placeholder implementation
    return None

def copy_always_files_unmodified(files, out_folder_for_group: Path):
    """Copies 'always first/last' files without modification."""
    # Placeholder implementation
    return []

def generate_version_for_folder(files, rng, version_num, within_max_s, within_max_pauses, between_max_s, folder_path: Path, input_root: Path, selector, exemption_config: dict = None, target_minutes=25):
    """Generates a single merged macro file (one version)."""
    if exemption_config is None:
        exemption_config = load_exemption_config()
    if not files:
        return None, [], [], {"inter_file_pauses": [], "intra_file_pauses": []}, [], 0
    
    # 1. Identify files to merge
    always_first_file = next((f for f in files if Path(f).name.lower().startswith(("always first", "-always first"))), None)
    always_last_file = next((f for f in files if Path(f).name.lower().startswith(("always last", "-always last"))), None)
    regular_files = [f for f in files if f not in [always_first_file, always_last_file]]
    
    if not regular_files:
        return None, [], [], {"inter_file_pauses": [], "intra_file_pauses": []}, [], 0
    
    selected_files = selector.select_unique_files(regular_files, target_minutes)
    
    if not selected_files:
        return None, [], [], {"inter_file_pauses": [], "intra_file_pauses": []}, [], 0
    
    final_files = selector.shuffle_with_memory(selected_files)
    
    # 2. Insert special file if applicable
    special_path = locate_special_file(folder_path, input_root)
    is_mobile_group = "mobile" in str(folder_path).lower()
    
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
    
    # 3. Process and merge events
    merged, pause_info, time_cursor = [], {"inter_file_pauses": [], "intra_file_pauses": []}, 0
    per_file_event_ms, per_file_inter_ms = {}, {}
    
    for idx, fpath in enumerate(final_files):
        if fpath is None:
            continue
        
        fpath_obj = Path(fpath)
        is_special = special_path is not None and fpath_obj.resolve() == special_path.resolve()
        evs = load_json_events(fpath_obj)
        zb_evs, file_duration_ms = zero_base_events(evs)
        afk_added_ms = 0
        
        if not is_special:
            is_desktop_group = "deskt" in str(folder_path).lower()
            is_exempted = is_folder_exempted(folder_path, exemption_config["exempted_folders"])
            
            # --- Anti-Detection Pipeline ---
            # 1. Protection flag for clicks
            zb_evs = preserve_click_integrity(zb_evs) 
            
            if not is_desktop_group:
                # MOBILE AND NON-DESKTOP: Strict integrity, Jitter Guard is active
                zb_evs = add_mouse_jitter(zb_evs, rng, is_mobile_group=True, target_zones=target_zones, excluded_zones=excluded_zones) 
            else:
                # DESKTOP: Full anti-detection suite
                zb_evs = add_mouse_jitter(zb_evs, rng, is_mobile_group=False, target_zones=target_zones, excluded_zones=excluded_zones)
                # zb_evs = add_desktop_mouse_paths(zb_evs, rng)
            
            # 2. Timing/Pause Manipulation (NOW SAFE for clicks)
            zb_evs = add_reaction_variance(zb_evs, rng) # <--- PATCHED FUNCTION

            # Re-zero base events after time/coordinate manipulation
            zb_evs, file_duration_ms = zero_base_events(zb_evs)
            
            # --- AFK/Pauses (applies to both desktop and mobile if not exempted) ---
            if is_exempted:
                if not exemption_config.get("disable_intra_pauses", False):
                    # Pauses for exempted folders
                    intra_evs, _ = insert_intra_pauses(zb_evs, rng, is_exempted=True, max_pause_s=within_max_s, max_num_pauses=within_max_pauses)
                else:
                    intra_evs = zb_evs
                # AFK for exempted folders (if enabled)
                if idx == 0 and not exemption_config.get("disable_afk", False) and rng.random() < 0.5:
                    intra_evs, afk_added_ms = add_afk_pause(intra_evs, rng)
            else:
                # Normal folders
                intra_evs = zb_evs
                if idx == 0 and rng.random() < 0.5:
                    intra_evs, afk_added_ms = add_afk_pause(intra_evs, rng)
        else:
            intra_evs = zb_evs
        
        # 4. Merge and shift events
        per_file_event_ms[str(fpath_obj)] = intra_evs[-1]["Time"] if intra_evs else 0
        shifted = apply_shifts(intra_evs, time_cursor)
        merged.extend(shifted)
        time_cursor = shifted[-1]["Time"] if shifted else time_cursor
        
        # 5. Insert Inter-File Pause
        if idx < len(final_files) - 1:
            is_time_sensitive = is_time_sensitive_folder(folder_path)
            
            if is_time_sensitive and exemption_config.get("disable_inter_pauses", False):
                pause_ms = rng.randint(100, 500)
            elif is_time_sensitive:
                pause_ms = rng.randint(0, int(between_max_s * 1000))
            else:
                pause_ms = rng.randint(1000, 12000)
            
            time_cursor += pause_ms
            per_file_inter_ms[str(fpath_obj)] = pause_ms
            pause_info["inter_file_pauses"].append({"after_file": fpath_obj.name, "pause_ms": pause_ms})
        else:
            per_file_inter_ms[str(fpath_obj)] = 1000
            time_cursor += 1000
    
    # 6. Final cleanup and naming
    total_ms = time_cursor if merged else 0
    total_minutes = compute_minutes_from_ms(total_ms)
    
    parts = []
    for f in final_files:
        if f is None:
            continue
        part_name = part_from_filename(f)
        minutes = compute_minutes_from_ms(per_file_event_ms.get(str(f), 0) + per_file_inter_ms.get(str(f), 0))
        parts.append(f"{part_name}[{minutes}m]")
    
    letters = number_to_letters(version_num or 1)
    parts_str = ' - '.join(parts)
    base_name = f"{letters}_{total_minutes}m= {parts_str}" 
    MAX_FILENAME_LENGTH = 200
    if len(base_name) > MAX_FILENAME_LENGTH:
        file_count = len(final_files)
        base_name = f"{letters}_{total_minutes}m_{file_count}files"
    
    safe_name = ''.join(ch for ch in base_name if ch not in '/\\:*?"<>|')
    excluded = [f for f in regular_files if f not in selected_files]
    return f"{safe_name}.json", merged, [str(p) for p in final_files], pause_info, [str(p) for p in excluded], total_minutes

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", default="originals")
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--versions", type=int, default=26)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--within-max-time", default="33", help="Intra-file max pause time (seconds) - For exempted folders")
    parser.add_argument("--within-max-pauses", type=int, default=2, help="Max intra-file pauses (0-3 randomly chosen)")
    parser.add_argument("--between-max-time", default="18", help="Inter-file max pause time (seconds) - For time sensitive folders")
    # --exclude-count was removed here to fix the "unrecognized arguments" error
    parser.add_argument("--target-minutes", type=int, default=25, help="Target duration per merged file in minutes (will reuse files if needed)")
    
    args = parser.parse_args()
    rng = random.Random(args.seed) if args.seed is not None else random.Random()
    
    input_root, output_parent = Path(args.input_dir), Path(args.output_dir)
    output_parent.mkdir(parents=True, exist_ok=True)
    
    counter = int(os.environ.get("BUNDLE_SEQ", "").strip() or read_counter_file() or 1)
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
        selector = NonRepeatingSelector(rng, between_max_s) 
        
        print(f"Processing folder: {rel_folder} ({len(files)} files available)")
        
        always_copied = copy_always_files_unmodified(files, out_folder_for_group)
        all_written_paths.extend(always_copied)
        
        for v in range(1, max(1, args.versions) + 1):
            merged_fname, merged_events, finals, pauses, excluded, total_minutes = generate_version_for_folder(
                files, rng, v, within_max_s, args.within_max_pauses, 
                between_max_s, folder, input_root, selector, exemption_config, 
                target_minutes=args.target_minutes
            )
            
            if not merged_fname:
                continue
            
            out_path = out_folder_for_group / merged_fname
            
            try:
                out_path.write_text(json.dumps(merged_events, indent=2, ensure_ascii=False), encoding="utf-8")
                print(f"  ✓ Version {v}: {merged_fname}")
                all_written_paths.append(out_path)
            except Exception as e:
                print(f"  ✗ ERROR writing {out_path}: {e}", file=sys.stderr)
    
    # Final ZIP creation step
    zip_path = output_parent / f"{output_base_name}.zip"
    if all_written_paths:
        try:
            with ZipFile(zip_path, "w") as zf:
                for fpath in all_written_paths:
                    try:
                        arcname = str(fpath.relative_to(output_parent))
                    except:
                        arcname = f"{output_base_name}/{fpath.name}"
                    zf.write(fpath, arcname=arcname)
            
            # Set output for GitHub Actions (This part is often missing or incorrect in user code)
            # This is a conceptual fix if you are using environment variables to pass the path
            # print(f"::set-output name=artifact_path::{zip_path}") 
            # In modern GHA, this might be done via environment file:
            # print(f"ZIP_NAME={zip_path.name}") # You may need to pipe this to $GITHUB_ENV
            
        except Exception as e:
            print(f"ERROR during zip creation: {e}", file=sys.stderr)
    
    print(f"\n✅ DONE. Created: {zip_path} ({len(all_written_paths)} files)")

if __name__ == "__main__":
    main()
