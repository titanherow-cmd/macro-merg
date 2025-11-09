#!/usr/bin/env python3

"""
merge_macros.py (Anti-Detection Enhanced)
- Minimum pause times set to 0 seconds (both intra and inter)
- UI defaults: 18s for inter-file, 33s for intra-file, 26 versions, 2 max pauses, 10 exclude
- Pause selection uses millisecond precision
- Non-repeating randomization: exhausts all possibilities before allowing repeats
- Preserves event integrity for Free Macro playback
- ANTI-DETECTION FEATURES:
  * Mouse jitter (±1-3 pixels per click)
  * Occasional right-click misclicks (2-5% chance before real action)
  * Micro-pauses (50-250ms hesitations before actions)
  * Mouse drift (cursor wanders between actions)
  * Reaction time variance (200-600ms delays after visual triggers)
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

# --------- constants ---------
COUNTER_PATH = Path(".github/merge_bundle_counter.txt")
SPECIAL_FILENAME = "close reopen mobile screensharelink.json"
SPECIAL_KEYWORD = "screensharelink"
BETWEEN_MAX_PAUSES = 1  # Hardcoded: always 1 pause between files

# --------- Anti-detection helpers ---------
def add_mouse_jitter(events, rng):
    """
    Add random ±1 pixel offset to X/Y coordinates for CLICKS ONLY.
    Clicks one of the 8 surrounding pixels around the target (or stays on target).
    
    CRITICAL: Only applies to actual click events, NOT mouse movements.
    This ensures cursor moves to the right place, then clicks with tiny variance.
    
    Possible offsets: (-1,-1), (-1,0), (-1,1), (0,-1), (0,0), (0,1), (1,-1), (1,0), (1,1)
    """
    jittered = []
    for e in deepcopy(events):
        # Only apply jitter to CLICK events, not mouse movements
        is_click = (e.get('Type') in ['Click', 'LeftClick', 'RightClick'] or 
                   'button' in e or 'Button' in e)
        
        if is_click and 'X' in e and 'Y' in e and e['X'] is not None and e['Y'] is not None:
            try:
                # Choose one of the 8 surrounding pixels (or center)
                offset_x = rng.choice([-1, 0, 1])
                offset_y = rng.choice([-1, 0, 1])
                e['X'] = int(e['X']) + offset_x
                e['Y'] = int(e['Y']) + offset_y
            except:
                pass  # If X/Y aren't numbers, skip jitter
        jittered.append(e)
    return jittered

def add_occasional_misclicks(events, rng, misclick_chance=0.035):
    """
    2-5% chance to insert a RIGHT-CLICK misclick before certain LEFT-CLICK actions.
    Simulates human error - we sometimes miss and have to correct.
    
    CRITICAL SAFETY:
    - Misclick is ALWAYS a RIGHT-CLICK (never left-click to avoid wrong actions)
    - Misclick happens 5-15 pixels AWAY from target (not at target)
    - Happens 150-350ms BEFORE the real click
    - After misclick, cursor moves BACK toward target, then does real click
    
    This simulates: miss slightly → realize mistake → move back → correct click
    """
    enhanced = []
    for i, e in enumerate(deepcopy(events)):
        # Check if this is a LEFT-CLICK event only
        is_left_click = (e.get('Type') == 'Click' or 
                        e.get('Type') == 'LeftClick' or
                        e.get('button') == 'left' or 
                        e.get('Button') == 'Left')
        
        if is_left_click and rng.random() < misclick_chance:
            if 'X' in e and 'Y' in e and e['X'] is not None and e['Y'] is not None:
                try:
                    target_x = int(e['X'])
                    target_y = int(e['Y'])
                    
                    # Misclick position: 5-15 pixels away in random direction
                    offset_distance = rng.randint(5, 15)
                    angle = rng.uniform(0, 6.28318)  # Random angle in radians
                    misclick_x = target_x + int(offset_distance * rng.choice([-1, 1]))
                    misclick_y = target_y + int(offset_distance * rng.choice([-1, 1]))
                    
                    # Create RIGHT-CLICK misclick event
                    misclick_time = int(e.get('Time', 0)) - rng.randint(150, 350)
                    misclick = {
                        'Time': misclick_time,
                        'Type': 'RightClick',
                        'X': misclick_x,
                        'Y': misclick_y
                    }
                    if 'button' in e:
                        misclick['button'] = 'right'
                    if 'Button' in e:
                        misclick['Button'] = 'Right'
                    
                    # Add cursor movement BACK toward target (correction movement)
                    # This happens between misclick and real click
                    correction_time = misclick_time + rng.randint(50, 120)
                    correction_move = {
                        'Time': correction_time,
                        'Type': 'MouseMove',
                        'X': target_x + rng.randint(-2, 2),  # Move near target
                        'Y': target_y + rng.randint(-2, 2)
                    }
                    
                    enhanced.append(misclick)
                    enhanced.append(correction_move)
                except:
                    pass
        
        enhanced.append(e)
    
    return enhanced

def add_micro_pauses(events, rng, micropause_chance=0.15):
    """
    15% chance to add tiny hesitation (50-250ms) before actions.
    Simulates human thinking/reaction time - we don't act instantly.
    """
    paused = []
    for e in deepcopy(events):
        if rng.random() < micropause_chance:
            hesitation = rng.randint(50, 250)
            e['Time'] = int(e.get('Time', 0)) + hesitation
        paused.append(e)
    return paused

def add_mouse_drift(events, rng, drift_chance=0.08):
    """
    8% chance to add "mouse wander" events BETWEEN actions (not during clicks).
    Humans don't keep cursor perfectly still - it drifts naturally.
    
    CRITICAL: Only adds drift AFTER non-click events (like delays/waits).
    Never adds drift right before a click - that would move cursor away from target.
    
    Adds 1-3 small mouse movements within ±20 pixels of current position.
    """
    events_copy = deepcopy(events)
    drifted = []
    
    for i, e in enumerate(events_copy):
        drifted.append(e)
        
        # Only add drift if:
        # 1. Not the first event
        # 2. Current event is NOT a click (don't drift after clicks)
        # 3. Next event (if exists) is NOT a click (don't drift before clicks)
        is_current_click = (e.get('Type') in ['Click', 'LeftClick', 'RightClick'] or 
                          'button' in e or 'Button' in e)
        
        # Check if next event is a click
        is_next_click = False
        if i + 1 < len(events_copy):
            next_e = events_copy[i + 1]
            is_next_click = (next_e.get('Type') in ['Click', 'LeftClick', 'RightClick'] or 
                           'button' in next_e or 'Button' in next_e)
        
        # Only add drift if current and next are both NOT clicks
        if i > 0 and not is_current_click and not is_next_click and rng.random() < drift_chance:
            if 'X' in e and 'Y' in e and e['X'] is not None and e['Y'] is not None:
                try:
                    current_x = int(e['X'])
                    current_y = int(e['Y'])
                    
                    # Create 1-3 small mouse movements (idle wandering)
                    drift_count = rng.randint(1, 3)
                    base_time = int(e.get('Time', 0))
                    
                    for d in range(drift_count):
                        drift_event = {
                            'Time': base_time + (d + 1) * rng.randint(80, 200),
                            'Type': 'MouseMove',
                            'X': current_x + rng.randint(-20, 20),
                            'Y': current_y + rng.randint(-20, 20)
                        }
                        drifted.append(drift_event)
                except (ValueError, TypeError):
                    pass
    
    return drifted

def add_reaction_variance(events, rng):
    """
    Add human reaction time (200-600ms) to events that would require visual response.
    Humans don't react instantly to screen changes - we need time to see and process.
    
    Applied to clicks that likely respond to game state changes.
    """
    varied = []
    for i, e in enumerate(deepcopy(events)):
        # If this is a click and not the first event, add reaction delay
        is_click = (e.get('Type') in ['Click', 'RightClick'] or 
                   'button' in e or 'Button' in e)
        
        if is_click and i > 0 and rng.random() < 0.3:  # 30% of clicks get reaction delay
            reaction_time = rng.randint(200, 600)
            e['Time'] = int(e.get('Time', 0)) + reaction_time
        
        varied.append(e)
    
    return varied

def add_time_of_day_fatigue(events, rng):
    """
    Simulate human fatigue based on time of day.
    - Late night (11pm-5am): 15-30% slower, more mistakes
    - Peak hours (6pm-10pm): normal speed
    - Morning (6am-12pm): slightly faster, more focused
    - Afternoon (1pm-5pm): normal to slightly slower
    
    Also considers weekend vs weekday (weekends = more relaxed timing).
    """
    from datetime import datetime
    
    now = datetime.now()
    hour = now.hour
    is_weekend = now.weekday() >= 5  # Saturday=5, Sunday=6
    
    # Determine fatigue multiplier based on time
    if 23 <= hour or hour < 5:  # Late night (11pm-5am)
        fatigue_min, fatigue_max = 1.15, 1.30  # 15-30% slower
        mistake_increase = 0.02  # 2% more misclicks
    elif 6 <= hour < 12:  # Morning (6am-12pm)
        fatigue_min, fatigue_max = 0.95, 1.05  # Slightly faster
        mistake_increase = -0.01  # 1% fewer mistakes (more focused)
    elif 18 <= hour < 23:  # Peak evening (6pm-11pm)
        fatigue_min, fatigue_max = 1.0, 1.1  # Normal
        mistake_increase = 0.0
    else:  # Afternoon (12pm-6pm)
        fatigue_min, fatigue_max = 1.05, 1.15  # Slightly slower
        mistake_increase = 0.005
    
    # Weekend adjustment - more relaxed
    if is_weekend:
        fatigue_min += 0.05
        fatigue_max += 0.1
    
    # Apply fatigue to all events
    fatigue_multiplier = rng.uniform(fatigue_min, fatigue_max)
    fatigued = []
    
    for e in deepcopy(events):
        # Slow down actions based on fatigue
        original_time = int(e.get('Time', 0))
        e['Time'] = int(original_time * fatigue_multiplier)
        fatigued.append(e)
    
    return fatigued, mistake_increase

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
    Return events sorted by Time, shifted so earliest Time is 0, and the event duration in ms.
    Preserves ALL original event fields - only modifies Time values.
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
    
    # Sort by time - this ensures events play in chronological order
    # Free Macro expects events in time order, so this is correct
    try:
        events_with_time.sort(key=lambda x: x[1])
    except Exception as e:
        print(f"WARNING: Could not sort events, proceeding without sorting. Error: {e}", file=sys.stderr)
    
    if not events_with_time:
        return [], 0
    
    min_t = events_with_time[0][1]
    shifted = []
    for (original_event, original_time) in events_with_time:
        ne = deepcopy(original_event)  # Preserve all fields
        ne["Time"] = original_time - min_t  # Only shift time
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
    """
    Return events with inserted intra pauses and a list of the pauses (ms).
    Randomly choose 1 to max_pauses number of pauses to insert.
    Preserves ALL event fields, only modifies Time values.
    """
    if not events or max_pauses <= 0:
        return deepcopy(events), []
    
    evs = deepcopy(events)
    n = len(evs)
    if n < 2:
        return evs, []
    
    # Randomly choose between 1 and max_pauses (inclusive)
    k = rng.randint(1, min(max_pauses, n-1))
    if k == 0:
        return evs, []
    
    chosen = rng.sample(range(n-1), k)
    pauses_info = []
    
    for gap_idx in sorted(chosen):
        # Random pause with millisecond precision
        pause_ms = rng.randint(min_s * 1000, max_s * 1000)
        
        # shift subsequent events - only modify Time field
        for j in range(gap_idx+1, n):
            evs[j]["Time"] = int(evs[j].get("Time", 0)) + pause_ms
        
        pauses_info.append({"after_event_index": gap_idx, "pause_ms": pause_ms})
    
    return evs, pauses_info

def apply_shifts(events, shift_ms):
    """Preserve ALL keys but update Time by adding shift_ms."""
    shifted = []
    for e in events:
        ne = deepcopy(e)  # Preserve all original fields
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

# --------- Non-repeating randomization helpers ---------
class NonRepeatingSelector:
    """
    Ensures all combinations are exhausted before repeating.
    Generates permutations/combinations on-the-fly to avoid memory issues.
    """
    def __init__(self, rng):
        self.rng = rng
        self.used_combos = set()
        self.all_exhausted = False
    
    def select_files(self, files, exclude_count):
        """
        Select which files to include (non-repeating).
        Returns tuple of (included_files, excluded_files).
        """
        if not files:
            return [], []
        
        n = len(files)
        max_exclude = min(exclude_count, max(0, n - 1))
        
        # Generate all possible exclusion combinations
        file_indices = list(range(n))
        all_possible = []
        
        for exclude_k in range(0, max_exclude + 1):
            for combo in combinations(file_indices, exclude_k):
                all_possible.append(frozenset(combo))
        
        # Filter out used combinations
        available = [c for c in all_possible if c not in self.used_combos]
        
        # If all exhausted, reset
        if not available:
            self.used_combos.clear()
            available = all_possible
        
        # Pick one
        chosen_exclude_indices = self.rng.choice(available)
        self.used_combos.add(chosen_exclude_indices)
        
        excluded = [files[i] for i in chosen_exclude_indices]
        included = [files[i] for i in file_indices if i not in chosen_exclude_indices]
        
        return included if included else files.copy(), excluded
    
    def shuffle_with_memory(self, items):
        """
        Shuffle items without repeating previous shuffles until all exhausted.
        For large item counts, falls back to regular shuffle after threshold.
        """
        if not items or len(items) <= 1:
            return items
        
        # For large lists, permutations are too many - use regular shuffle
        if len(items) > 8:
            shuffled = items.copy()
            self.rng.shuffle(shuffled)
            return shuffled
        
        # Convert to tuple for hashing
        items_tuple = tuple(items)
        
        # Generate all permutations (memory intensive for n>8)
        from itertools import permutations as iter_perms
        all_perms = [perm for perm in iter_perms(items_tuple)]
        
        # Filter unused
        available = [p for p in all_perms if p not in self.used_combos]
        
        # Reset if exhausted
        if not available:
            self.used_combos.clear()
            available = all_perms
        
        # Pick one
        chosen = self.rng.choice(available)
        self.used_combos.add(chosen)
        
        return list(chosen)

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
                                input_root: Path,
                                selector: NonRepeatingSelector):
    """Merge provided list of files (all from same folder) with non-repeating randomization."""
    if not files:
        return None, [], [], {"inter_file_pauses":[], "intra_file_pauses":[]}, [], 0
    
    # Use non-repeating selector for file selection
    included, excluded = selector.select_files(files, exclude_count)
    
    if not included:
        included = files.copy()
    
    # NO DUPLICATION - each file used only once
    # Use non-repeating shuffle to ensure orders don't repeat
    final_files = selector.shuffle_with_memory(included)
    
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
        
        # Apply anti-detection features (preserve special file behavior)
        if not is_special:
            # ANTI-DETECTION CONTROLS - Set to False to disable any feature
            ENABLE_TIME_FATIGUE = False
            ENABLE_MOUSE_JITTER = False
            ENABLE_MISCLICKS = False
            ENABLE_MICRO_PAUSES = False
            ENABLE_MOUSE_DRIFT = True
            ENABLE_REACTION_VARIANCE = False
            
            # 1. Time-of-day fatigue - adjusts ALL timings proportionally
            if ENABLE_TIME_FATIGUE:
                zb_evs, extra_mistake_chance = add_time_of_day_fatigue(zb_evs, rng)
            else:
                extra_mistake_chance = 0
            
            # 2. Mouse jitter - ONLY modifies click coordinates by ±1 pixel
            if ENABLE_MOUSE_JITTER:
                zb_evs = add_mouse_jitter(zb_evs, rng)
            
            # 3. Occasional right-click misclicks - INSERTS events before left-clicks
            if ENABLE_MISCLICKS:
                base_misclick_chance = 0.035 + extra_mistake_chance
                zb_evs = add_occasional_misclicks(zb_evs, rng, base_misclick_chance)
            
            # 4. Micro-pauses - adds small time delays
            if ENABLE_MICRO_PAUSES:
                zb_evs = add_micro_pauses(zb_evs, rng)
            
            # 5. Mouse drift - INSERTS mouse movements between non-click events
            if ENABLE_MOUSE_DRIFT:
                zb_evs = add_mouse_drift(zb_evs, rng)
            
            # 6. Reaction time variance - adds delays to some clicks
            if ENABLE_REACTION_VARIANCE:
                zb_evs = add_reaction_variance(zb_evs, rng)
            
            # Re-sort by Time after insertions to ensure correct playback order
            zb_evs, file_duration_ms = zero_base_events(zb_evs)
        
        # For special file: no intra pauses, no anti-detection
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
        
        # Pause logic - hardcoded 1 pause between files
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
    p.add_argument("--versions", type=int, default=26)
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--exclude-count", type=int, default=10)
    p.add_argument("--within-max-time", type=str, default="33")
    p.add_argument("--within-max-pauses", type=int, default=2)
    p.add_argument("--between-max-time", type=str, default="18")
    
    # legacy aliases
    p.add_argument("--intra-file-max", type=str, dest="within_max_time", help=argparse.SUPPRESS)
    p.add_argument("--intra-file-max-pauses", type=int, dest="within_max_pauses", help=argparse.SUPPRESS)
    p.add_argument("--inter-file-max", type=str, dest="between_max_time", help=argparse.SUPPRESS)
    
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
        
        # Create non-repeating selector per folder group
        selector = NonRepeatingSelector(rng)
        
        for v in range(1, max(1, args.versions) + 1):
            merged_fname, merged_events, finals, pauses, excluded, total_minutes = generate_version_for_folder(
                files, rng, v,
                args.exclude_count,
                within_min_s, within_max_s, getattr(args, "within_max_pauses"),
                between_min_s, between_max_s,
                folder, input_root,
                selector
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
