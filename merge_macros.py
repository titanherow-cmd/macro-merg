#!/usr/bin/env python3
"""
merge_macros.py - v3.23.0
- Alphabetical naming: Raw (A,B,C) -> Ineff (D,E,F) -> Normal (G,H,I...)
- DROP ONLY insertion for Mining folders
- Working whitelist + random file queue
"""

import argparse, json, random, re, sys, os, math, shutil
from pathlib import Path

# Script version
VERSION = "v3.24.9"


def load_folder_whitelist(root_path: Path) -> dict:
    """
    Load folder whitelist from 'specific folders to include for merge' file.
    Returns dict with 'folders' and 'parent_folders' sets (None = process all).
    
    Parent folders (Desktop, Mobile) include ALL their subfolders.
    Specific folders (1-Mining) include only that folder.
    """
    possible_names = [
        "specific folders to include for merge",
        "specific folders to include for merge.txt",
        "SPECIFIC FOLDERS TO INCLUDE FOR MERGE",
        "SPECIFIC FOLDERS TO INCLUDE FOR MERGE.TXT",
    ]
    
    whitelist_file = None
    for name in possible_names:
        test_file = root_path / name
        if test_file.exists():
            whitelist_file = test_file
            break
    
    if not whitelist_file:
        return None
    
    try:
        folders = set()
        parent_folders = set()
        
        with open(whitelist_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                line_lower = line.lower()
                if line_lower in ['desktop', 'mobile']:
                    parent_folders.add(line_lower)
                else:
                    folders.add(line_lower)
        
        if not folders and not parent_folders:
            print(f"⚠️ Whitelist file is empty: {whitelist_file}")
            return None
        
        print(f"✓ Loaded whitelist from: {whitelist_file}")
        if parent_folders:
            print(f"  Including ALL subfolders under:")
            for pf in sorted(parent_folders):
                print(f"    - {pf}/* (all subfolders)")
        if folders:
            print(f"  Including specific folders:")
            for folder in sorted(folders):
                print(f"    - {folder}")
        
        return {'folders': folders, 'parent_folders': parent_folders}
    except Exception as e:
        print(f"⚠️ Error reading whitelist: {e}")
        return None


def should_process_folder(folder_path: Path, originals_root: Path, whitelist: dict) -> bool:
    """Check if folder should be processed based on whitelist"""
    if whitelist is None:
        return True
    
    folder_name = folder_path.name.lower()
    
    # Check specific folder match
    if folder_name in whitelist["folders"]:
        return True
    
    # Check parent folder match AND check if any ancestor folder is in whitelist
    try:
        rel_path = folder_path.relative_to(originals_root)
        for part in rel_path.parts:
            part_lower = part.lower()
            # Check if this part matches parent_folders (Desktop, Mobile)
            if part_lower in whitelist["parent_folders"]:
                return True
            # Check if this part matches regular folders (Mining, etc.)
            if part_lower in whitelist["folders"]:
                return True
    except ValueError:
        pass
    
    return False



# Chat inserts are loaded from 'chat inserts' folder at runtime
def load_json_events(path: Path):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        events = []
        if isinstance(data, dict):
            found_list = None
            for k in ("events", "items", "entries", "records"):
                if k in data and isinstance(data[k], list):
                    found_list = data[k]
                    break
            events = found_list if found_list is not None else ([data] if "Time" in data else [])
        elif isinstance(data, list):
            events = data
        
        cleaned = []
        for e in events:
            if isinstance(e, list) and len(e) > 0: e = e[0]
            if isinstance(e, dict) and "Time" in e: cleaned.append(e)
        return cleaned
    except Exception:
        return []

def get_file_duration_ms(path: Path) -> int:
    events = load_json_events(path)
    if not events: return 0
    try:
        times = [int(e.get("Time", 0)) for e in events]
        return max(times) - min(times)
    except: return 0

def format_ms_precise(ms: int) -> str:
    ts = int(round(ms / 1000))
    m, s = ts // 60, ts % 60
    return f"{m}m {s}s" if m > 0 else f"{s}s"

def clean_identity(name: str) -> str:
    return re.sub(r'(\s*-\s*Copy(\s*\(\d+\))?)|(\s*\(\d+\))', '', name, flags=re.IGNORECASE).strip().lower()


def find_drop_only_files(folder_path: Path, all_files: list) -> list:
    """
    Find all DROP ONLY files in Mining folders and subfolders.
    Checks if "mining" appears ANYWHERE in the folder path.
    Returns list of file paths.
    """
    # Check if "mining" appears anywhere in the full path
    full_path_str = str(folder_path).lower()
    if "mining" not in full_path_str:
        return []
    
    drop_only_files = []
    
    # Search through all files for DROP ONLY pattern
    for file_path in all_files:
        filename = file_path.name.lower()
        # Match: "drop only", "drop only1", "drop only 1", "drop only - copy", etc.
        if "drop only" in filename:
            drop_only_files.append(file_path)
    
    return drop_only_files


def extract_folder_number(folder_name: str) -> int:
    """
    Extract number from folder name like '1-Mining' or '23-Fishing'.
    Returns the number, or 0 if not found.
    """
    match = re.match(r'^(\d+)-', folder_name)
    if match:
        return int(match.group(1))
    return 0

def is_always_first_or_last_file(filename: str) -> bool:
    """
    Check if a file should be treated as "always first" or "always last".
    Checks if these phrases appear ANYWHERE in the filename (case-insensitive).
    """
    filename_lower = filename.lower()
    patterns = ["always first", "always last", "alwaysfirst", "alwayslast"]
    return any(pattern in filename_lower for pattern in patterns)

def is_in_drag_sequence(events, index):
    """
    Check if the given index is inside a drag sequence (between DragStart and DragEnd).
    Returns True if we're in the middle of a drag.
    """
    drag_started = False
    for j in range(index, -1, -1):
        event_type = events[j].get("Type", "")
        if event_type == "DragEnd":
            return False
        elif event_type == "DragStart":
            drag_started = True
            break
    
    if not drag_started:
        return False
    
    for j in range(index + 1, len(events)):
        event_type = events[j].get("Type", "")
        if event_type == "DragEnd":
            return True
        elif event_type == "DragStart":
            return False
    
    return False

def generate_human_path(start_x, start_y, end_x, end_y, duration_ms, rng):
    """
    Generate a human-like path with variable speed, wobbles, and imperfections.
    
    Returns: List of (time_ms, x, y) tuples with realistic timing and positions.
    """
    if duration_ms < 100:
        return [(0, end_x, end_y)]
    
    path = []
    
    # Calculate distance
    dx = end_x - start_x
    dy = end_y - start_y
    distance = math.sqrt(dx**2 + dy**2)
    
    if distance < 5:
        return [(0, end_x, end_y)]
    
    # Determine speed profile (variable speeds make it human)
    speed_profile = rng.choice(['fast_start', 'slow_start', 'medium', 'hesitant'])
    
    # Number of steps based on distance and duration
    num_steps = max(3, min(int(distance / 15), int(duration_ms / 50)))
    
    # Add control points for curve (not perfect bezier)
    num_control = rng.randint(1, 3)
    control_points = []
    for _ in range(num_control):
        # Offset perpendicular to main direction
        offset = rng.uniform(-0.3, 0.3) * distance
        t = rng.uniform(0.2, 0.8)
        ctrl_x = start_x + dx * t + (-dy / (distance + 1)) * offset
        ctrl_y = start_y + dy * t + (dx / (distance + 1)) * offset
        control_points.append((ctrl_x, ctrl_y, t))
    
    control_points.sort(key=lambda p: p[2])  # Sort by t position
    
    current_time = 0
    
    for step in range(num_steps + 1):
        # Non-linear time progression based on speed profile
        t_raw = step / num_steps
        
        if speed_profile == 'fast_start':
            # Fast at start, slow at end
            t = 1 - (1 - t_raw) ** 2
        elif speed_profile == 'slow_start':
            # Slow at start, fast at end
            t = t_raw ** 2
        elif speed_profile == 'hesitant':
            # Slow-fast-slow with micro-pauses
            t = 0.5 * (1 - math.cos(t_raw * math.pi))
        else:  # medium
            # Slight ease in/out
            t = 0.5 * (1 - math.cos(t_raw * math.pi))
        
        # Calculate position using control points (imperfect curve)
        if not control_points:
            # Simple interpolation with wobble
            x = start_x + dx * t
            y = start_y + dy * t
        else:
            # Multi-segment curve through control points
            x, y = start_x, start_y
            for i, (ctrl_x, ctrl_y, ctrl_t) in enumerate(control_points):
                if t <= ctrl_t:
                    segment_t = t / ctrl_t if ctrl_t > 0 else 0
                    x = start_x + (ctrl_x - start_x) * segment_t
                    y = start_y + (ctrl_y - start_y) * segment_t
                    break
                else:
                    if i == len(control_points) - 1:
                        # Last segment
                        segment_t = (t - ctrl_t) / (1 - ctrl_t) if (1 - ctrl_t) > 0 else 0
                        x = ctrl_x + (end_x - ctrl_x) * segment_t
                        y = ctrl_y + (end_y - ctrl_y) * segment_t
                    else:
                        start_x, start_y = ctrl_x, ctrl_y
        
        # Add random wobble (humans don't move in perfect lines)
        wobble_amount = rng.uniform(1, 5) if step > 0 and step < num_steps else 0
        x += rng.uniform(-wobble_amount, wobble_amount)
        y += rng.uniform(-wobble_amount, wobble_amount)
        
        # Add occasional micro-corrections (overshoot and correct)
        if step > 0 and step < num_steps and rng.random() < 0.15:
            overshoot = rng.uniform(5, 15)
            direction = 1 if rng.random() < 0.5 else -1
            x += direction * overshoot * (dx / (distance + 1))
            y += direction * overshoot * (dy / (distance + 1))
        
        # Keep within bounds
        x = max(100, min(1800, int(x)))
        y = max(100, min(1000, int(y)))
        
        # Calculate time with variable speed
        time_progress = t
        
        # Add micro-pauses (humans sometimes pause mid-movement)
        if step > 0 and step < num_steps and rng.random() < 0.1:
            pause = rng.randint(30, 100)
            current_time += pause
        
        step_time = int(time_progress * duration_ms)
        current_time = max(current_time, step_time)  # Ensure monotonic
        
        path.append((current_time, x, y))
    
    return path

def add_pre_click_jitter(events: list, rng: random.Random) -> tuple:
    """
    Add realistic pre-move jitter: before a random 20-45% of ALL mouse movements,
    add 2-3 micro-movements around the target (Â±1-3px), then snap to exact position.
    The percentage is randomly chosen per file (non-rounded).
    Returns (events_with_jitter, jitter_count, total_moves, jitter_percentage).
    """
    if not events or len(events) < 2:
        return events, 0, 0, 0.0
    
    # Randomly choose jitter percentage for this file (20-45%, non-rounded)
    jitter_percentage = rng.uniform(0.20, 0.45)
    
    jitter_count = 0
    total_moves = 0
    i = 0
    
    while i < len(events):
        event = events[i]
        event_type = event.get('Type', '')
        
        # Apply to ALL mouse movements (MouseMove, Click, RightDown)
        if event_type in ('MouseMove', 'Click', 'RightDown'):
            total_moves += 1
            
            # Random chance based on jitter_percentage
            if rng.random() < jitter_percentage:
                move_x = event.get('X')
                move_y = event.get('Y')
                move_time = event.get('Time')
                
                if move_x is not None and move_y is not None and move_time is not None:
                    num_jitters = rng.randint(2, 3)
                    jitter_events = []
                    
                    time_budget = rng.randint(100, 200)
                    time_per_jitter = time_budget // (num_jitters + 1)
                    
                    current_time = move_time - time_budget
                    
                    for j in range(num_jitters):
                        offset_x = rng.randint(-3, 3)
                        offset_y = rng.randint(-3, 3)
                        
                        jitter_x = int(move_x) + offset_x
                        jitter_y = int(move_y) + offset_y
                        
                        jitter_x = max(100, min(1800, jitter_x))
                        jitter_y = max(100, min(1000, jitter_y))
                        
                        jitter_events.append({
                            'Type': 'MouseMove',
                            'Time': current_time,
                            'X': jitter_x,
                            'Y': jitter_y
                        })
                        
                        current_time += time_per_jitter
                    
                    # Final movement: snap to EXACT target position
                    jitter_events.append({
                        'Type': 'MouseMove',
                        'Time': current_time,
                        'X': int(move_x),
                        'Y': int(move_y)
                    })
                    
                    for idx, jitter_event in enumerate(jitter_events):
                        events.insert(i + idx, jitter_event)
                    
                    i += len(jitter_events)
                    jitter_count += 1
        
        i += 1
    
    return events, jitter_count, total_moves, jitter_percentage

def insert_chat_from_file(events: list, rng: random.Random, chat_files: list) -> tuple:
    """
    20% chance to insert a random chat message from 'chat inserts' folder.
    Loads a recorded .json file and inserts it at a random point.
    Returns (events_with_chat, chat_inserted).
    """
    if not events or not chat_files or rng.random() > 0.20:
        return events, False
    
    # Pick random chat file
    chat_file = rng.choice(chat_files)
    
    try:
        # Load chat events
        chat_events = load_json_events(chat_file)
        if not chat_events:
            return events, False
        
        # Filter problematic keys from chat
        chat_events = filter_problematic_keys(chat_events)
        if not chat_events:
            return events, False
        
        # Find insertion point (20-80% through file)
        if len(events) < 10:
            return events, False
        
        start_idx = int(len(events) * 0.20)
        end_idx = int(len(events) * 0.80)
        insertion_point = rng.randint(start_idx, end_idx)
        
        # Get time at insertion point
        base_time = events[insertion_point].get('Time', 0)
        
        # Normalize chat events to start at base_time
        chat_start_time = min(e.get('Time', 0) for e in chat_events)
        for event in chat_events:
            event['Time'] = event['Time'] - chat_start_time + base_time
        
        # Calculate chat duration
        chat_duration = max(e.get('Time', 0) for e in chat_events) - base_time
        
        # Shift all events AFTER insertion point (no rounding!)
        for i in range(insertion_point, len(events)):
            events[i]['Time'] = events[i]['Time'] + chat_duration
        
        # Insert chat events
        for i, chat_event in enumerate(chat_events):
            events.insert(insertion_point + i, chat_event)
        
        return events, True
        
    except Exception as e:
        print(f"  âš ï¸ Error loading chat file {chat_file.name}: {e}")
        return events, False

def insert_intra_file_pauses(events: list, rng: random.Random) -> tuple:
    """
    Insert random pauses before recorded actions.
    Each file gets 1-4 random pauses (randomly chosen per file).
    Each pause is 1000-2000ms (non-rounded).
    Returns (events_with_pauses, total_pause_time).
    """
    if not events or len(events) < 5:
        return events, 0
    
    # Randomly decide how many pauses for this file (1-4)
    num_pauses = rng.randint(1, 4)
    
    # Randomly select which event indices will get pauses
    max_idx = len(events) - 1
    if max_idx < num_pauses:
        num_pauses = max_idx
    
    # Select random unique indices (skip first event at index 0)
    pause_indices = rng.sample(range(1, len(events)), num_pauses)
    pause_indices.sort()
    
    total_pause_added = 0
    
    # Apply pauses at selected indices
    for pause_idx in pause_indices:
        # Generate non-rounded pause duration (1000-2000ms)
        pause_duration = int(rng.uniform(1000.123, 1999.987))
        total_pause_added += pause_duration
        
        # Shift this event and all subsequent events by the pause (no rounding!)
        for j in range(pause_idx, len(events)):
            events[j]["Time"] = events[j]["Time"] + pause_duration
    
    return events, total_pause_added

def insert_normal_file_pauses(events: list, rng: random.Random) -> tuple:
    """
    Insert 1-3 random extended pauses (0-2 minutes each) for NORMAL files only.
    These are inserted at random points throughout the merged file.
    Returns (events_with_pauses, total_pause_time).
    """
    if not events or len(events) < 10:
        return events, 0
    
    # Randomly decide how many extended pauses (1-3)
    num_pauses = rng.randint(1, 3)
    
    # Select random unique indices
    max_idx = len(events) - 1
    if max_idx < num_pauses:
        num_pauses = max_idx
    
    pause_indices = rng.sample(range(1, len(events)), num_pauses)
    pause_indices.sort()
    
    total_pause_added = 0
    
    # Apply pauses at selected indices
    for pause_idx in pause_indices:
        # Generate non-rounded pause duration (0-2 minutes = 0-120000ms)
        pause_duration = int(rng.uniform(0.123, 119999.987))
        total_pause_added += pause_duration
        
        # Shift this event and all subsequent events by the pause (no rounding!)
        for j in range(pause_idx, len(events)):
            events[j]["Time"] = events[j]["Time"] + pause_duration
    
    return events, total_pause_added

def filter_problematic_keys(events: list) -> list:
    """
    Remove problematic key events that could trigger macro player hotkeys.
    Filters out: HOME (36), END (35), PAGE UP (33), PAGE DOWN (34), 
    ESC (27), PAUSE/BREAK (19), PRINT SCREEN (44)
    """
    problematic_keycodes = {27, 19, 33, 34, 35, 36, 44}
    
    filtered = []
    for event in events:
        # Skip KeyDown/KeyUp events with problematic keycodes
        if event.get('Type') in ['KeyDown', 'KeyUp']:
            keycode = event.get('KeyCode')
            if keycode in problematic_keycodes:
                continue  # Skip this event
        
        filtered.append(event)
    
    return filtered

def insert_idle_mouse_movements(events, rng, movement_percentage):
    """
    Insert realistic human-like mouse movements during idle periods (gaps > 5 seconds).
    
    Movements have:
    - Variable speeds (fast bursts, slow drifts, hesitations)
    - Imperfect paths (wobbles, overshoots, corrections)
    - Natural patterns (wandering, checking, fidgeting)
    - Smooth transition back to next recorded position
    """
    if not events or len(events) < 2:
        return events, 0
    
    result = []
    total_idle_time = 0
    
    for i in range(len(events)):
        result.append(events[i])
        
        # Check gap to next event
        if i < len(events) - 1:
            current_time = int(events[i].get("Time", 0))
            next_time = int(events[i + 1].get("Time", 0))
            gap = next_time - current_time
            
            # Only process gaps >= 5 seconds
            if gap >= 5000:
                # Skip if in drag sequence
                if is_in_drag_sequence(events, i):
                    continue
                
                # Calculate active window
                active_duration = int(gap * movement_percentage)
                buffer_start = (gap - active_duration) // 2
                movement_start = current_time + buffer_start
                
                # Get start position
                start_x, start_y = 500, 500
                for j in range(i, -1, -1):
                    x_val = events[j].get("X")
                    y_val = events[j].get("Y")
                    if x_val is not None and y_val is not None:
                        start_x = int(x_val)
                        start_y = int(y_val)
                        break
                
                # Get next position (where we need to end up)
                next_x, next_y = start_x, start_y
                for j in range(i + 1, min(i + 20, len(events))):
                    x_val = events[j].get("X")
                    y_val = events[j].get("Y")
                    if x_val is not None and y_val is not None:
                        next_x = int(x_val)
                        next_y = int(y_val)
                        break
                
                # Reserve last 25% for smooth transition back
                transition_duration = int(active_duration * 0.25)
                pattern_duration = active_duration - transition_duration
                
                # Choose movement behavior
                behavior = rng.choice([
                    'wander',      # Random wandering around
                    'check_edge',  # Quick look at screen edge
                    'fidget',      # Small nervous movements
                    'explore',     # Move far then return
                    'drift',       # Slow meandering
                    'scan'         # Move across screen
                ])
                
                pattern_end_x, pattern_end_y = start_x, start_y
                pattern_time_used = 0
                
                if behavior == 'wander':
                    # Random wandering - multiple small moves
                    num_moves = rng.randint(3, 6)
                    move_duration = pattern_duration // num_moves
                    
                    current_x, current_y = start_x, start_y
                    
                    for move_idx in range(num_moves):
                        # Pick random nearby target
                        target_x = current_x + rng.randint(-150, 150)
                        target_y = current_y + rng.randint(-100, 100)
                        target_x = max(100, min(1800, target_x))
                        target_y = max(100, min(1000, target_y))
                        
                        # Generate human path
                        path = generate_human_path(current_x, current_y, target_x, target_y, move_duration, rng)
                        
                        for path_time, px, py in path:
                            abs_time = movement_start + pattern_time_used + path_time
                            result.append({
                                "Time": abs_time,
                                "Type": "MouseMove",
                                "X": px,
                                "Y": py
                            })
                        
                        current_x, current_y = path[-1][1], path[-1][2]
                        pattern_time_used += move_duration
                    
                    pattern_end_x, pattern_end_y = current_x, current_y
                
                elif behavior == 'check_edge':
                    # Quick look at screen edge then back
                    edges = [
                        (150, start_y),    # Left edge
                        (1750, start_y),   # Right edge
                        (start_x, 150),    # Top edge
                        (start_x, 950),    # Bottom edge
                    ]
                    edge_x, edge_y = rng.choice(edges)
                    
                    # Move to edge (60% of time, fast)
                    edge_duration = int(pattern_duration * 0.6)
                    path_to_edge = generate_human_path(start_x, start_y, edge_x, edge_y, edge_duration, rng)
                    
                    for path_time, px, py in path_to_edge:
                        abs_time = movement_start + path_time
                        result.append({"Time": abs_time, "Type": "MouseMove", "X": px, "Y": py})
                    
                    # Return near start (40% of time, slower)
                    return_duration = pattern_duration - edge_duration
                    return_x = start_x + rng.randint(-40, 40)
                    return_y = start_y + rng.randint(-40, 40)
                    return_x = max(100, min(1800, return_x))
                    return_y = max(100, min(1000, return_y))
                    
                    path_return = generate_human_path(edge_x, edge_y, return_x, return_y, return_duration, rng)
                    
                    for path_time, px, py in path_return:
                        abs_time = movement_start + edge_duration + path_time
                        result.append({"Time": abs_time, "Type": "MouseMove", "X": px, "Y": py})
                    
                    pattern_end_x, pattern_end_y = path_return[-1][1], path_return[-1][2]
                    pattern_time_used = pattern_duration
                
                elif behavior == 'fidget':
                    # Small rapid movements in small area
                    num_fidgets = rng.randint(5, 10)
                    fidget_duration = pattern_duration // num_fidgets
                    
                    current_x, current_y = start_x, start_y
                    
                    for fidget_idx in range(num_fidgets):
                        # Small offset
                        target_x = current_x + rng.randint(-30, 30)
                        target_y = current_y + rng.randint(-30, 30)
                        target_x = max(100, min(1800, target_x))
                        target_y = max(100, min(1000, target_y))
                        
                        path = generate_human_path(current_x, current_y, target_x, target_y, fidget_duration, rng)
                        
                        for path_time, px, py in path:
                            abs_time = movement_start + pattern_time_used + path_time
                            result.append({"Time": abs_time, "Type": "MouseMove", "X": px, "Y": py})
                        
                        current_x, current_y = path[-1][1], path[-1][2]
                        pattern_time_used += fidget_duration
                    
                    pattern_end_x, pattern_end_y = current_x, current_y
                
                elif behavior == 'explore':
                    # Move far away then return near start
                    away_x = start_x + rng.randint(-400, 400)
                    away_y = start_y + rng.randint(-300, 300)
                    away_x = max(100, min(1800, away_x))
                    away_y = max(100, min(1000, away_y))
                    
                    # Go away (65% of time)
                    away_duration = int(pattern_duration * 0.65)
                    path_away = generate_human_path(start_x, start_y, away_x, away_y, away_duration, rng)
                    
                    for path_time, px, py in path_away:
                        abs_time = movement_start + path_time
                        result.append({"Time": abs_time, "Type": "MouseMove", "X": px, "Y": py})
                    
                    # Return (35% of time)
                    return_duration = pattern_duration - away_duration
                    return_x = start_x + rng.randint(-15, 15)
                    return_y = start_y + rng.randint(-15, 15)
                    return_x = max(100, min(1800, return_x))
                    return_y = max(100, min(1000, return_y))
                    
                    path_return = generate_human_path(away_x, away_y, return_x, return_y, return_duration, rng)
                    
                    for path_time, px, py in path_return:
                        abs_time = movement_start + away_duration + path_time
                        result.append({"Time": abs_time, "Type": "MouseMove", "X": px, "Y": py})
                    
                    pattern_end_x, pattern_end_y = path_return[-1][1], path_return[-1][2]
                    pattern_time_used = pattern_duration
                
                elif behavior == 'drift':
                    # Slow continuous drift
                    target_x = start_x + rng.randint(-200, 200)
                    target_y = start_y + rng.randint(-150, 150)
                    target_x = max(100, min(1800, target_x))
                    target_y = max(100, min(1000, target_y))
                    
                    path = generate_human_path(start_x, start_y, target_x, target_y, pattern_duration, rng)
                    
                    for path_time, px, py in path:
                        abs_time = movement_start + path_time
                        result.append({"Time": abs_time, "Type": "MouseMove", "X": px, "Y": py})
                    
                    pattern_end_x, pattern_end_y = path[-1][1], path[-1][2]
                    pattern_time_used = pattern_duration
                
                elif behavior == 'scan':
                    # Scan across screen
                    scan_distance = rng.randint(300, 600)
                    direction = rng.choice(['horizontal', 'vertical', 'diagonal'])
                    
                    if direction == 'horizontal':
                        target_x = start_x + (scan_distance if rng.random() < 0.5 else -scan_distance)
                        target_y = start_y + rng.randint(-50, 50)
                    elif direction == 'vertical':
                        target_x = start_x + rng.randint(-50, 50)
                        target_y = start_y + (scan_distance if rng.random() < 0.5 else -scan_distance)
                    else:  # diagonal
                        target_x = start_x + (scan_distance if rng.random() < 0.5 else -scan_distance)
                        target_y = start_y + (scan_distance if rng.random() < 0.5 else -scan_distance)
                    
                    target_x = max(100, min(1800, target_x))
                    target_y = max(100, min(1000, target_y))
                    
                    path = generate_human_path(start_x, start_y, target_x, target_y, pattern_duration, rng)
                    
                    for path_time, px, py in path:
                        abs_time = movement_start + path_time
                        result.append({"Time": abs_time, "Type": "MouseMove", "X": px, "Y": py})
                    
                    pattern_end_x, pattern_end_y = path[-1][1], path[-1][2]
                    pattern_time_used = pattern_duration
                
                # Smooth transition back to next recorded position
                transition_path = generate_human_path(
                    pattern_end_x, pattern_end_y,
                    next_x, next_y,
                    transition_duration,
                    rng
                )
                
                for path_time, px, py in transition_path:
                    abs_time = movement_start + pattern_duration + path_time
                    result.append({"Time": abs_time, "Type": "MouseMove", "X": px, "Y": py})
                
                total_idle_time += active_duration
    
    return result, total_idle_time

class QueueFileSelector:
    def __init__(self, rng, all_files, durations_cache):
        self.rng = rng
        self.durations = durations_cache
        self.efficient = [f for f in all_files if "¬¬" not in f.name]
        self.inefficient = [f for f in all_files if "¬¬" in f.name]
        self.eff_pool = list(self.efficient)
        self.ineff_pool = list(self.inefficient)
        self.rng.shuffle(self.eff_pool)
        self.rng.shuffle(self.ineff_pool)

    def get_sequence(self, target_minutes, force_inef=False, is_time_sensitive=False):
        seq, cur_ms = [], 0.0
        target_ms = target_minutes * 60000
        # Add ±5% margin for flexibility
        margin = int(target_ms * 0.05)
        target_min = target_ms - margin
        target_max = target_ms + margin
        actual_force = force_inef if not is_time_sensitive else False
        
        # Keep adding files until we reach target
        # Stop conditions:
        # 1. Reached target OR
        # 2. Adding next file would overshoot by more than 4 minutes
        
        while cur_ms < target_max:
            # Try to get next file
            if actual_force and self.ineff_pool: pick = self.ineff_pool.pop(0)
            elif self.eff_pool: pick = self.eff_pool.pop(0)
            elif self.efficient:
                self.eff_pool = list(self.efficient); self.rng.shuffle(self.eff_pool)
                pick = self.eff_pool.pop(0)
            elif self.ineff_pool and not is_time_sensitive: pick = self.ineff_pool.pop(0)
            else: break  # No more files
            
            file_duration = self.durations.get(pick, 500)
            
            # File selector multiplier - CRITICAL for accuracy
            # 1.0x = too many files (overshoot 11-18 min)
            # 1.8x = too few files (undershoot 10-13 min)
            # 1.35x = sweet spot (target Â±2-4 min)
            if is_time_sensitive:
                estimated_time = file_duration * 1.05  # TIME SENSITIVE: minimal overhead
            else:
                estimated_time = file_duration * 1.35  # NORMAL: balanced estimate
            
            # Check if adding would overshoot too much
            potential_total = cur_ms + estimated_time
            overshoot = potential_total - target_ms
            
            if overshoot > margin:  # Would overshoot beyond acceptable margin
                # Only skip if we're already reasonably close to target
                if cur_ms >= (target_ms - (4 * 60000)):  # Within 4 min of target
                    break  # Close enough, stop
                else:
                    # Still far from target, add it anyway
                    seq.append(pick)
                    cur_ms += estimated_time
            else:
                # Safe to add (won't overshoot by more than 4 min)
                seq.append(pick)
                cur_ms += estimated_time
            
            # Safety limits
            if len(seq) > 800: break
            if cur_ms > target_ms * 3: break
        
        return seq


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_root", type=str)
    parser.add_argument("output_root", type=Path)
    parser.add_argument("--versions", type=int, default=6)
    parser.add_argument("--target-minutes", type=int, default=35)
    parser.add_argument("--bundle-id", type=int, required=True)
    parser.add_argument("--speed-range", type=str, default="1.0 1.0")
    parser.add_argument("--no-chat", action="store_true", help="Disable chat inserts (default: enabled)")
    parser.add_argument("--use-whitelist", action="store_true", help="Use whitelist from 'specific folders to include for merge.txt' (default: off)")
    args = parser.parse_args()

    search_base = Path(args.input_root).resolve()
    if not search_base.exists():
        search_base = Path(".").resolve()
        
    originals_root = None
    for d in ["originals", "input_macros"]:
        test_path = search_base / d
        if test_path.exists() and test_path.is_dir():
            originals_root = test_path
            break
            
    if not originals_root:
        originals_root = search_base
    
    # NEW: Load folder whitelist (only if --use-whitelist flag is set)
    folder_whitelist = None
    if args.use_whitelist:
        folder_whitelist = load_folder_whitelist(originals_root.parent)
        if folder_whitelist is None:
            print("⚠️  --use-whitelist flag set but no whitelist file found or file is empty")
            print("    Will process ALL folders")
    else:
        print("📋 Whitelist is DISABLED (--use-whitelist flag not set)")
        print("   Processing ALL folders")
    
    logout_file = None
    logout_patterns = ["logout.json", "- logout.json", "-logout.json", "logout", "- logout", "-logout"]
    
    for location_dir in [originals_root, originals_root.parent, search_base]:
        if logout_file:
            break
        for pattern in logout_patterns:
            test_file = location_dir / pattern
            for test_path in [test_file, Path(str(test_file) + ".json")]:
                if test_path.exists() and test_path.is_file():
                    logout_file = test_path
                    print(f"âœ“ Found logout file at: {logout_file}")
                    break
            if logout_file:
                break

    bundle_dir = args.output_root / f"merged_bundle_{args.bundle_id}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random()
    pools = {}
    durations_cache = {}
    
    # Load chat insert files from 'chat inserts' folder (unless --no-chat is set)
    chat_files = []
    if not args.no_chat:
        chat_dir = Path(args.input_root).parent / "chat inserts"
        if chat_dir.exists() and chat_dir.is_dir():
            chat_files = list(chat_dir.glob("*.json"))
            if chat_files:
                print(f"âœ“ Found {len(chat_files)} chat insert files in: {chat_dir}")
            else:
                print(f"âš ï¸ 'chat inserts' folder exists but is empty: {chat_dir}")
        else:
            print(f"âš ï¸ No 'chat inserts' folder found (will skip chat inserts)")
    else:
        print(f"ðŸ”• Chat inserts DISABLED (--no-chat flag)")

    # Track skipped and processed folders for summary
    skipped_folders = []
    processed_folders = []


    for root, dirs, files in os.walk(originals_root):
        curr = Path(root)
        if any(p in curr.parts for p in [".git", ".github", "output"]): continue
        
        jsons = [f for f in files if f.endswith(".json") and "click_zones" not in f.lower()]
        non_jsons = [f for f in files if not f.endswith(".json")]
        
        if not jsons: continue

        # NEW: Check whitelist before processing
        if not should_process_folder(curr, originals_root, folder_whitelist):
            skipped_folders.append(curr.name)
            continue

        processed_folders.append(curr.name)

        
        macro_id = clean_identity(curr.name)
        rel_path = curr.relative_to(originals_root)
            
        parent_scope = None
        for part in curr.parts:
            if "desktop" in part.lower() or "mobile" in part.lower():
                parent_scope = part
                break
            
        key = str(rel_path).lower()
        if key not in pools:
            is_ts = bool(re.search(r'time[\s-]*sens', key))
            file_paths = [curr / f for f in jsons]
            
            # Separate DROP ONLY files from regular files (for Mining folders)
            drop_only_files = find_drop_only_files(curr, file_paths)
            
            # Remove DROP ONLY files from regular merge pool
            if drop_only_files:
                file_paths = [f for f in file_paths if f not in drop_only_files]
                print(f"  Found {len(drop_only_files)} DROP ONLY file(s), excluded from regular pool")
            
                
            pools[key] = {
                "rel_path": rel_path,
                "files": file_paths,
                "is_ts": is_ts,
                "macro_id": macro_id,
                "parent_scope": parent_scope,
                "non_json_files": [curr / f for f in non_jsons],
                "drop_only_files": drop_only_files
            }
                
            for fp in file_paths:
                durations_cache[fp] = get_file_duration_ms(fp)

    for pool_key, pool_data in pools.items():
        all_files = pool_data["files"]
        always_files = [f for f in all_files if is_always_first_or_last_file(Path(f).name)]
        mergeable_files = [f for f in all_files if f not in always_files]
        pool_data["files"] = mergeable_files
        pool_data["always_files"] = always_files
    
    # GLOBAL chat queue - persists across ALL folders and versions in this batch
    # Ensures each merged file gets unique chat before any repeats
    global_chat_queue = list(chat_files) if chat_files else []
    if global_chat_queue:
        rng.shuffle(global_chat_queue)
        print(f"ðŸ”„ Initialized global chat queue with {len(global_chat_queue)} files (shuffled)")
    
    for key, data in pools.items():
        folder_name = data["rel_path"].name
        folder_number = extract_folder_number(folder_name)
        
        if folder_number == 0:
            print(f"WARNING: No number found in folder name '{folder_name}', using 0")
        
        data["folder_number"] = folder_number
    
    for key, data in pools.items():
        folder_number = data["folder_number"]
        
        if not data["files"]:
            print(f"Skipping folder (0 files): {data['rel_path']}")
            continue
        
        original_rel_path = data["rel_path"]
        
        out_f = bundle_dir / original_rel_path
        out_f.mkdir(parents=True, exist_ok=True)
        
        if logout_file:
            try:
                original_name = logout_file.name
                # Simple @ prefix with UPPERCASE, no folder number: "logout.json" → "@ LOGOUT.JSON"
                if original_name.startswith("-"):
                    # Has dash: "- logout.json" → "@ LOGOUT.JSON"
                    new_name = "@ " + original_name[1:].strip().upper()
                else:
                    # Add @ prefix: "logout.json" â†’ "- 46 LOGOUT.JSON"
                    new_name = "@ " + original_name.upper()
                logout_dest = out_f / new_name
                shutil.copy2(logout_file, logout_dest)
                print(f"  âœ“ Copied logout: {original_name} â†’ {new_name}")
            except Exception as e:
                print(f"  âœ— Error copying {logout_file.name}: {e}")
        else:
            print(f"  âš  Warning: No logout file found")
        
        if "non_json_files" in data and data["non_json_files"]:
            for non_json_file in data["non_json_files"]:
                try:
                    original_name = non_json_file.name
                    # Keep @ prefix if present: "RuneLite_file.png" â†’ "- 46 RuneLite_file.png"
                    if original_name.startswith("-"):
                        # Already has @ prefix: "- file.png" â†’ "- 46 file.png"
                        new_name = f"@ {folder_number} {original_name[1:].strip()}"
                    else:
                        # Add @ prefix: "file.png" â†’ "- 46 file.png"
                        new_name = f"@ {folder_number} {original_name}"
                    shutil.copy2(non_json_file, out_f / new_name)
                    print(f"  âœ“ Copied non-JSON file: {original_name} â†’ {new_name}")
                except Exception as e:
                    print(f"  âœ— Error copying {non_json_file.name}: {e}")
        
        if "always_files" in data and data["always_files"]:
            for always_file in data["always_files"]:
                try:
                    original_name = Path(always_file).name
                    # Add folder number prefix: "- always first.json" â†’ "- 46 always first.json"
                    # Handle files starting with "-" or "always"
                    if original_name.startswith("-"):
                        new_name = f"@ {folder_number} {original_name[1:].strip()}"
                    else:
                        new_name = f"@ {folder_number} {original_name}"
                    shutil.copy2(always_file, out_f / new_name)
                    print(f"  âœ“ Copied 'always' file: {original_name} â†’ {new_name}")
                except Exception as e:
                    print(f"  âœ— Error copying {Path(always_file).name}: {e}")
        
        total_original_ms = sum(durations_cache.get(f, 0) for f in data["files"])
        
        manifest = [
            f"MANIFEST FOR FOLDER: {original_rel_path}",
            "=" * 40,
            f"Script Version: {VERSION}",
            f"Merged Bundle: merged_bundle_{args.bundle_id}",
            f"Total Original Files: {len(data['files'])}",
            f"Total Original Files Duration: {format_ms_precise(total_original_ms)}",
            " "
        ]
        
        # Note: global_chat_queue is created BEFORE folder loop (persists across all folders)
        
        norm_v = args.versions
        is_ts  = data["is_ts"]

        # Regular folder:  norm_v normal  +  norm_v//2 inef  +  3 raw
        # TS folder:        norm_v TS      +  0 inef          +  3 raw
        inef_v  = 0 if is_ts else (norm_v // 2)
        raw_v   = 3   # always 3 raw (^ tag) for every folder type
        total_v = norm_v + inef_v + raw_v

        # NEW NAMING SCHEME: Raw gets A,B,C first, then Inefficient, then Normal
        # This ensures alphabetical sorting works: ^A, ^B, ^C, ¬¬D, ¬¬E, ¬¬F, G, H, I...
        # 
        # Order of generation:
        # 1. Raw files (raw_v = 3): indices 1, 2, 3 → letters A, B, C
        # 2. Inefficient files (inef_v): indices 4, 5, 6 → letters D, E, F
        # 3. Normal files (norm_v = 6): indices 7-12 → letters G, H, I, J, K, L
        
        for v_idx in range(1, total_v + 1):
            # Determine file type based on NEW ordering
            if v_idx <= raw_v:
                is_raw = True
                is_inef = False
                is_ts_version = False
            elif v_idx <= raw_v + inef_v:
                is_raw = False
                is_inef = True
                is_ts_version = False
            else:
                is_raw = False
                is_inef = False
                is_ts_version = is_ts  # Only normal files can be TS
            
            v_letter = chr(64 + v_idx)
            v_code = f"{folder_number}_{v_letter}"

            if is_ts_version: mult = rng.choice([1.0, 1.2, 1.5])
            elif is_inef:     mult = rng.choices([1, 2, 3], weights=[20, 40, 40], k=1)[0]
            elif is_raw:      mult = rng.choices([1, 2, 3], weights=[50, 30, 20], k=1)[0]
            else:             mult = rng.choices([1, 2, 3], weights=[50, 30, 20], k=1)[0]

            movement_percentage = rng.uniform(0.40, 0.50)
            jitter_percentage = 0.0  # Will be set per file
            
            total_idle_movements = 0
            total_intra_pauses = 0
            total_normal_pauses = 0  # NEW: Track normal file pauses
            total_gaps = 0
            total_afk_pool = 0
            total_jitter_count = 0
            total_clicks = 0
            file_segments = []
            massive_pause_info = None
            merged = []
            timeline = 0
            
            paths = QueueFileSelector(rng, data["files"], durations_cache).get_sequence(args.target_minutes, is_inef, is_ts_version)
            
            if not paths:
                continue

            # DROP ONLY insertion for Mining folders (1 file in middle)
            drop_only_file = None
            if "drop_only_files" in data and data["drop_only_files"]:
                # Select ONE random DROP file
                drop_only_file = rng.choice(data["drop_only_files"])
                print(f"  ℹ️  Mining folder: Will insert DROP ONLY file: {drop_only_file.name}")

            # Chat - only 1 per merged file, using global queue
            chat_used = False
            # Insert chat in only 50% of merged files
            should_insert_chat = rng.random() < 0.50
            chat_insertion_point = rng.randint(1, max(1, len(paths)-1)) if len(paths) > 1 and should_insert_chat else -1
            file_segments = []
            
            for i, p in enumerate(paths):
                raw = load_json_events(p)
                if not raw: continue
                
                # Filter problematic keys
                raw = filter_problematic_keys(raw)
                if not raw: continue
                
                # is_time_sensitive = True only for explicitly TS versions (not normal versions in TS folders)
                is_time_sensitive = is_ts_version
                
                # INSERT CHAT ONCE (before the chosen file index)
                if not chat_used and i == chat_insertion_point and global_chat_queue:
                    try:
                        chat_events = load_json_events(chat_file)
                        if chat_events:
                            chat_events = filter_problematic_keys(chat_events)
                            if chat_events:
                                # Normalize to current timeline
                                chat_start = min(e.get('Time', 0) for e in chat_events)
                                chat_file_start_idx = len(merged)
                                for e in chat_events:
                                    e['Time'] = e['Time'] - chat_start + timeline
                                    merged.append(e)
                                
                                timeline = merged[-1]["Time"] if merged else timeline
                                file_segments.append({
                                    "name": chat_file.name,
                                    "end_time": timeline,
                                    "start_idx": chat_file_start_idx,
                                    "end_idx": len(merged) - 1,
                                    "is_chat": True
                                })
                                chat_used = True
                                
                                # Put used file at END of queue (ensures all files used before repeat)
                                global_chat_queue.append(chat_file)
                                
                                # If queue is empty, refill and shuffle
                                if not global_chat_queue and chat_files:
                                    global_chat_queue = list(chat_files)
                                    rng.shuffle(global_chat_queue)
                    except Exception as e:
                        print(f"  âš ï¸ Error loading chat {chat_file.name}: {e}")
                        global_chat_queue.append(chat_file)  # Return to queue
                
                # Step 1: Add pre-move jitter (random 20-45% of moves)
                # All types get jitter (doesn't affect time)
                raw_with_jitter, jitter_count, click_count, jitter_pct = add_pre_click_jitter(raw, rng)
                total_jitter_count += jitter_count
                total_clicks += click_count
                jitter_percentage = jitter_pct
                
                # Step 2: Insert random intra-file pauses between actions
                # TIME SENSITIVE and RAW: Skip (adds time)
                if not is_time_sensitive and not is_raw:
                    raw_with_pauses, intra_pause_time = insert_intra_file_pauses(raw_with_jitter, rng)
                    total_intra_pauses += intra_pause_time
                else:
                    raw_with_pauses = raw_with_jitter
                
                # Step 3: Insert idle mouse movements in gaps >= 5 seconds
                # Fills gaps with movement, does NOT add time
                raw_with_movements, idle_time = insert_idle_mouse_movements(raw_with_pauses, rng, movement_percentage)
                total_idle_movements += idle_time
                
                
                t_vals = [int(e["Time"]) for e in raw_with_movements]
                base_t = min(t_vals)
                
                # Inter-file gap: 500-5000ms (non-rounded) Ã— multiplier
                if i > 0:
                    gap = int(rng.uniform(500.123, 4999.987) * mult)
                    
                    # CRITICAL: Add cursor transition during gap to prevent teleporting
                    # Get last cursor position from previous file (must have non-None X/Y)
                    last_cursor_event = None
                    for e in reversed(merged):
                        if e.get('X') is not None and e.get('Y') is not None:
                            last_cursor_event = e
                            break
                    
                    # Get first cursor position from current file (must have non-None X/Y)
                    first_cursor_event = None
                    for e in raw_with_movements:
                        if e.get('X') is not None and e.get('Y') is not None:
                            first_cursor_event = e
                            break
                    
                    # If both exist and positions differ, add smooth transition
                    if last_cursor_event and first_cursor_event:
                        last_x, last_y = int(last_cursor_event['X']), int(last_cursor_event['Y'])
                        first_x, first_y = int(first_cursor_event['X']), int(first_cursor_event['Y'])
                        
                        # Only add transition if positions are different
                        if (last_x != first_x) or (last_y != first_y):
                            transition_path = generate_human_path(
                                last_x, last_y,
                                first_x, first_y,
                                gap,
                                rng
                            )
                            
                            for rel_time, x, y in transition_path:
                                if rel_time < gap:
                                    merged.append({
                                        'Type': 'MouseMove',
                                        'Time': timeline + rel_time,
                                        'X': x,
                                        'Y': y
                                    })
                else:
                    gap = 0
                    
                timeline += gap
                total_gaps += gap
                
                file_start_idx = len(merged)  # Track where this file starts in merged array
                
                for e in raw_with_movements:
                    ne = {**e}
                    rel_offset = e["Time"] - base_t  # No rounding!
                    ne["Time"] = timeline + rel_offset
                    merged.append(ne)
                
                timeline = merged[-1]["Time"]
                file_end_idx = len(merged) - 1
                file_segments.append({
                    "name": p.name, 
                    "end_time": timeline,
                    "start_idx": file_start_idx,
                    "end_idx": file_end_idx,
                    "is_chat": False  # Regular file
                })
            




            # INSERT DROP ONLY file in middle (Mining folders only)
            if drop_only_file and merged and len(merged) > 10:
                drop_events = load_json_events(drop_only_file)
                if drop_events:
                    drop_events = filter_problematic_keys(drop_events)
                    if drop_events:
                        # Random insertion point (25-75% through file)
                        drop_start_idx = int(len(merged) * 0.25)
                        drop_end_idx = int(len(merged) * 0.75)
                        drop_insertion_point = rng.randint(drop_start_idx, drop_end_idx)
                        
                        drop_base_time = merged[drop_insertion_point].get("Time", 0)
                        drop_start_time = min(e.get("Time", 0) for e in drop_events)
                        normalized_drop = []
                        for e in drop_events:
                            ne = {**e}
                            ne["Time"] = e["Time"] - drop_start_time + drop_base_time
                            normalized_drop.append(ne)
                        
                        drop_duration = max(e.get("Time", 0) for e in normalized_drop) - drop_base_time
                        
                        # Shift all events AFTER insertion point by drop duration
                        for j in range(drop_insertion_point, len(merged)):
                            merged[j]["Time"] += drop_duration
                        
                        # Insert DROP events at the insertion point
                        for idx, drop_event in enumerate(normalized_drop):
                            merged.insert(drop_insertion_point + idx, drop_event)
                        
                        timeline = merged[-1]["Time"]
                        
                        file_segments.append({
                            "name": f"[DROP ONLY] {drop_only_file.name}",
                            "end_time": drop_base_time + drop_duration,
                            "start_idx": drop_insertion_point,
                            "end_idx": drop_insertion_point + len(normalized_drop) - 1,
                            "is_chat": False
                        })
                        
                        print(f"    ✓ Inserted DROP ONLY at {format_ms_precise(drop_base_time)}")

            total_afk_pool = total_idle_movements
            chat_inserted = chat_used  # Track if chat was used
            
            # Normal File Pause: only for NORMAL files (not inef, not TS, not raw)
            if not is_inef and not is_time_sensitive and not is_raw and merged:
                merged, normal_pause_time = insert_normal_file_pauses(merged, rng)
                total_normal_pauses += normal_pause_time
                if normal_pause_time > 0:
                    timeline = merged[-1]["Time"]
                    # Update file_segments to reflect new timeline after pauses
                    for seg in file_segments:
                        if seg["end_idx"] < len(merged):
                            seg["end_time"] = merged[seg["end_idx"]]["Time"]
            
            if is_inef and not data["is_ts"] and len(merged) > 1:
                # Massive pause: 4-9 minutes (240000-540000ms)
                p_ms = rng.randint(240000, 540000)
                split = rng.randint(0, len(merged) - 2)
                for j in range(split + 1, len(merged)): merged[j]["Time"] += p_ms
                timeline = merged[-1]["Time"]
                massive_pause_info = f"Massive P1: {format_ms_precise(p_ms)}"
                
                for seg in file_segments:
                    if seg["end_idx"] > split:
                        seg["end_time"] = merged[seg["end_idx"]]["Time"]
            
            # Calculate exact time for filename
            total_minutes = int(timeline / 60000)
            total_seconds = int((timeline % 60000) / 1000)
            
            # File prefix: ¬¬ = inefficient, ^ = raw, blank = normal/TS
            if is_raw:        prefix = "^"
            elif is_inef:     prefix = "¬¬"
            else:             prefix = ""
            
            fname = f"{prefix}{v_code}_{total_minutes}m{total_seconds}s.json"
            (out_f / fname).write_text(json.dumps(merged, indent=2))
            
            # Calculate pause time (idle movements are informational only)
            total_pause = total_intra_pauses + total_gaps + total_normal_pauses
            
            # Determine file type
            if is_ts_version:
                file_type = "Time sensitive"
            elif is_inef:
                file_type = "Inefficient"
            elif is_raw:
                file_type = "Raw"
            else:
                file_type = "Normal"
            
            # Calculate pause times
            # Only inter-file gaps get multiplied!
            original_intra = total_intra_pauses  # Not multiplied
            original_inter = int(total_gaps / mult) if mult > 0 else total_gaps
            original_normal = total_normal_pauses
            original_total = original_intra + original_inter + original_normal
            
            # Calculate total time in minutes and seconds
            total_min = int(timeline / 60000)
            total_sec = int((timeline % 60000) / 1000)
            
            # Version label with duration and separator
            version_label = f"Version {prefix}{v_code}_{total_min}m{total_sec}s:"
            separator = "=" * 40
            
            if is_raw:
                # Raw files: minimal manifest (only inter-file gaps, no anti-detection)
                manifest_entry = [
                    separator,
                    " ",
                    version_label,
                    f"FILE TYPE: Raw (no time-adding features, no chat)",
                    f"  Between files pause: {format_ms_precise(total_gaps)} (x{mult} Multiplier)",
                ]
            else:
                manifest_entry = [
                    separator,
                    " ",
                    version_label,
                    f"FILE TYPE: {file_type}",
                    f"  Total PAUSE ADDED: {format_ms_precise(total_pause)} (x{mult} Multiplier)",
                    f"BREAKDOWN",
                    f"total before    - Within original files pauses: {format_ms_precise(original_intra)}",
                    f"multiplier      - Between original files pauses: {format_ms_precise(original_inter)}",
                    f"                - Normal file pause: {format_ms_precise(original_normal)}",
                ]
            
            # Idle and jitter: all types (raw included, since they don't add time)
            manifest_entry.extend([
                f"Idle Mouse Movements: {format_ms_precise(total_idle_movements)}",
                f"Mouse Jitter: {int(jitter_percentage * 100)}%"
            ])
            
            # Add files list with chat highlighting
            # Sort file segments by end_time for chronological order
            file_segments.sort(key=lambda x: x["end_time"])
            
            manifest_entry.append("")
            for seg in file_segments:
                if seg.get("is_chat", False):
                    manifest_entry.append(f"  ****** {seg['name']} (Ends at {format_ms_precise(seg['end_time'])})")
                else:
                    manifest_entry.append(f"  * {seg['name']} (Ends at {format_ms_precise(seg['end_time'])})")
            
            manifest.append("\n".join(manifest_entry))

        (out_f / f"!_MANIFEST_{folder_number}_!.txt").write_text("\n".join(manifest))

if __name__ == "__main__":
    main()
