#!/usr/bin/env python3
"""
merge_macros.py - STABLE RESTORE POINT (v3.4.0) - HUMAN-LIKE MOVEMENTS
- NEW: Imperfect, human-like mouse movements (no perfect geometry)
- NEW: Variable cursor speeds during idle movements (fast/slow/acceleration)
- NEW: Random wobbles, overshoots, corrections, pauses mid-movement
- NEW: Total original file duration shown in manifest
- FIX: AFK pool now properly calculated (was always showing 0)
- FIX: "always first/last" files now detected properly
- FIX: Logout file copied to EVERY folder with a manifest
- FEATURE: Smooth transitions back to next recorded position
- FIX: Idle mouse movements SKIP drag sequences
- FIX: Naming scheme 29A, 29B, 29C (number before letter)
- OPTIMIZED: Cached durations, single os.walk(), shallow copy.
"""

import argparse, json, random, re, sys, os, math, shutil
from pathlib import Path

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
        self.efficient = [f for f in all_files if "¬¬¬" not in f.name]
        self.inefficient = [f for f in all_files if "¬¬¬" in f.name]
        self.eff_pool = list(self.efficient)
        self.ineff_pool = list(self.inefficient)
        self.rng.shuffle(self.eff_pool)
        self.rng.shuffle(self.ineff_pool)

    def get_sequence(self, target_minutes, force_inef=False, strictly_eff=False):
        seq, cur_ms = [], 0.0
        target_ms = target_minutes * 60000
        actual_force = force_inef if not strictly_eff else False
        while cur_ms < target_ms:
            if actual_force and self.ineff_pool: pick = self.ineff_pool.pop(0)
            elif self.eff_pool: pick = self.eff_pool.pop(0)
            elif self.efficient:
                self.eff_pool = list(self.efficient); self.rng.shuffle(self.eff_pool)
                pick = self.eff_pool.pop(0)
            elif self.ineff_pool and not strictly_eff: pick = self.ineff_pool.pop(0)
            else: break
            seq.append(pick)
            cur_ms += (self.durations.get(pick, 2000) + 1500)
            if len(seq) > 1000: break
        return seq

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_root", type=str)
    parser.add_argument("output_root", type=Path)
    parser.add_argument("--versions", type=int, default=6)
    parser.add_argument("--target-minutes", type=int, default=35)
    parser.add_argument("--bundle-id", type=int, required=True)
    parser.add_argument("--speed-range", type=str, default="1.0 1.0")
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
                    print(f"✓ Found logout file at: {logout_file}")
                    break
            if logout_file:
                break

    bundle_dir = args.output_root / f"merged_bundle_{args.bundle_id}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random()
    pools = {}
    z_storage = {}
    durations_cache = {}

    for root, dirs, files in os.walk(originals_root):
        curr = Path(root)
        if any(p in curr.parts for p in [".git", ".github", "output"]): continue
        
        jsons = [f for f in files if f.endswith(".json") and "click_zones" not in f.lower()]
        non_jsons = [f for f in files if not f.endswith(".json")]
        
        if not jsons: continue
        
        is_z_storage = "z +100" in str(curr).lower()
        
        if is_z_storage:
            parent_scope = None
            for part in curr.parts:
                if "desktop" in part.lower() or "mobile" in part.lower():
                    parent_scope = part
                    break
            
            if parent_scope:
                macro_id = clean_identity(curr.name)
                key = (parent_scope, macro_id)
                if key not in z_storage:
                    z_storage[key] = []
                
                for f in jsons:
                    file_path = curr / f
                    z_storage[key].append(file_path)
                    durations_cache[file_path] = get_file_duration_ms(file_path)
        else:
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
                
                pools[key] = {
                    "rel_path": rel_path,
                    "files": file_paths,
                    "is_ts": is_ts,
                    "macro_id": macro_id,
                    "parent_scope": parent_scope,
                    "non_json_files": [curr / f for f in non_jsons]
                }
                
                for fp in file_paths:
                    durations_cache[fp] = get_file_duration_ms(fp)

    for pool_key, pool_data in pools.items():
        parent_scope = pool_data["parent_scope"]
        macro_id = pool_data["macro_id"]
        
        z_key = (parent_scope, macro_id)
        if z_key in z_storage:
            pool_data["files"].extend(z_storage[z_key])
    
    for pool_key, pool_data in pools.items():
        all_files = pool_data["files"]
        always_files = [f for f in all_files if is_always_first_or_last_file(Path(f).name)]
        mergeable_files = [f for f in all_files if f not in always_files]
        pool_data["files"] = mergeable_files
        pool_data["always_files"] = always_files
    
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
                # Add folder number prefix with leading dash and make UPPERCASE: "logout.json" → "- 46 LOGOUT.JSON"
                if original_name.startswith("-"):
                    # Already has dash: "- logout.json" → "- 46 LOGOUT.JSON"
                    new_name = f"- {folder_number} {original_name[1:].strip()}".upper()
                else:
                    # Add dash: "logout.json" → "- 46 LOGOUT.JSON"
                    new_name = f"- {folder_number} {original_name}".upper()
                logout_dest = out_f / new_name
                shutil.copy2(logout_file, logout_dest)
                print(f"  ✓ Copied logout: {original_name} → {new_name}")
            except Exception as e:
                print(f"  ✗ Error copying {logout_file.name}: {e}")
        else:
            print(f"  ⚠ Warning: No logout file found")
        
        if "non_json_files" in data and data["non_json_files"]:
            for non_json_file in data["non_json_files"]:
                try:
                    original_name = non_json_file.name
                    # Keep leading dash if present: "RuneLite_file.png" → "- 46 RuneLite_file.png"
                    if original_name.startswith("-"):
                        # Already has dash: "- file.png" → "- 46 file.png"
                        new_name = f"- {folder_number} {original_name[1:].strip()}"
                    else:
                        # Add dash: "file.png" → "- 46 file.png"
                        new_name = f"- {folder_number} {original_name}"
                    shutil.copy2(non_json_file, out_f / new_name)
                    print(f"  ✓ Copied non-JSON file: {original_name} → {new_name}")
                except Exception as e:
                    print(f"  ✗ Error copying {non_json_file.name}: {e}")
        
        if "always_files" in data and data["always_files"]:
            for always_file in data["always_files"]:
                try:
                    original_name = Path(always_file).name
                    # Add folder number prefix: "- always first.json" → "- 46 always first.json"
                    # Handle files starting with "-" or "always"
                    if original_name.startswith("-"):
                        new_name = f"- {folder_number} {original_name[1:].strip()}"
                    else:
                        new_name = f"{folder_number} {original_name}"
                    shutil.copy2(always_file, out_f / new_name)
                    print(f"  ✓ Copied 'always' file: {original_name} → {new_name}")
                except Exception as e:
                    print(f"  ✗ Error copying {Path(always_file).name}: {e}")
        
        total_original_ms = sum(durations_cache.get(f, 0) for f in data["files"])
        
        manifest = [
            f"MANIFEST FOR FOLDER: {original_rel_path}",
            "=" * 40,
            f"Total Available Files: {len(data['files'])}",
            f"Total Original Duration: {format_ms_precise(total_original_ms)}",
            f"Folder Number: {folder_number}",
            "",
            ""
        ]
        
        norm_v = args.versions
        inef_v = 0 if data["is_ts"] else (norm_v // 2)
        
        for v_idx in range(1, (norm_v + inef_v) + 1):
            is_inef = (v_idx > norm_v)
            v_letter = chr(64 + v_idx)
            v_code = f"{folder_number}{v_letter}"
            
            if data["is_ts"]: mult = rng.choice([1.0, 1.2, 1.5])
            elif is_inef: mult = rng.choices([1, 2, 3], weights=[20, 40, 40], k=1)[0]
            else: mult = rng.choices([1, 2, 3], weights=[50, 30, 20], k=1)[0]
            
            movement_percentage = rng.uniform(0.40, 0.50)
            
            total_idle_movements = 0
            total_gaps = 0
            total_afk_pool = 0
            file_segments = []
            massive_pause_info = None
            merged = []
            timeline = 0
            
            paths = QueueFileSelector(rng, data["files"], durations_cache).get_sequence(args.target_minutes, is_inef, data["is_ts"])
            
            if not paths:
                continue
            
            for i, p in enumerate(paths):
                raw = load_json_events(p)
                if not raw: continue
                
                raw_with_movements, idle_time = insert_idle_mouse_movements(raw, rng, movement_percentage)
                total_idle_movements += idle_time
                
                t_vals = [int(e["Time"]) for e in raw_with_movements]
                base_t = min(t_vals)
                
                gap = int(rng.randint(500, 2500) * mult) if i > 0 else 0
                timeline += gap
                total_gaps += gap
                
                file_start_idx = len(merged)  # Track where this file starts in merged array
                
                for e in raw_with_movements:
                    ne = {**e}
                    rel_offset = int(int(e["Time"]) - base_t)
                    ne["Time"] = timeline + rel_offset
                    merged.append(ne)
                
                timeline = merged[-1]["Time"]
                file_end_idx = len(merged) - 1  # Track where this file ends
                file_segments.append({
                    "name": p.name, 
                    "end_time": timeline,
                    "start_idx": file_start_idx,
                    "end_idx": file_end_idx
                })
            
            total_afk_pool = total_idle_movements
            
            if is_inef and not data["is_ts"] and len(merged) > 1:
                p_ms = rng.randint(300000, 720000)
                split = rng.randint(0, len(merged) - 2)
                for j in range(split + 1, len(merged)): merged[j]["Time"] += p_ms
                timeline = merged[-1]["Time"]
                massive_pause_info = f"Massive P1: {format_ms_precise(p_ms)}"
                
                # ✅ FIXED: Update file segment end times based on which events are affected
                for seg in file_segments:
                    # If ANY event in this file segment is after the split, update end time
                    if seg["end_idx"] > split:
                        # The last event of this file is affected by the pause
                        seg["end_time"] = merged[seg["end_idx"]]["Time"]
            
            fname = f"{'¬¬¬' if is_inef else ''}{v_code}_{int(timeline/60000)}m.json"
            (out_f / fname).write_text(json.dumps(merged, indent=2))
            
            total_pause = total_gaps + total_afk_pool
            if massive_pause_info:
                version_label = f"Version {v_code} [EXTRA - INEFFICIENT] (Multiplier: x{mult}):"
            else:
                version_label = f"Version {v_code} (Multiplier: x{mult}):"
            
            manifest_entry = [
                version_label,
                f"  TOTAL DURATION: {format_ms_precise(timeline)}",
                f"  Idle Mouse Movements: {format_ms_precise(total_idle_movements)} ({int(movement_percentage*100)}% of idle time)",
                f"  total PAUSE: {format_ms_precise(total_pause)} +BREAKDOWN:",
                f"    - Inter-file Gaps: {format_ms_precise(total_gaps)}",
                f"    - AFK Pool (Idle Movements): {format_ms_precise(total_afk_pool)}"
            ]
            
            if massive_pause_info:
                manifest_entry.append(f"    - {massive_pause_info}")
            
            manifest_entry.append("")
            
            for idx, seg in enumerate(file_segments):
                bullet = "*" if idx < 11 else "-"
                manifest_entry.append(f"  {bullet} {seg['name']} (Ends at {format_ms_precise(seg['end_time'])})")
            
            manifest_entry.append("-" * 30)
            manifest.append("\n".join(manifest_entry))

        (out_f / f"!_MANIFEST_{folder_number}_!.txt").write_text("\n".join(manifest))

if __name__ == "__main__":
    main()
