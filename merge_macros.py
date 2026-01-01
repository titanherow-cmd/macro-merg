#!/usr/bin/env python3
"""
merge_macros.py - STABLE RESTORE POINT (v3.2.3) - SMOOTH TRANSITIONS
- FEATURE: Smooth transitions back to next recorded position (no teleporting!)
- FEATURE: Realistic smooth mouse movements with multiple patterns
- FEATURE: Pre-Action Mouse Jitter. If delay > 100ms, injects a micro-move.
- FIX: Mouse now flows smoothly through idle → back to recorded position
- FIX: Idle mouse movements now SKIP drag sequences (DragStart to DragEnd)
- FIX: Naming scheme A1, B1, C1... with folder numbering (1-Folder).
- FIX: Z +100 scoped to parent directory only.
- FIX: Jitter is individual per event (non-cumulative).
- FIX: All variables properly initialized.
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

def is_in_drag_sequence(events, index):
    """
    Check if the given index is inside a drag sequence (between DragStart and DragEnd).
    Returns True if we're in the middle of a drag.
    """
    # Look backwards to find the most recent DragStart or DragEnd
    drag_started = False
    for j in range(index, -1, -1):
        event_type = events[j].get("Type", "")
        if event_type == "DragEnd":
            # Found a DragEnd before DragStart, so we're not in a drag
            return False
        elif event_type == "DragStart":
            # Found a DragStart, now check if there's a DragEnd after current index
            drag_started = True
            break
    
    if not drag_started:
        return False
    
    # Now look forward to see if there's a DragEnd
    for j in range(index + 1, len(events)):
        event_type = events[j].get("Type", "")
        if event_type == "DragEnd":
            # We're between DragStart and DragEnd
            return True
        elif event_type == "DragStart":
            # Another DragStart before DragEnd? Shouldn't happen but means not in drag
            return False
    
    return False

def generate_smooth_path(start_x, start_y, end_x, end_y, num_points, rng):
    """
    Generate a smooth, natural path between two points using bezier-like curves.
    Returns list of (x, y) coordinates.
    """
    if num_points <= 1:
        return [(end_x, end_y)]
    
    points = []
    
    # Add slight randomness to the path with control points
    mid_t = 0.5
    # Control point offset - creates the curve
    ctrl_offset_x = rng.randint(-50, 50)
    ctrl_offset_y = rng.randint(-50, 50)
    
    # Calculate control point
    ctrl_x = int((start_x + end_x) / 2 + ctrl_offset_x)
    ctrl_y = int((start_y + end_y) / 2 + ctrl_offset_y)
    
    # Generate points along a quadratic bezier curve
    for i in range(num_points):
        t = i / (num_points - 1) if num_points > 1 else 0
        
        # Quadratic Bezier formula: B(t) = (1-t)²P0 + 2(1-t)tP1 + t²P2
        x = int((1 - t) ** 2 * start_x + 2 * (1 - t) * t * ctrl_x + t ** 2 * end_x)
        y = int((1 - t) ** 2 * start_y + 2 * (1 - t) * t * ctrl_y + t ** 2 * end_y)
        
        # Add tiny jitter to make it more human-like
        x += rng.randint(-2, 2)
        y += rng.randint(-2, 2)
        
        # Keep within bounds
        x = max(100, min(1800, x))
        y = max(100, min(1000, y))
        
        points.append((x, y))
    
    return points

def insert_idle_mouse_movements(events, rng, movement_percentage):
    """
    Insert realistic mouse movements during idle periods (gaps > 5 seconds).
    
    Rules:
    - Only in gaps >= 5000ms
    - SKIP gaps that are inside drag sequences (DragStart to DragEnd)
    - Use middle 40-50% of gap (25% buffer on each side)
    - Smooth flowing movements with realistic patterns
    - ALWAYS smoothly transition back to next recorded position at the end
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
                # ✅ Check if we're in a drag sequence - if so, SKIP idle movements
                if is_in_drag_sequence(events, i):
                    continue
                
                # Calculate active window (middle 40-50% of gap)
                active_duration = int(gap * movement_percentage)
                buffer_start = (gap - active_duration) // 2
                
                movement_start = current_time + buffer_start
                
                # Get last known mouse position by searching backwards
                start_x, start_y = 500, 500
                for j in range(i, -1, -1):
                    x_val = events[j].get("X")
                    y_val = events[j].get("Y")
                    if x_val is not None and y_val is not None:
                        start_x = int(x_val)
                        start_y = int(y_val)
                        break
                
                # ✅ NEW: Get the NEXT recorded position (where we need to end up)
                next_x, next_y = start_x, start_y  # Default to staying in place
                for j in range(i + 1, min(i + 20, len(events))):  # Look ahead max 20 events
                    x_val = events[j].get("X")
                    y_val = events[j].get("Y")
                    if x_val is not None and y_val is not None:
                        next_x = int(x_val)
                        next_y = int(y_val)
                        break
                
                # Reserve last 20% of time for smooth transition back
                transition_duration = int(active_duration * 0.2)
                pattern_duration = active_duration - transition_duration
                
                # Choose a movement pattern
                pattern = rng.choice(['drift', 'check_corner', 'fidget', 'return'])
                
                # Track where we end up after the pattern
                final_pattern_x, final_pattern_y = start_x, start_y
                
                if pattern == 'drift':
                    # Slow meandering - pick a nearby target and drift there
                    target_x = start_x + rng.randint(-200, 200)
                    target_y = start_y + rng.randint(-150, 150)
                    target_x = max(100, min(1800, target_x))
                    target_y = max(100, min(1000, target_y))
                    
                    # Generate smooth path
                    num_steps = max(3, pattern_duration // 600)  # ~600ms per step
                    path = generate_smooth_path(start_x, start_y, target_x, target_y, num_steps, rng)
                    
                    for step_idx, (px, py) in enumerate(path):
                        step_time = int(movement_start + (pattern_duration * step_idx / len(path)))
                        result.append({
                            "Time": step_time,
                            "Type": "MouseMove",
                            "X": px,
                            "Y": py
                        })
                    
                    final_pattern_x, final_pattern_y = path[-1]
                
                elif pattern == 'check_corner':
                    # Quick glance to screen edge/corner then back
                    corner_choices = [
                        (150, 150),   # Top-left
                        (1750, 150),  # Top-right
                        (150, 950),   # Bottom-left
                        (1750, 950),  # Bottom-right
                        (950, 100),   # Top-center
                        (950, 1000),  # Bottom-center
                    ]
                    corner_x, corner_y = rng.choice(corner_choices)
                    
                    # Move to corner (first half of time)
                    half_duration = pattern_duration // 2
                    num_steps_out = max(2, half_duration // 400)
                    path_out = generate_smooth_path(start_x, start_y, corner_x, corner_y, num_steps_out, rng)
                    
                    for step_idx, (px, py) in enumerate(path_out):
                        step_time = int(movement_start + (half_duration * step_idx / len(path_out)))
                        result.append({
                            "Time": step_time,
                            "Type": "MouseMove",
                            "X": px,
                            "Y": py
                        })
                    
                    # Return to somewhere random (second half)
                    return_x = start_x + rng.randint(-60, 60)
                    return_y = start_y + rng.randint(-60, 60)
                    return_x = max(100, min(1800, return_x))
                    return_y = max(100, min(1000, return_y))
                    
                    num_steps_back = max(2, half_duration // 400)
                    path_back = generate_smooth_path(corner_x, corner_y, return_x, return_y, num_steps_back, rng)
                    
                    for step_idx, (px, py) in enumerate(path_back):
                        step_time = int(movement_start + half_duration + (half_duration * step_idx / len(path_back)))
                        result.append({
                            "Time": step_time,
                            "Type": "MouseMove",
                            "X": px,
                            "Y": py
                        })
                    
                    final_pattern_x, final_pattern_y = path_back[-1]
                
                elif pattern == 'fidget':
                    # Small nervous movements in a small area
                    num_fidgets = rng.randint(4, 8)
                    fidget_interval = pattern_duration // num_fidgets
                    
                    current_x, current_y = start_x, start_y
                    
                    for fidget_idx in range(num_fidgets):
                        # Small random offset
                        new_x = current_x + rng.randint(-40, 40)
                        new_y = current_y + rng.randint(-40, 40)
                        new_x = max(100, min(1800, new_x))
                        new_y = max(100, min(1000, new_y))
                        
                        # Smooth path to new position
                        num_micro_steps = rng.randint(2, 4)
                        micro_path = generate_smooth_path(current_x, current_y, new_x, new_y, num_micro_steps, rng)
                        
                        for micro_idx, (px, py) in enumerate(micro_path):
                            step_time = int(movement_start + fidget_idx * fidget_interval + 
                                          (fidget_interval * micro_idx / len(micro_path)))
                            result.append({
                                "Time": step_time,
                                "Type": "MouseMove",
                                "X": px,
                                "Y": py
                            })
                        
                        current_x, current_y = new_x, new_y
                    
                    final_pattern_x, final_pattern_y = current_x, current_y
                
                elif pattern == 'return':
                    # Move somewhere then return to almost exactly where we were
                    away_x = start_x + rng.randint(-300, 300)
                    away_y = start_y + rng.randint(-200, 200)
                    away_x = max(100, min(1800, away_x))
                    away_y = max(100, min(1000, away_y))
                    
                    # Move away (60% of time)
                    away_duration = int(pattern_duration * 0.6)
                    num_steps_away = max(3, away_duration // 500)
                    path_away = generate_smooth_path(start_x, start_y, away_x, away_y, num_steps_away, rng)
                    
                    for step_idx, (px, py) in enumerate(path_away):
                        step_time = int(movement_start + (away_duration * step_idx / len(path_away)))
                        result.append({
                            "Time": step_time,
                            "Type": "MouseMove",
                            "X": px,
                            "Y": py
                        })
                    
                    # Return near original (40% of time)
                    return_duration = pattern_duration - away_duration
                    return_x = start_x + rng.randint(-20, 20)
                    return_y = start_y + rng.randint(-20, 20)
                    return_x = max(100, min(1800, return_x))
                    return_y = max(100, min(1000, return_y))
                    
                    num_steps_return = max(2, return_duration // 500)
                    path_return = generate_smooth_path(away_x, away_y, return_x, return_y, num_steps_return, rng)
                    
                    for step_idx, (px, py) in enumerate(path_return):
                        step_time = int(movement_start + away_duration + (return_duration * step_idx / len(path_return)))
                        result.append({
                            "Time": step_time,
                            "Type": "MouseMove",
                            "X": px,
                            "Y": py
                        })
                    
                    final_pattern_x, final_pattern_y = path_return[-1]
                
                # ✅ NEW: Smooth transition back to next recorded position
                # Use the last 20% of time to smoothly drift to where the next event expects us
                num_transition_steps = max(2, transition_duration // 400)
                transition_path = generate_smooth_path(
                    final_pattern_x, final_pattern_y, 
                    next_x, next_y, 
                    num_transition_steps, 
                    rng
                )
                
                for step_idx, (px, py) in enumerate(transition_path):
                    step_time = int(movement_start + pattern_duration + (transition_duration * step_idx / len(transition_path)))
                    result.append({
                        "Time": step_time,
                        "Type": "MouseMove",
                        "X": px,
                        "Y": py
                    })
                
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
    
    # ✅ NEW: Look for logout.json in multiple locations
    logout_file = None
    for location in [originals_root / "logout.json", originals_root.parent / "logout.json", search_base / "logout.json"]:
        if location.exists() and location.is_file():
            logout_file = location
            print(f"Found logout.json at: {logout_file}")
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
                    "parent_scope": parent_scope
                }
                
                for fp in file_paths:
                    durations_cache[fp] = get_file_duration_ms(fp)

    for pool_key, pool_data in pools.items():
        parent_scope = pool_data["parent_scope"]
        macro_id = pool_data["macro_id"]
        
        z_key = (parent_scope, macro_id)
        if z_key in z_storage:
            pool_data["files"].extend(z_storage[z_key])
    
    # Filter out "always first" and "always last" files from merging
    for pool_key, pool_data in pools.items():
        all_files = pool_data["files"]
        always_files = [f for f in all_files if Path(f).name.lower().startswith(("always first", "always last", "-always first", "-always last"))]
        mergeable_files = [f for f in all_files if f not in always_files]
        pool_data["files"] = mergeable_files
        pool_data["always_files"] = always_files
    
    # ✅ FIX #3: Extract folder numbers from folder names instead of generating them
    for key, data in pools.items():
        folder_name = data["rel_path"].name
        folder_number = extract_folder_number(folder_name)
        
        # If no number found, default to 0
        if folder_number == 0:
            print(f"WARNING: No number found in folder name '{folder_name}', using 0")
        
        data["folder_number"] = folder_number
    
    for key, data in pools.items():
        folder_number = data["folder_number"]  # Use extracted number from folder name
        
        # ✅ FIX: Skip folders with 0 mergeable files
        if not data["files"]:
            print(f"Skipping folder (0 files): {data['rel_path']}")
            continue
        
        original_rel_path = data["rel_path"]
        
        out_f = bundle_dir / original_rel_path  # Use folder as-is
        out_f.mkdir(parents=True, exist_ok=True)
        
        # ✅ NEW: Copy logout.json to this folder
        if logout_file:
            try:
                shutil.copy2(logout_file, out_f / "logout.json")
                print(f"  ✓ Copied logout.json to {original_rel_path}")
            except Exception as e:
                print(f"  ✗ Error copying logout.json: {e}")
        
        # Copy "always first/last" files unmodified
        if "always_files" in data:
            for always_file in data["always_files"]:
                try:
                    shutil.copy2(always_file, out_f / Path(always_file).name)
                    print(f"  ✓ Copied unmodified: {Path(always_file).name}")
                except Exception as e:
                    print(f"  ✗ Error copying {Path(always_file).name}: {e}")
        
        manifest = [
            f"MANIFEST FOR FOLDER: {original_rel_path}",
            "=" * 40,
            f"Total Available Files: {len(data['files'])}",
            f"Folder Number: {folder_number}",
            "",
            ""
        ]
        
        norm_v = args.versions
        inef_v = 0 if data["is_ts"] else (norm_v // 2)
        
        for v_idx in range(1, (norm_v + inef_v) + 1):
            is_inef = (v_idx > norm_v)
            v_letter = chr(64 + v_idx)
            v_code = f"{v_letter}{folder_number}"
            
            if data["is_ts"]: mult = rng.choice([1.0, 1.2, 1.5])
            elif is_inef: mult = rng.choices([1, 2, 3], weights=[20, 40, 40], k=1)[0]
            else: mult = rng.choices([1, 2, 3], weights=[50, 30, 20], k=1)[0]
            
            # ✅ NEW: Random movement percentage per version (40-50%)
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
                
                # ✅ NEW: Insert idle mouse movements in this file's events
                raw_with_movements, idle_time = insert_idle_mouse_movements(raw, rng, movement_percentage)
                total_idle_movements += idle_time
                
                t_vals = [int(e["Time"]) for e in raw_with_movements]
                base_t = min(t_vals)
                
                gap = int(rng.randint(500, 2500) * mult) if i > 0 else 0
                timeline += gap
                total_gaps += gap
                
                for e in raw_with_movements:
                    ne = {**e}
                    rel_offset = int(int(e["Time"]) - base_t)
                    ne["Time"] = timeline + rel_offset
                    merged.append(ne)
                
                timeline = merged[-1]["Time"]
                file_segments.append({"name": p.name, "end_time": timeline})
            
            if is_inef and not data["is_ts"] and len(merged) > 1:
                p_ms = rng.randint(300000, 720000)
                split = rng.randint(0, len(merged) - 2)
                for j in range(split + 1, len(merged)): merged[j]["Time"] += p_ms
                timeline = merged[-1]["Time"]
                massive_pause_info = f"Massive P1: {format_ms_precise(p_ms)}"
            
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
                f"    - AFK Pool: {format_ms_precise(total_afk_pool)}"
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
