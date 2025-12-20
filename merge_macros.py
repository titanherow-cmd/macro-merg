#!/usr/bin/env python3
"""merge_macros.py - Advanced Pause Logic with Jittered Delay-Before-Action (-18/+19ms)"""

from pathlib import Path
import argparse, json, random, re, sys, os, math, shutil
from copy import deepcopy

COUNTER_PATH = Path(".github/merge_bundle_counter.txt")
SPECIAL_FILENAME = "close reopen mobile screensharelink.json"
SPECIAL_KEYWORD = "screensharelink"

# ==============================================================================
# CORE HELPERS
# ==============================================================================

def read_counter(path: Path) -> int:
    try:
        return int(path.read_text(encoding="utf-8").strip()) if path.exists() else 1
    except: return 1

def write_counter(path: Path, n: int):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(n), encoding="utf-8")

def find_all_dirs_with_json(input_root: Path):
    found = []
    if not input_root.exists(): return found
    for p in sorted(input_root.rglob("*")):
        if p.is_dir():
            if any(child.suffix.lower() == ".json" for child in p.iterdir() if child.is_file()):
                found.append(p)
    return found

def find_json_files_in_dir(dirpath: Path):
    return sorted([p for p in dirpath.glob("*.json") if p.is_file() and "click_zones" not in p.name])

def load_json_events(path: Path):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            for k in ("events", "items", "entries", "records"):
                if k in data and isinstance(data[k], list): return deepcopy(data[k])
            return [data] if "Time" in data else []
        return deepcopy(data) if isinstance(data, list) else []
    except: return []

def get_file_duration_ms(path: Path) -> int:
    events = load_json_events(path)
    if not events: return 0
    try:
        times = [int(e.get("Time", 0)) for e in events]
        return max(times) - min(times)
    except: return 0

def format_ms_precise(ms: int) -> str:
    total_seconds = int(ms / 1000)
    minutes = total_seconds // 60
    seconds = total_seconds % 60
    return f"{minutes}.Min {seconds}.Sec"

def process_macro_file(events: list[dict]) -> tuple[list[dict], int]:
    if not events: return [], 0
    try:
        times = [int(e.get("Time", 0)) for e in events]
        min_t = min(times)
        shifted = []
        for e in events:
            ne = deepcopy(e)
            ne["Time"] = int(e.get("Time", 0)) - min_t
            shifted.append(ne)
        duration = shifted[-1]["Time"] if shifted else 0
        return shifted, duration
    except: return [], 0

def preserve_click_integrity(events):
    preserved = []
    for e in events:
        ne = deepcopy(e)
        if any(t in str(e.get('Type', '')) for t in ['Down', 'Up', 'Click', 'Button', 'Drag']):
            ne['PROTECTED'] = True
        preserved.append(ne)
    return preserved

def number_to_letters(n: int) -> str:
    res = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        res = chr(65 + rem) + res
    return res or "A"

# ==============================================================================
# PAUSE & ANTI-DETECTION LOGIC
# ==============================================================================

def apply_delay_before_action(events, delay_ms, rng):
    """
    Calculates a pool (20-30% of duration) and inserts delay_ms chunks
    randomly before actions. Applies -18ms to +19ms jitter to each chunk.
    """
    if delay_ms <= 0 or not events: return events, 0
    
    original_duration = events[-1]['Time'] - events[0]['Time']
    # Choose pool percentage between 20% and 30%
    pool_pct = rng.uniform(20, 30)
    total_delay_pool = int(original_duration * (pool_pct / 100))
    
    # Estimate insertions based on the base delay_ms
    num_insertions = total_delay_pool // delay_ms
    if num_insertions <= 0: return events, 0
    
    modified_events = deepcopy(events)
    total_added = 0
    
    for _ in range(num_insertions):
        idx = rng.randint(1, len(modified_events) - 1)
        # Apply the specific -18 to +19 jitter logic
        jitter = rng.randint(-18, 19)
        actual_chunk = delay_ms + jitter
        
        # Ensure we don't accidentally create a negative time shift (safety check)
        if actual_chunk < 0: actual_chunk = 0
        
        for i in range(idx, len(modified_events)):
            modified_events[i]['Time'] += actual_chunk
        total_added += actual_chunk
            
    return modified_events, total_added

def apply_intra_file_pauses(events, rng):
    if not events: return events, 0
    
    choices = [0, 8, 15, 21, 25.5, 29]
    weights = [40, 20, 15, 10, 10, 5]
    pct = rng.choices(choices, weights=weights, k=1)[0]
    
    if pct == 0:
        return events, 0

    original_duration = events[-1]['Time'] - events[0]['Time']
    total_pause_needed = int(original_duration * (pct / 100))
    
    if total_pause_needed <= 0:
        return events, 0

    num_chunks = rng.randint(3, 6)
    chunk_sizes = []
    remaining = total_pause_needed
    for i in range(num_chunks - 1):
        c = rng.randint(1, remaining // 2) if remaining > 2 else 1
        chunk_sizes.append(c)
        remaining -= c
    chunk_sizes.append(remaining)

    modified_events = deepcopy(events)
    actual_added_ms = 0
    
    for pause_amt in chunk_sizes:
        idx = rng.randint(1, len(modified_events) - 2)
        # Standard jitter for probability pauses
        jitter = rng.randint(1, 49) 
        actual_pause = pause_amt + jitter
        actual_added_ms += actual_pause
        
        for i in range(idx, len(modified_events)):
            modified_events[i]['Time'] += actual_pause
            
    return modified_events, actual_added_ms

def add_mouse_jitter(events, rng):
    jittered = []
    for e in events:
        ne = deepcopy(e)
        if not e.get('PROTECTED') and 'X' in e and 'Y' in e:
            try:
                ne['X'] = int(e['X']) + rng.randint(-1, 1)
                ne['Y'] = int(e['Y']) + rng.randint(-1, 1)
            except: pass
        jittered.append(ne)
    return jittered

def add_reaction_variance(events, rng):
    varied = []
    offset = 0
    for e in events:
        ne = deepcopy(e)
        if not e.get('PROTECTED') and rng.random() < 0.15:
            offset += rng.randint(-30, 30) + (rng.random() * 2 - 1)
        ne['Time'] = max(0, int(e.get('Time', 0)) + int(offset))
        varied.append(ne)
    return varied

def merge_events_with_pauses(base: list[dict], new: list[dict], pause_ms: int) -> list[dict]:
    if not new: return base
    last_t = base[-1]['Time'] if base else 0
    shift = last_t + pause_ms
    shifted = deepcopy(new)
    for e in shifted: e['Time'] = int(e.get('Time', 0)) + shift
    return base + shifted

# ==============================================================================
# ROUND-ROBIN SELECTOR
# ==============================================================================

class QueueFileSelector:
    def __init__(self, rng, all_files):
        self.rng = rng
        self.all_files = [str(f.resolve()) for f in all_files]
        self.pool = list(self.all_files)
        self.rng.shuffle(self.pool)
        
    def get_files_for_time(self, target_minutes):
        selected = []
        current_ms = 0.0
        target_ms = target_minutes * 60000
        
        while current_ms < target_ms:
            if not self.pool:
                self.pool = list(self.all_files)
                self.rng.shuffle(self.pool)
            
            pick = None
            for f in self.pool:
                if f not in selected:
                    pick = f
                    break
            
            if not pick: pick = self.pool[0]
            
            dur = get_file_duration_ms(Path(pick))
            if dur <= 0 and len(self.all_files) > 1:
                if pick in self.pool: self.pool.remove(pick)
                continue

            selected.append(pick)
            if pick in self.pool: self.pool.remove(pick)
            
            # Use 45% as safe upper bound for target calculation (Inc. new delays)
            current_ms += (dur * 1.45) + 300
            if len(selected) > 100: break 
            
        return selected

# ==============================================================================
# MAIN LOGIC
# ==============================================================================

def generate_version_for_folder(rng, v_num, folder, selector, target_min, delay_before_ms):
    selected_paths = selector.get_files_for_time(target_min)
    if not selected_paths: return None, None, None
    
    selected_paths.sort(key=lambda x: (
        0 if "always first" in Path(x).name.lower() else 
        2 if "always last" in Path(x).name.lower() else 1
    ))

    all_evs = []
    manifest_lines = []
    total_inter_pause_ms = 0
    total_internal_pause_ms = 0
    
    for i, path_str in enumerate(selected_paths):
        p = Path(path_str)
        is_special = SPECIAL_KEYWORD in p.name.lower() or p.name.lower() == SPECIAL_FILENAME
        
        raw_evs, _ = process_macro_file(load_json_events(p))
        if not raw_evs: continue
        
        evs = preserve_click_integrity(raw_evs)
        added_internal_ms = 0
        added_delay_ms = 0
        
        if not is_special:
            evs, added_internal_ms = apply_intra_file_pauses(evs, rng)
            # Use the new jittered delay logic
            evs, added_delay_ms = apply_delay_before_action(evs, delay_before_ms, rng)
            
            evs = add_mouse_jitter(evs, rng)
            evs = add_reaction_variance(evs, rng)
        
        total_internal_pause_ms += (added_internal_ms + added_delay_ms)
        
        inter_pause = 0
        if i > 0:
            inter_pause = rng.randint(100, 500)
            inter_pause += rng.randint(1, 9) 
            total_inter_pause_ms += inter_pause
                
        all_evs = merge_events_with_pauses(all_evs, evs, inter_pause)
        
        letter = number_to_letters(i+1)
        seg_dur = evs[-1]['Time'] - evs[0]['Time']
        running_total_ms = all_evs[-1]['Time']
        
        manifest_lines.append(
            f"  {letter}: {p.name} | Dur: {format_ms_precise(seg_dur)} (Inc. {format_ms_precise(added_internal_ms + added_delay_ms)} Random Gaps) | Total: {format_ms_precise(running_total_ms)}"
        )
    
    if not all_evs: return None, None, None
    
    total_ms = all_evs[-1]['Time'] if all_evs else 0
    total_dur_str = format_ms_precise(total_ms)
    
    v_code = number_to_letters(v_num)
    final_min_only = int(total_ms / 60000)
    fname = f"{v_code}_{final_min_only}m.json"
    
    manifest_entry = (
        f"FILENAME: {fname}\n"
        f"TOTAL DURATION: {total_dur_str}\n"
        f"TOTAL COMBINED AFK: {format_ms_precise(total_inter_pause_ms + total_internal_pause_ms)}\n"
        f"  ├─ Internal delays/pauses: {format_ms_precise(total_internal_pause_ms)}\n"
        f"  └─ Inter-file gaps: {format_ms_precise(total_inter_pause_ms)}\n"
        f"COMPONENTS:\n" + "\n".join(manifest_lines) + "\n" + "-"*40
    )
    
    return fname, all_evs, manifest_entry

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_root", type=Path)
    parser.add_argument("output_root", type=Path)
    parser.add_argument("--versions", type=int, default=6)
    parser.add_argument("--target-minutes", type=int, default=25)
    parser.add_argument("--delay-before-action-ms", type=int, default=0)
    parser.add_argument("--between-max-time", type=int, default=0)
    parser.add_argument("--exclude-count", type=int, default=0) 
    args = parser.parse_args()

    rng = random.Random()
    bundle_n = read_counter(COUNTER_PATH)
    bundle_dir = args.output_root / f"merged_bundle_{bundle_n}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Starting merge for bundle {bundle_n}...")
    
    folders = find_all_dirs_with_json(args.input_root)
    for folder in folders:
        files = find_json_files_in_dir(folder)
        if not files: continue
        
        rel_path = folder.relative_to(args.input_root)
        out_folder = bundle_dir / rel_path
        out_folder.mkdir(parents=True, exist_ok=True)
        
        selector = QueueFileSelector(rng, files)
        folder_manifest = [f"MANIFEST FOR FOLDER: {rel_path}\n{'='*40}\n"]
        
        for v in range(1, args.versions + 1):
            fname, evs, m_entry = generate_version_for_folder(
                rng, v, folder, selector, args.target_minutes, args.delay_before_action_ms
            )
            if fname and evs:
                (out_folder / fname).write_text(json.dumps(evs, indent=2), encoding="utf-8")
                folder_manifest.append(m_entry)
        
        if len(folder_manifest) > 1:
            (out_folder / "manifest.txt").write_text("\n\n".join(folder_manifest), encoding="utf-8")
                
    write_counter(COUNTER_PATH, bundle_n + 1)

if __name__ == "__main__":
    main()
