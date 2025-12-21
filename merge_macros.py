#!/usr/bin/env python3
"""merge_macros.py - Discovery with Accurate Timings, Asset Preservation (including first/last files)"""

from pathlib import Path
import argparse, json, random, sys, os, math, shutil
from copy import deepcopy

def load_json_events(path: Path):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            for k in ("events", "items", "entries", "records"):
                if k in data and isinstance(data[k], list): return deepcopy(data[k])
            return [data] if "Time" in data else []
        return deepcopy(data) if isinstance(data, list) else []
    except Exception as e:
        print(f"Error loading {path.name}: {e}")
        return []

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
    return f"{minutes}m {seconds}s"

def number_to_letters(n: int) -> str:
    res = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        res = chr(65 + rem) + res
    return res or "A"

class QueueFileSelector:
    """Handles logic for the pool of mergeable files."""
    def __init__(self, rng, all_mergeable_files):
        self.rng = rng
        self.pool_src = [f for f in all_mergeable_files]
        self.pool = list(self.pool_src)
        self.rng.shuffle(self.pool)
        
    def get_sequence(self, target_minutes):
        sequence = []
        current_ms = 0.0
        target_ms = target_minutes * 60000
        
        if not self.pool_src:
            return []

        while current_ms < target_ms:
            if not self.pool:
                self.pool = list(self.pool_src)
                self.rng.shuffle(self.pool)
            
            pick = self.pool.pop(0)
            sequence.append(str(pick.resolve()))
            # Estimate: Duration + 30% buffer + 1.5s gap
            current_ms += (get_file_duration_ms(pick) * 1.3) + 1500
            
            if len(sequence) > 150: break 
        
        return sequence

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_root", type=Path)
    parser.add_argument("output_root", type=Path)
    parser.add_argument("--versions", type=int, default=6)
    parser.add_argument("--target-minutes", type=int, default=25)
    parser.add_argument("--delay-before-action-ms", type=int, default=10)
    parser.add_argument("--bundle-id", type=int, required=True)
    args, unknown = parser.parse_known_args()

    search_root = args.input_root
    if not search_root.exists():
        if Path("originals").exists():
            search_root = Path("originals")
        else:
            search_root = Path(".")

    rng = random.Random()
    bundle_dir = args.output_root / f"merged_bundle_{args.bundle_id}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"--- DEBUG: SEARCHING FOR MACROS ---")
    
    folders_with_json = []
    seen_folders = set()
    
    # 1. Identify which files are "special" and which are for merging
    for p in search_root.rglob("*.json"):
        # Exclude output or hidden files
        if "output" in p.parts or p.name.startswith('.'):
            continue
            
        # Define what makes a file "Special" (Excluded from merge)
        is_special = any(x in p.name.lower() for x in ["click_zones", "first", "last"])
        
        if not is_special and p.parent not in seen_folders:
            folder = p.parent
            # Only JSONs that are NOT special go into the merge pool
            mergeable_jsons = sorted([
                f for f in folder.glob("*.json") 
                if not any(x in f.name.lower() for x in ["click_zones", "first", "last"])
            ])
            if mergeable_jsons:
                folders_with_json.append((folder, mergeable_jsons))
                seen_folders.add(folder)
                print(f"Found group: {folder.relative_to(search_root)}")

    if not folders_with_json:
        print(f"CRITICAL ERROR: No mergeable JSON files found!")
        sys.exit(1)

    for folder_path, mergeable_files in folders_with_json:
        rel_path = folder_path.relative_to(search_root)
        out_folder = bundle_dir / rel_path
        out_folder.mkdir(parents=True, exist_ok=True)
        
        # --- PRESERVE ALL ASSETS ---
        # Includes .png, click_zones, and now 'first'/'last' files
        for item in folder_path.iterdir():
            if item.is_file():
                # If it's not in our mergeable list, copy it exactly as is
                if item not in mergeable_files:
                    shutil.copy2(item, out_folder / item.name)
        
        selector = QueueFileSelector(rng, mergeable_files)
        folder_manifest = [f"MANIFEST FOR FOLDER: {rel_path}\n{'='*40}\n"]

        for v in range(1, args.versions + 1):
            selected_paths = selector.get_sequence(args.target_minutes)
            if not selected_paths: continue
            
            merged_events = []
            timeline_ms = 0
            accumulated_afk = 0
            total_gaps = 0
            is_time_sensitive = "time sensitive" in str(folder_path).lower()
            
            file_segments = []

            for i, p_str in enumerate(selected_paths):
                p = Path(p_str)
                raw = load_json_events(p)
                if not raw: continue
                
                t_vals = [int(e.get("Time", 0)) for e in raw]
                base_t = min(t_vals) if t_vals else 0
                dur = (max(t_vals) - base_t) if t_vals else 0
                
                gap = rng.randint(500, 2500) if i > 0 else 0
                timeline_ms += gap
                total_gaps += gap
                
                start_in_merge = len(merged_events)
                for e in raw:
                    ne = deepcopy(e)
                    ne["Time"] = (int(e.get("Time", 0)) - base_t) + timeline_ms
                    merged_events.append(ne)
                end_in_merge = len(merged_events) - 1
                
                file_segments.append({
                    "name": p.name,
                    "start_idx": start_in_merge,
                    "end_idx": end_in_merge
                })
                
                if "screensharelink" not in p.name.lower():
                    pct = rng.choice([0, 0, 0, 0.12, 0.20, 0.28])
                    accumulated_afk += int(dur * pct)
                
                timeline_ms = merged_events[-1]["Time"]

            # Apply AFK Pool
            shift_idx = len(merged_events)
            if accumulated_afk > 0:
                if not is_time_sensitive:
                    shift_idx = rng.randint(1, len(merged_events) - 1)
                
                for k in range(shift_idx, len(merged_events)):
                    merged_events[k]["Time"] += accumulated_afk

            manifest_entry = [f"Version {number_to_letters(v)}:"]
            for seg in file_segments:
                actual_end_time = merged_events[seg["end_idx"]]["Time"]
                manifest_entry.append(f"  - {seg['name']} (Ends at {format_ms_precise(actual_end_time)})")

            final_dur = merged_events[-1]["Time"]
            v_code = number_to_letters(v)
            fname = f"{v_code}_{int(final_dur / 60000)}m.json"
            
            (out_folder / fname).write_text(json.dumps(merged_events, indent=2))
            
            manifest_entry.append(f"  TOTAL DURATION: {format_ms_precise(final_dur)}")
            manifest_entry.append(f"  HUMANIZATION ADDED (AFK/Gaps): {format_ms_precise(accumulated_afk + total_gaps)}")
            manifest_entry.append("-" * 20)
            folder_manifest.append("\n".join(manifest_entry))

        (out_folder / "manifest.txt").write_text("\n\n".join(folder_manifest))
        print(f"Processed folder: {rel_path}")

    print(f"--- SUCCESS: Bundle {args.bundle_id} created ---")

if __name__ == "__main__":
    main()
