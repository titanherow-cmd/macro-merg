#!/usr/bin/env python3
"""merge_macros.py - AFK Priority Logic: Normal (x1, x2, x3), TS (x1, x1.2, x1.5), No Inefficient Versions, Scoped Z +100 Pooling"""

from pathlib import Path
import argparse, json, random, sys, os, math, shutil, re
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
    if ms < 1000 and ms > 0:
        return f"{ms}ms"
    total_seconds = int(round(ms / 1000))
    minutes = total_seconds // 60
    seconds = total_seconds % 60
    if minutes == 0:
        return f"{seconds}s"
    return f"{minutes}m {seconds}s"

def round_to_sec(ms: int) -> int:
    return int(round(ms / 1000.0) * 1000)

def number_to_letters(n: int) -> str:
    res = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        res = chr(65 + rem) + res
    return res or "A"

def clean_identity(name: str) -> str:
    """Standardizes names for matching purposes."""
    # Remove common suffixes like '- Copy', '(1)', etc.
    name = re.sub(r'(\s*-\s*Copy(\s*\(\d+\))?)|(\s*\(\d+\))', '', name, flags=re.IGNORECASE).strip()
    return name.lower()

class QueueFileSelector:
    def __init__(self, rng, all_mergeable_files):
        self.rng = rng
        self.pool_src = [f for f in all_mergeable_files]
        self.pool = list(self.pool_src)
        self.rng.shuffle(self.pool)
        
    def get_sequence(self, target_minutes):
        sequence = []
        current_ms = 0.0
        target_ms = target_minutes * 60000
        if not self.pool_src: return []
        while current_ms < target_ms:
            if not self.pool:
                self.pool = list(self.pool_src)
                self.rng.shuffle(self.pool)
            pick = self.pool.pop(0)
            sequence.append(str(pick.resolve()))
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
    parser.add_argument("--speed-range", type=str, default="1.0 1.0")
    args, unknown = parser.parse_known_args()

    try:
        parts = args.speed_range.replace(',', ' ').split()
        s_min = float(parts[0])
        s_max = float(parts[1]) if len(parts) > 1 else s_min
    except:
        s_min, s_max = 1.0, 1.0

    base_dir = args.input_root
    if not (base_dir / "originals").exists():
        base_dir = Path(".")
    
    if not (base_dir / "originals").exists():
        print(f"CRITICAL ERROR: 'originals' folder not found in {base_dir.resolve()}")
        sys.exit(1)
    
    rng = random.Random()
    bundle_dir = args.output_root / f"merged_bundle_{args.bundle_id}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    
    unified_pools = {}
    
    # 1. SCAN ORIGINALS FIRST (These define our output structure)
    originals_root = base_dir / "originals"
    print(f"--- SCANNING ORIGINALS: {originals_root} ---")
    for p in originals_root.rglob("*.json"):
        if "output" in p.parts or p.name.startswith('.') or "manifest" in p.name.lower(): continue
        if any(x in p.name.lower() for x in ["click_zones", "first", "last"]): continue
        
        rel_parts = p.parent.relative_to(originals_root).parts
        if len(rel_parts) >= 2:
            game_name = rel_parts[0]
            macro_folder = rel_parts[1]
        elif len(rel_parts) == 1:
            game_name = "General"
            macro_folder = rel_parts[0]
        else:
            game_name = "General"
            macro_folder = "Root"

        # Identity is based on CLEANED names
        key = (clean_identity(game_name), clean_identity(macro_folder))
        
        if key not in unified_pools:
            unified_pools[key] = {
                "out_rel_path": Path(game_name) / macro_folder,
                "display_name": f"{game_name} / {macro_folder}",
                "files": [],
                "is_ts": "time sensitive" in macro_folder.lower(),
                "source_folders": []
            }
        
        unified_pools[key]["files"].append(p)
        if p.parent not in unified_pools[key]["source_folders"]:
            unified_pools[key]["source_folders"].append(p.parent)

    # 2. SCAN SUPPLEMENTAL FOLDERS (Any folder starting with Z)
    print(f"--- SCANNING SUPPLEMENTAL STORAGE (Z folders) ---")
    # Search for any directory in root that starts with 'Z'
    for z_folder in base_dir.iterdir():
        if z_folder.is_dir() and z_folder.name.upper().startswith('Z'):
            print(f" Found supplemental source: {z_folder.name}")
            for p in z_folder.rglob("*.json"):
                if "output" in p.parts or p.name.startswith('.'): continue
                
                # Determine where this would fit in the 'originals' structure
                rel_parts = p.parent.relative_to(z_folder).parts
                if len(rel_parts) >= 2:
                    z_game = rel_parts[0]
                    z_macro = rel_parts[1]
                elif len(rel_parts) == 1:
                    z_game = "General"
                    z_macro = rel_parts[0]
                else:
                    z_game = "General"
                    z_macro = "Root"
                
                z_key = (clean_identity(z_game), clean_identity(z_macro))
                
                # ONLY add if it matches an existing pool from 'originals'
                if z_key in unified_pools:
                    unified_pools[z_key]["files"].append(p)
                    if p.parent not in unified_pools[z_key]["source_folders"]:
                        unified_pools[z_key]["source_folders"].append(p.parent)

    if not unified_pools:
        print("CRITICAL ERROR: No macro files found!")
        sys.exit(1)

    # 3. PROCESS POOLS
    for key, data in unified_pools.items():
        mergeable_files = data["files"]
        out_rel = data["out_rel_path"]
        out_folder = bundle_dir / out_rel
        out_folder.mkdir(parents=True, exist_ok=True)
        
        print(f"Processing: {data['display_name']} ({len(mergeable_files)} files)")
        
        # Copy non-json assets
        for src in data["source_folders"]:
            for item in src.iterdir():
                if item.is_file() and not item.name.endswith(".json"):
                    shutil.copy2(item, out_folder / item.name)
                elif item.is_file() and "click_zones" in item.name.lower():
                    shutil.copy2(item, out_folder / item.name)

        selector = QueueFileSelector(rng, mergeable_files)
        folder_manifest = [f"MANIFEST: {data['display_name']}\nTotal Pool: {len(mergeable_files)}\n"]

        for v_num in range(1, args.versions + 1):
            afk_multiplier = rng.choice([1.0, 1.2, 1.5]) if data["is_ts"] else rng.choice([1, 2, 3])
            selected_paths = selector.get_sequence(args.target_minutes)
            if not selected_paths: continue
            
            speed = rng.uniform(s_min, s_max)
            merged_events = []
            timeline_ms = 0
            file_segments = []

            for i, p_str in enumerate(selected_paths):
                p = Path(p_str)
                raw = load_json_events(p)
                if not raw: continue
                
                t_vals = [int(e.get("Time", 0)) for e in raw]
                base_t = min(t_vals) if t_vals else 0
                dur = (max(t_vals) - base_t) if t_vals else 0
                
                gap = round_to_sec((rng.randint(500, 2500) if i > 0 else 0) * afk_multiplier)
                timeline_ms += gap
                
                start_idx = len(merged_events)
                for e in raw:
                    ne = deepcopy(e)
                    ne["Time"] = int((int(e.get("Time", 0)) - base_t) * speed + timeline_ms)
                    merged_events.append(ne)
                
                file_segments.append({"name": p.name, "end_time": merged_events[-1]["Time"]})
                timeline_ms = merged_events[-1]["Time"]

            v_code = number_to_letters(v_num)
            fname = f"{v_code}_{int(timeline_ms / 60000)}m.json"
            (out_folder / fname).write_text(json.dumps(merged_events, indent=2))
            
            manifest_entry = [f"Version {v_code} (x{afk_multiplier}): {format_ms_precise(timeline_ms)}"]
            for seg in file_segments:
                manifest_entry.append(f"  - {seg['name']}")
            folder_manifest.append("\n".join(manifest_entry))

        (out_folder / "manifest.txt").write_text("\n\n".join(folder_manifest))

    print(f"--- Bundle {args.bundle_id} complete ---")

if __name__ == "__main__":
    main()
