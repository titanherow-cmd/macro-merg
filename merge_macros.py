#!/usr/bin/env python3
"""merge_macros.py - Weighted Multipliers (50/30/20), Deep Nesting Support, Detailed Manifests, Scoped Z +100 Pooling, Root Logout Support"""

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
    parser.add_argument("--target-minutes", type=int, default=35)
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
        print(f"CRITICAL ERROR: 'originals' folder not found.")
        sys.exit(1)

    logout_file = base_dir / "logout.json"
    if not logout_file.exists():
        logout_file = base_dir.parent / "logout.json"

    rng = random.Random()
    bundle_dir = args.output_root / f"merged_bundle_{args.bundle_id}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    
    unified_pools = {}
    originals_root = base_dir / "originals"

    for game_folder in originals_root.iterdir():
        if not game_folder.is_dir(): continue
        game_name_clean = clean_identity(game_folder.name)
        z_sources = [sub for sub in game_folder.iterdir() if sub.is_dir() and sub.name.upper().startswith('Z')]
        
        for root, dirs, files in os.walk(game_folder):
            current_path = Path(root)
            if any(part.upper().startswith('Z') for part in current_path.relative_to(game_folder).parts):
                continue
            json_files = [f for f in files if f.endswith(".json") and f.lower() != "logout.json" and "click_zones" not in f.lower() and "manifest" not in f.lower()]
            if not json_files: continue
                
            macro_rel_path = current_path.relative_to(game_folder)
            macro_id = clean_identity(current_path.name)
            key = (game_name_clean, str(macro_rel_path).lower())
            
            if key not in unified_pools:
                unified_pools[key] = {
                    "out_rel_path": Path(game_folder.name) / macro_rel_path,
                    "display_name": f"{game_folder.name} / {macro_rel_path}",
                    "files": [],
                    "is_ts": "time sensitive" in str(macro_rel_path).lower(),
                    "source_folders": [current_path],
                    "macro_name_only": macro_id
                }
            for f in json_files:
                unified_pools[key]["files"].append(current_path / f)

        for z_src in z_sources:
            for root, dirs, files in os.walk(z_src):
                z_path = Path(root)
                z_macro_id = clean_identity(z_path.name)
                for pool_key, pool_data in unified_pools.items():
                    if pool_data["macro_name_only"] == z_macro_id:
                        for f in files:
                            if f.endswith(".json") and "click_zones" not in f.lower():
                                pool_data["files"].append(z_path / f)
                        if z_path not in pool_data["source_folders"]:
                            pool_data["source_folders"].append(z_path)

    if not unified_pools:
        print("CRITICAL ERROR: No macro pools identified.")
        sys.exit(1)

    for key, data in unified_pools.items():
        mergeable_files = data["files"]
        if not mergeable_files: continue
        out_folder = bundle_dir / data["out_rel_path"]
        out_folder.mkdir(parents=True, exist_ok=True)
        
        if logout_file and logout_file.exists():
            shutil.copy2(logout_file, out_folder / "logout.json")

        for src in data["source_folders"]:
            for item in src.iterdir():
                if item.is_file() and (not item.name.endswith(".json") or "click_zones" in item.name.lower()):
                    shutil.copy2(item, out_folder / item.name)

        selector = QueueFileSelector(rng, mergeable_files)
        folder_manifest = [f"MANIFEST FOR FOLDER: {data['display_name']}\n{'='*40}\n"]

        for v_num in range(1, args.versions + 1):
            # APPLY WEIGHTED PERCENTAGES FOR MULTIPLIERS
            if data["is_ts"]:
                # Time Sensitive: Equal weights for 1.0, 1.2, 1.5
                afk_multiplier = rng.choice([1.0, 1.2, 1.5])
            else:
                # Normal: x1 (50%), x2 (30%), x3 (20%)
                afk_multiplier = rng.choices([1, 2, 3], weights=[50, 30, 20], k=1)[0]
            
            selected_paths = selector.get_sequence(args.target_minutes)
            if not selected_paths: continue
            
            speed = rng.uniform(s_min, s_max)
            merged_events = []
            timeline_ms = 0
            total_dba, total_gaps, total_afk_pool = 0, 0, 0
            file_segments = []

            for i, p_str in enumerate(selected_paths):
                p = Path(p_str)
                raw = load_json_events(p)
                if not raw: continue
                
                t_vals = [int(e.get("Time", 0)) for e in raw]
                base_t, dur = min(t_vals), (max(t_vals) - min(t_vals))
                
                gap = round_to_sec((rng.randint(500, 2500) if i > 0 else 0) * afk_multiplier)
                timeline_ms += gap
                total_gaps += gap
                
                dba_val, split_idx = 0, -1
                if rng.random() < 0.40:
                    dba_val = round_to_sec((max(0, args.delay_before_action_ms + rng.randint(-118, 119))) * afk_multiplier)
                    if len(raw) > 1: split_idx = rng.randint(1, len(raw) - 1)
                total_dba += dba_val
                
                for ev_idx, e in enumerate(raw):
                    ne = deepcopy(e)
                    off = (int(e.get("Time", 0)) - base_t) * speed
                    if ev_idx >= split_idx and split_idx != -1: off += dba_val
                    ne["Time"] = int(off + timeline_ms)
                    merged_events.append(ne)
                
                if "screensharelink" not in p.name.lower():
                    pct = rng.choices([0, 0.12, 0.20, 0.28], weights=[55, 20, 15, 10])[0]
                    total_afk_pool += round_to_sec((int(dur * speed * pct)) * afk_multiplier)
                
                timeline_ms = merged_events[-1]["Time"]
                file_segments.append({"name": p.name, "end_time": timeline_ms})

            if total_afk_pool > 0:
                if data["is_ts"]: 
                    timeline_ms += total_afk_pool
                    merged_events[-1]["Time"] = timeline_ms
                else:
                    shift_point = len(merged_events) // 2
                    for k in range(shift_point, len(merged_events)):
                        merged_events[k]["Time"] += total_afk_pool
                    timeline_ms = merged_events[-1]["Time"]

            v_code = number_to_letters(v_num)
            fname = f"{v_code}_{int(timeline_ms / 60000)}m.json"
            (out_folder / fname).write_text(json.dumps(merged_events, indent=2))
            
            total_human_pause = total_dba + total_gaps + total_afk_pool
            manifest_entry = [f"Version {v_code} (Multiplier: x{afk_multiplier}):", 
                              f"  TOTAL DURATION: {format_ms_precise(timeline_ms)}",
                              f"  total PAUSE: {format_ms_precise(total_human_pause)} +BREAKDOWN:",
                              f"    - Micro-pauses: {format_ms_precise(total_dba)}",
                              f"    - Inter-file Gaps: {format_ms_precise(total_gaps)}",
                              f"    - AFK Pool: {format_ms_precise(total_afk_pool)}\n"]
            for i, seg in enumerate(file_segments):
                bullet = "*" if i < 11 else "-"
                manifest_entry.append(f"  {bullet} {seg['name']} (Ends at {format_ms_precise(seg['end_time'])})")
            manifest_entry.append("-" * 30)
            folder_manifest.append("\n".join(manifest_entry))

        (out_folder / "manifest.txt").write_text("\n\n".join(folder_manifest))

    print(f"--- Process Complete for Bundle {args.bundle_id} ---")

if __name__ == "__main__":
    main()
