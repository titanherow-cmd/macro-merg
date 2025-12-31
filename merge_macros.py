#!/usr/bin/env python3
"""merge_macros.py - Weighted Multipliers (50/30/20), Deep Nesting Support, Detailed Manifests, and Root Logout Support"""

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
        return []

def get_file_duration_ms(path: Path) -> int:
    events = load_json_events(path)
    if not events: return 0
    try:
        times = [int(e.get("Time", 0)) for e in events]
        return max(times) - min(times)
    except: return 0

def format_ms_precise(ms: int) -> str:
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
    # Removes " - Copy", " (1)", etc. to group related macros together
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
            # Estimate 30% overhead for gaps
            current_ms += (get_file_duration_ms(Path(pick)) * 1.3) + 1500
            if len(sequence) > 500: break 
        return sequence

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_root", type=Path)
    parser.add_argument("output_root", type=Path)
    parser.add_argument("--versions", type=int, default=6)
    parser.add_argument("--target-minutes", type=int, default=25) 
    parser.add_argument("--delay-before-action-ms", type=int, default=10)
    parser.add_argument("--bundle-id", type=int, required=True)
    args, _ = parser.parse_known_args()

    # Find originals folder
    possible_paths = [
        args.input_root / "originals",
        args.input_root / "input_macros" / "originals",
        Path.cwd() / "originals"
    ]
    originals_root = next((p for p in possible_paths if p.exists() and p.is_dir()), None)

    if not originals_root:
        for root, dirs, _ in os.walk(args.input_root):
            if 'originals' in dirs:
                originals_root = Path(root) / 'originals'
                break

    if not originals_root:
        print("CRITICAL ERROR: 'originals' folder not found.")
        sys.exit(1)

    logout_file = args.input_root / "logout.json"
    rng = random.Random()
    bundle_dir = args.output_root / f"merged_bundle_{args.bundle_id}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    
    unified_pools = {}

    # Scan for macros and handle deep nesting
    for game_folder in filter(Path.is_dir, originals_root.iterdir()):
        game_id = clean_identity(game_folder.name)
        z_folders = [s for s in game_folder.iterdir() if s.is_dir() and s.name.upper().startswith('Z')]
        
        for root, _, files in os.walk(game_folder):
            curr = Path(root)
            # Skip folders inside Z-prefixed pooling directories for now
            if any(p.upper().startswith('Z') for p in curr.relative_to(game_folder).parts): continue
            
            jsons = [f for f in files if f.endswith(".json") and "click_zones" not in f.lower()]
            if not jsons: continue
            
            rel = curr.relative_to(game_folder)
            key = (game_id, str(rel).lower())
            
            if key not in unified_pools:
                unified_pools[key] = {
                    "out_rel_path": Path(game_folder.name) / rel,
                    "display_name": f"{game_folder.name} / {rel}",
                    "files": [],
                    "is_ts": "time sensitive" in str(rel).lower(),
                    "source_folders": [curr],
                    "macro_id": clean_identity(curr.name) # Fixed Key Name
                }
            for f in jsons: unified_pools[key]["files"].append(curr / f)

        # Scoped Z Pooling Logic
        for z in z_folders:
            for r, _, fs in os.walk(z):
                zp = Path(r)
                zid = clean_identity(zp.name)
                for pk, pd in unified_pools.items():
                    # Match Z-folder identity with standard folder identity
                    if pd["macro_id"] == zid:
                        for f in fs:
                            if f.endswith(".json") and "click_zones" not in f.lower(): 
                                pd["files"].append(zp / f)
                        if zp not in pd["source_folders"]: 
                            pd["source_folders"].append(zp)

    # Process merges
    for key, data in unified_pools.items():
        if not data["files"]: continue
        out_folder = bundle_dir / data["out_rel_path"]
        out_folder.mkdir(parents=True, exist_ok=True)
        
        if logout_file.exists():
            shutil.copy2(logout_file, out_folder / "logout.json")

        # Copy non-JSON assets
        for src in data["source_folders"]:
            for item in src.iterdir():
                if item.is_file() and (not item.name.endswith(".json") or "click_zones" in item.name.lower()):
                    try: shutil.copy2(item, out_folder / item.name)
                    except: pass

        selector = QueueFileSelector(rng, data["files"])
        folder_manifest = [f"MANIFEST FOR: {data['display_name']}"]

        for v_num in range(1, args.versions + 1):
            # 50/30/20 Weighted Probabilities
            if data["is_ts"]: 
                # Inefficient files: limited multiplier to protect timing
                afk_multiplier = rng.choice([1.0, 1.2, 1.5])
            else: 
                # Standard files: Full weighted AFK gaps
                afk_multiplier = rng.choices([1, 2, 3], weights=[50, 30, 20], k=1)[0]
            
            selected_paths = selector.get_sequence(args.target_minutes)
            if not selected_paths: continue
            
            merged_events = []
            timeline_ms = 0
            for i, p_str in enumerate(selected_paths):
                p = Path(p_str)
                raw = load_json_events(p)
                if not raw: continue
                
                t_vals = [int(e.get("Time", 0)) for e in raw]
                base_t = min(t_vals)
                
                # Gap between files
                gap = round_to_sec((rng.randint(500, 2500) if i > 0 else 0) * afk_multiplier)
                timeline_ms += gap
                
                for e in raw:
                    ne = deepcopy(e)
                    ne["Time"] = int((int(e.get("Time", 0)) - base_t) + timeline_ms)
                    merged_events.append(ne)
                timeline_ms = merged_events[-1]["Time"]

            v_code = number_to_letters(v_num)
            fname = f"{v_code}_{int(timeline_ms / 60000)}m.json"
            (out_folder / fname).write_text(json.dumps(merged_events, indent=2))
            folder_manifest.append(f"Version {v_code}: {format_ms_precise(timeline_ms)} (Gap Multiplier: x{afk_multiplier})")

        (out_folder / "manifest.txt").write_text("\n".join(folder_manifest))

if __name__ == "__main__":
    main()
