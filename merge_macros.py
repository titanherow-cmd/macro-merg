#!/usr/bin/env python3
"""merge_macros.py - Yesterday's stable working version"""

from pathlib import Path
import argparse, json, random, sys, os, shutil, re
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
    total_seconds = int(round(ms / 1000))
    minutes = total_seconds // 60
    seconds = total_seconds % 60
    return f"{minutes}m {seconds}s" if minutes > 0 else f"{seconds}s"

def round_to_sec(ms: int) -> int:
    return int(round(ms / 1000.0) * 1000)

def number_to_letters(n: int) -> str:
    res = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        res = chr(65 + rem) + res
    return res or "A"

def clean_identity(name: str) -> str:
    return re.sub(r'(\s*-\s*Copy(\s*\(\d+\))?)|(\s*\(\d+\))', '', name, flags=re.IGNORECASE).strip().lower()

class QueueFileSelector:
    def __init__(self, rng, all_mergeable_files):
        self.rng = rng
        self.pool_src = list(all_mergeable_files)
        self.pool = list(self.pool_src)
        self.rng.shuffle(self.pool)
        
    def get_sequence(self, target_minutes):
        sequence, current_ms, target_ms = [], 0.0, target_minutes * 60000
        if not self.pool_src: return []
        while current_ms < target_ms:
            if not self.pool:
                self.pool = list(self.pool_src)
                self.rng.shuffle(self.pool)
            pick = self.pool.pop(0)
            sequence.append(str(pick.resolve()))
            current_ms += (get_file_duration_ms(pick) * 1.3) + 1500
            if len(sequence) > 300: break
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

    originals_root = args.input_root / "originals"
    if not originals_root.exists():
        print("CRITICAL ERROR: 'originals' folder not found.")
        sys.exit(1)

    logout_file = args.input_root / "logout.json"
    rng = random.Random()
    bundle_dir = args.output_root / f"merged_bundle_{args.bundle_id}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    
    unified_pools = {}
    for game_folder in filter(Path.is_dir, originals_root.iterdir()):
        game_id = clean_identity(game_folder.name)
        z_folders = [s for s in game_folder.iterdir() if s.is_dir() and s.name.upper().startswith('Z')]
        
        for root, _, files in os.walk(game_folder):
            curr = Path(root)
            if any(p.upper().startswith('Z') for p in curr.relative_to(game_folder).parts): continue
            jsons = [f for f in files if f.endswith(".json") and "click_zones" not in f.lower()]
            if not jsons: continue
            rel = curr.relative_to(game_folder)
            key = (game_id, str(rel).lower())
            
            if key not in unified_pools:
                unified_pools[key] = {
                    "out_path": Path(game_folder.name) / rel, "display": f"{game_folder.name}/{rel}",
                    "files": [], "is_ts": "time sensitive" in str(rel).lower(),
                    "sources": [curr], "macro_id": clean_identity(curr.name)
                }
            for f in jsons: unified_pools[key]["files"].append(curr / f)

        for z in z_folders:
            for r, _, fs in os.walk(z):
                zp = Path(r)
                zid = clean_identity(zp.name)
                for pk, pd in unified_pools.items():
                    if pd["macro_id"] == zid:
                        for f in fs:
                            if f.endswith(".json") and "click_zones" not in f.lower(): pd["files"].append(zp / f)
                        if zp not in pd["sources"]: pd["sources"].append(zp)

    for key, data in unified_pools.items():
        if not data["files"]: continue
        out = bundle_dir / data["out_path"]
        out.mkdir(parents=True, exist_ok=True)
        if logout_file.exists(): shutil.copy2(logout_file, out / "logout.json")
        for src in data["sources"]:
            for item in src.iterdir():
                if item.is_file() and (not item.name.endswith(".json") or "click_zones" in item.name.lower()):
                    try: shutil.copy2(item, out / item.name)
                    except: pass

        selector = QueueFileSelector(rng, data["files"])
        manifest = [f"MANIFEST FOR: {data['display']}"]
        for v in range(1, args.versions + 1):
            mult = rng.choice([1.0, 1.2, 1.5]) if data["is_ts"] else rng.choices([1, 2, 3], [50, 30, 20])[0]
            paths = selector.get_sequence(args.target_minutes)
            if not paths: continue
            merged, timeline = [], 0
            for i, p in enumerate(map(Path, paths)):
                raw = load_json_events(p)
                if not raw: continue
                base = min(int(e.get("Time", 0)) for e in raw)
                timeline += round_to_sec((rng.randint(500, 2500) if i > 0 else 0) * mult)
                for e in raw:
                    ne = deepcopy(e)
                    ne["Time"] = int((int(e.get("Time", 0)) - base) + timeline)
                    merged.append(ne)
                timeline = merged[-1]["Time"]
            code = number_to_letters(v)
            (out / f"{code}_{int(timeline/60000)}m.json").write_text(json.dumps(merged, indent=2))
            manifest.append(f"Version {code}: {format_ms_precise(timeline)}")
        (out / "manifest.txt").write_text("\n".join(manifest))

if __name__ == "__main__":
    main()
