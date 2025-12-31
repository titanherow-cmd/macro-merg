#!/usr/bin/env python3
"""
merge_macros.py - ULTIMATE VERSION (v1.7)
- Rule: Standard Folders -> 50% x1, 30% x2, 20% x3
- Rule: Time Sensitive Folders -> Equal 1.0, 1.2, 1.5
- Every 3rd version (C, F, I) triggers Massive Random Pauses
- Manifest structure updated to match user requirements exactly
"""

from pathlib import Path
import argparse, json, random, re, sys, os, math, shutil
from copy import deepcopy

def load_json_events(path: Path):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            for k in ("events", "items", "entries", "records"):
                if k in data and isinstance(data[k], list): return deepcopy(data[k])
            return [data] if "Time" in data else []
        return deepcopy(data) if isinstance(data, list) else []
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
    total_seconds = int(round(ms / 1000))
    minutes = total_seconds // 60
    seconds = total_seconds % 60
    return f"{minutes}m {seconds}s" if minutes > 0 else f"{seconds}s"

def clean_identity(name: str) -> str:
    name = re.sub(r'(\s*-\s*Copy(\s*\(\d+\))?)|(\s*\(\d+\))', '', name, flags=re.IGNORECASE).strip()
    return name.lower()

class QueueFileSelector:
    def __init__(self, rng, all_files):
        self.rng = rng
        self.efficient = [f for f in all_files if "¬¬¬" not in f.name]
        self.inefficient = [f for f in all_files if "¬¬¬" in f.name]
        self.eff_pool = list(self.efficient)
        self.ineff_pool = list(self.inefficient)
        self.rng.shuffle(self.eff_pool)
        self.rng.shuffle(self.ineff_pool)

    def get_sequence(self, target_minutes, force_inefficient=False):
        sequence = []
        current_ms = 0.0
        target_ms = target_minutes * 60000
        
        while current_ms < target_ms:
            if force_inefficient and self.ineff_pool:
                pick = self.ineff_pool.pop(0)
            elif self.eff_pool:
                pick = self.eff_pool.pop(0)
            elif self.efficient:
                self.eff_pool = list(self.efficient)
                self.rng.shuffle(self.eff_pool)
                pick = self.eff_pool.pop(0)
            elif self.ineff_pool:
                pick = self.ineff_pool.pop(0)
            elif self.inefficient:
                self.ineff_pool = list(self.inefficient)
                self.rng.shuffle(self.ineff_pool)
                pick = self.ineff_pool.pop(0)
            else: break

            sequence.append(pick)
            current_ms += (get_file_duration_ms(pick) * 1.3) + 1500
            if len(sequence) > 1000: break 
        return sequence

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_root", type=str)
    parser.add_argument("output_root", type=Path)
    parser.add_argument("--versions", type=int, default=6)
    parser.add_argument("--target-minutes", type=int, default=35) 
    parser.add_argument("--delay-before-action-ms", type=int, default=10)
    parser.add_argument("--bundle-id", type=int, required=True)
    parser.add_argument("--speed-range", type=str, default="1.0 1.0")
    args, _ = parser.parse_known_args()

    try:
        s_min, s_max = map(float, args.speed_range.split())
    except:
        s_min, s_max = 1.0, 1.0

    search_base = Path(args.input_root).resolve()
    originals_root = next((search_base / n for n in ["Originals", "originals"] if (search_base / n).is_dir()), None)
    
    if not originals_root:
        for root, dirs, _ in os.walk(search_base):
            if any(x in root for x in [".git", "output"]): continue
            if "originals" in [d.lower() for d in dirs]:
                originals_root = Path(root) / next(d for d in dirs if d.lower() == "originals")
                break
    if not originals_root: sys.exit("Originals folder missing.")

    bundle_dir = args.output_root / f"merged_bundle_{args.bundle_id}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    logout_file = search_base / "logout.json"
    rng = random.Random()
    unified_pools = {}

    for game_folder in filter(Path.is_dir, originals_root.iterdir()):
        game_id = clean_identity(game_folder.name)
        for root, _, files in os.walk(game_folder):
            curr = Path(root)
            if any(p.upper().startswith('Z') for p in curr.relative_to(game_folder).parts): continue
            jsons = [f for f in files if f.endswith(".json") and "click_zones" not in f.lower()]
            if not jsons: continue
            
            rel = curr.relative_to(game_folder)
            key = (game_id, str(rel).lower())
            if key not in unified_pools:
                unified_pools[key] = {
                    "out_rel_path": Path(game_folder.name) / rel,
                    "display_name": f"{game_folder.name} / {rel}",
                    "files": [], "is_ts": "time sensitive" in str(curr).lower(),
                    "source_folders": [curr], "macro_id": clean_identity(curr.name)
                }
            for f in jsons: unified_pools[key]["files"].append(curr / f)

    for game_folder in filter(Path.is_dir, originals_root.iterdir()):
        for z in [s for s in game_folder.iterdir() if s.is_dir() and s.name.upper().startswith('Z')]:
            for r, _, fs in os.walk(z):
                zp = Path(r)
                zid = clean_identity(zp.name)
                for pk, pd in unified_pools.items():
                    if pd["macro_id"] == zid:
                        for f in [f for f in fs if f.endswith(".json") and "click_zones" not in f.lower()]:
                            pd["files"].append(zp / f)
                        if zp not in pd["source_folders"]: pd["source_folders"].append(zp)

    for key, data in unified_pools.items():
        out_folder = bundle_dir / data["out_rel_path"]
        out_folder.mkdir(parents=True, exist_ok=True)
        if logout_file.exists(): shutil.copy2(logout_file, out_folder / "logout.json")
        for src in data["source_folders"]:
            for item in src.iterdir():
                if item.is_file() and (not item.name.endswith(".json") or "click_zones" in item.name.lower()):
                    try: shutil.copy2(item, out_folder / item.name)
                    except: pass

        selector = QueueFileSelector(rng, data["files"])
        manifest_header = [
            f"MANIFEST FOR FOLDER: {data['display_name']}",
            f"========================================",
            f"TOTAL FILES IN POOL: {len(data['files'])}",
            ""
        ]
        manifest_body = []

        for v_num in range(1, args.versions + 1):
            v_code = chr(64 + v_num)
            is_inef_version = (v_num % 3 == 0)
            
            # RULE: Correct AFK Multiplier Logic
            if data["is_ts"]:
                # Time Sensitive: Equally 1.0, 1.2, 1.5
                mult = rng.choice([1.0, 1.2, 1.5])
            else:
                # Standard: Weighted 50% x1, 30% x2, 20% x3
                mult = rng.choices([1, 2, 3], weights=[50, 30, 20], k=1)[0]
            
            internal_speed = rng.uniform(s_min, s_max)
            paths = selector.get_sequence(args.target_minutes, force_inefficient=is_inef_version)
            
            v_title = f"Version {v_code}"
            if is_inef_version: v_title += " [EXTRA - INEFFICIENT]"
            manifest_body.append(f"{v_title} (Multiplier: x{mult}):")
            
            merged, timeline = [], 0
            pause_breakdown = {"Micro": 0, "Gap": 0, "AFK": 0, "Massive": []}
            file_entries = []

            for i, p_obj in enumerate(paths):
                raw = load_json_events(p_obj)
                if not raw: continue
                t_v = [int(e.get("Time", 0)) for e in raw]
                base_t = min(t_v) if t_v else 0
                
                base_gap = rng.randint(500, 2500) if i > 0 else 0
                gap = int(base_gap * mult)
                pause_breakdown["Gap"] += base_gap
                pause_breakdown["AFK"] += (gap - base_gap)
                
                # Rule: Massive Pauses for Version C, F, I
                if is_inef_version and rng.random() < 0.2 and i > 0:
                    m_pause = rng.randint(300000, 900000)
                    gap += m_pause
                    pause_breakdown["Massive"].append(m_pause)

                timeline += gap
                for e_idx, e in enumerate(raw):
                    ne = deepcopy(e)
                    offset = int((int(e.get("Time", 0)) - base_t) * internal_speed)
                    ne["Time"] = timeline + offset + (e_idx * args.delay_before_action_ms)
                    merged.append(ne)
                
                pause_breakdown["Micro"] += (len(raw) * args.delay_before_action_ms)
                timeline = merged[-1]["Time"]
                bullet = '-' if is_inef_version else '*'
                file_entries.append(f"  {bullet} {p_obj.name} (Ends at {format_ms_precise(timeline)})")

            total_pause = pause_breakdown["Micro"] + pause_breakdown["Gap"] + pause_breakdown["AFK"] + sum(pause_breakdown["Massive"])
            manifest_body.append(f"  TOTAL DURATION: {format_ms_precise(timeline)}")
            manifest_body.append(f"  total PAUSE: {format_ms_precise(total_pause)} +BREAKDOWN:")
            manifest_body.append(f"    - Micro-pauses: {pause_breakdown['Micro']//1000}s")
            manifest_body.append(f"    - Inter-file Gaps: {pause_breakdown['Gap']//1000}s")
            manifest_body.append(f"    - AFK Pool: {pause_breakdown['AFK']//1000}s")
            for idx, m in enumerate(pause_breakdown["Massive"]):
                manifest_body.append(f"    - Massive P{idx+1}: {format_ms_precise(m)}")
            manifest_body.append("")
            manifest_body.extend(file_entries)
            manifest_body.append("-" * 30)
            manifest_body.append("")

            fname = f"{v_code}_{int(timeline / 60000)}m.json"
            (out_folder / fname).write_text(json.dumps(merged, indent=2), encoding="utf-8")

        final_manifest = manifest_header + manifest_body
        (out_folder / "manifest.txt").write_text("\n".join(final_manifest), encoding="utf-8")

    print(f"Done. Bundle: {args.bundle_id}")

if __name__ == "__main__":
    main()
