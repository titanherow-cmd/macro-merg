#!/usr/bin/env python3
"""
merge_macros.py - STABLE RESTORE POINT (v2.8.2)
- FEATURE: Internal speed multiplier removed to prevent mechanical desync.
- Massive Pause: One random event injection per inefficient file (300s-720s).
- Identity Engine: Robust regex for " - Copy" and "Z_" variation pooling.
- Typo-Tolerant Time Sensitive: regex r'time[\s-]*sens'.
- Multipliers: Standard (x1, x2, x3) @ 50/30/20% | TS (1.0, 1.2, 1.5) @ Equal weights.
- Manifest Update: Renamed to '!_MANIFEST_!' for maximum visibility.
"""

import argparse, json, random, re, sys, os, math, shutil
from pathlib import Path
from copy import deepcopy

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
        return deepcopy(cleaned)
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

class QueueFileSelector:
    def __init__(self, rng, all_files):
        self.rng = rng
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
            cur_ms += (get_file_duration_ms(pick) + 1500)
            if len(seq) > 1000: break
        return seq

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_root", type=str)
    parser.add_argument("output_root", type=Path)
    parser.add_argument("--versions", type=int, default=6)
    parser.add_argument("--target-minutes", type=int, default=35)
    parser.add_argument("--delay-before-action-ms", type=int, default=10)
    parser.add_argument("--bundle-id", type=int, required=True)
    parser.add_argument("--speed-range", type=str, default="1.0 1.0")
    args = parser.parse_args()

    search_base = Path(args.input_root).resolve()
    originals_root = search_base 
    for d in ["originals", "input_macros"]:
        if (search_base / d).exists(): originals_root = search_base / d; break

    bundle_dir = args.output_root / f"merged_bundle_{args.bundle_id}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random()
    pools = {}

    for folder in filter(Path.is_dir, originals_root.iterdir()):
        if folder.name.lower() in [".git", "output"]: continue
        game_id = clean_identity(folder.name)
        for root, _, files in os.walk(folder):
            curr = Path(root)
            jsons = [f for f in files if f.endswith(".json") and "click_zones" not in f.lower()]
            if not jsons: continue
            rel = curr.relative_to(folder)
            key = (game_id, str(rel).lower())
            if key not in pools:
                is_ts = bool(re.search(r'time[\s-]*sens', str(curr).lower()))
                pools[key] = {"out": Path(folder.name) / rel, "files": [], "is_ts": is_ts, "id": clean_identity(curr.name), "srcs": [curr]}
            for f in jsons: pools[key]["files"].append(curr / f)

    for folder in filter(Path.is_dir, originals_root.iterdir()):
        if not folder.name.upper().startswith('Z'): continue
        for r, _, fs in os.walk(folder):
            zp = Path(r); zid = clean_identity(zp.name)
            for pk, pd in pools.items():
                if pd["id"] == zid:
                    for f in [f for f in fs if f.endswith(".json") and "click_zones" not in f.lower()]: pd["files"].append(zp / f)

    for key, data in pools.items():
        out_f = bundle_dir / data["out"]; out_f.mkdir(parents=True, exist_ok=True)
        manifest = [f"FOLDER: {data['out']}", f"TS MODE: {data['is_ts']}", ""]
        
        norm_v = args.versions
        inef_v = 0 if data["is_ts"] else (norm_v // 2)
        
        for v_idx in range(1, (norm_v + inef_v) + 1):
            is_inef = (v_idx > norm_v)
            v_code = chr(64 + v_idx)
            
            if data["is_ts"]: mult = rng.choice([1.0, 1.2, 1.5])
            elif is_inef: mult = rng.choices([1, 2, 3], weights=[20, 40, 40], k=1)[0]
            else: mult = rng.choices([1, 2, 3], weights=[50, 30, 20], k=1)[0]
            
            paths = QueueFileSelector(rng, data["files"]).get_sequence(args.target_minutes, is_inef, data["is_ts"])
            merged, timeline = [], 0
            
            for i, p in enumerate(paths):
                raw = load_json_events(p)
                if not raw: continue
                t_vals = [int(e["Time"]) for e in raw]
                base_t = min(t_vals)
                
                gap = int(rng.randint(500, 2500) * mult) if i > 0 else 0
                timeline += gap
                
                for e_idx, e in enumerate(raw):
                    ne = deepcopy(e)
                    rel_offset = int(int(e["Time"]) - base_t)
                    ne["Time"] = timeline + rel_offset + (e_idx * args.delay_before_action_ms)
                    merged.append(ne)
                timeline = merged[-1]["Time"]

            if is_inef and not data["is_ts"] and len(merged) > 1:
                p_ms = rng.randint(300000, 720000)
                split = rng.randint(0, len(merged) - 2)
                for j in range(split + 1, len(merged)): merged[j]["Time"] += p_ms
                timeline = merged[-1]["Time"]
                manifest.append(f"Version {v_code} (Inef): Pause {p_ms}ms at index {split}")

            fname = f"{'¬¬¬' if is_inef else ''}{v_code}_{int(timeline/60000)}m.json"
            (out_f / fname).write_text(json.dumps(merged, indent=2))
            manifest.append(f"  {v_code}: {format_ms_precise(timeline)} (Mult: x{mult})")

        # Filename updated to !_MANIFEST_!
        (out_f / "!_MANIFEST_!").write_text("\n".join(manifest))

if __name__ == "__main__":
    main()
