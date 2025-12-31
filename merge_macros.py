#!/usr/bin/env python3
"""
merge_macros.py - STABLE RESTORE POINT (v2.9.8)
- FIX: Nested paths are now properly grouped under a single numbered root.
- FIX: Numbering prefixes the TOP-LEVEL folder only (e.g., 1-deskt- osrs).
- FIX: Unique IDs (A1, B1...) are still assigned per sub-folder to avoid name collisions.
- Massive Pause: One random event injection per inefficient file (300s-720s).
- Identity Engine: Robust regex for " - Copy" and "Z_" variation pooling.
- Manifest: Named '!_MANIFEST_!' for maximum visibility.
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

    bundle_dir = args.output_root / f"merged_bundle_{args.bundle_id}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random()
    pools = {}

    # 1. Discovery
    for root, dirs, files in os.walk(originals_root):
        curr = Path(root)
        if any(p in curr.parts for p in [".git", ".github", "output"]): continue
        
        jsons = [f for f in files if f.endswith(".json") and "click_zones" not in f.lower()]
        if not jsons: continue
        
        macro_id = clean_identity(curr.name)
        rel_path = curr.relative_to(originals_root)
        
        if any(p.lower().startswith("z_") for p in rel_path.parts):
            continue

        key = str(rel_path).lower()
        if key not in pools:
            is_ts = bool(re.search(r'time[\s-]*sens', key))
            pools[key] = {
                "rel_path": rel_path,
                "files": [curr / f for f in jsons],
                "is_ts": is_ts,
                "macro_id": macro_id,
                "root_parent": rel_path.parts[0] if rel_path.parts else ""
            }

    # 2. Z-Variation Injection
    for root, dirs, files in os.walk(originals_root):
        curr = Path(root)
        if not any(p.lower().startswith("z_") for p in curr.parts): continue
        zid = clean_identity(curr.name)
        jsons = [f for f in files if f.endswith(".json") and "click_zones" not in f.lower()]
        for pk, pd in pools.items():
            if pd["macro_id"] == zid:
                pd["files"].extend([curr / f for f in jsons])

    # 3. Smart Numbering Logic
    # We need to map root folders to numbers, AND subfolders to unique IDs.
    sorted_keys = sorted(pools.keys())
    
    # Map each unique top-level root to a number
    root_to_id = {}
    unique_roots = sorted(list(set(p["root_parent"] for p in pools.values() if p["root_parent"])))
    for idx, rname in enumerate(unique_roots, start=1):
        root_to_id[rname] = idx

    # Every individual macro folder gets a unique index for its file naming (A1, B2...)
    for f_idx, key in enumerate(sorted_keys, start=1):
        data = pools[key]
        
        # PRESERVE NESTING BUT NUMBER THE ROOT:
        # If path is 'deskt- osrs/Edge/Macro', and deskt- osrs is root #1
        # Target: '1-deskt- osrs/Edge/Macro'
        path_parts = list(data["rel_path"].parts)
        root_name = data["root_parent"]
        if root_name in root_to_id:
            root_num = root_to_id[root_name]
            path_parts[0] = f"{root_num}-{root_name}"
            
        out_f = bundle_dir.joinpath(*path_parts)
        out_f.mkdir(parents=True, exist_ok=True)
        
        # We use f_idx for the file naming (A1, B2...) to ensure uniqueness 
        # even if multiple folders share the same root number.
        manifest = [f"FOLDER: {out_f.relative_to(bundle_dir)}", f"TS MODE: {data['is_ts']}", f"MACRO ID: {f_idx}", ""]
        
        norm_v = args.versions
        inef_v = 0 if data["is_ts"] else (norm_v // 2)
        
        for v_idx in range(1, (norm_v + inef_v) + 1):
            is_inef = (v_idx > norm_v)
            v_letter = chr(64 + v_idx)
            v_code = f"{v_letter}{f_idx}"
            
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

        (out_f / "!_MANIFEST_!").write_text("\n".join(manifest))

if __name__ == "__main__":
    main()
