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

def clean_folder_name(name: str) -> str:
    """Removes common suffixes like '- Copy' or '(2)' to find the base folder identity."""
    return re.sub(r'(\s*-\s*Copy(\s*\(\d+\))?)|(\s*\(\d+\))', '', name, flags=re.IGNORECASE).strip()

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

    # Robust root detection
    base_dir = args.input_root
    if not (base_dir / "originals").exists():
        print(f"Warning: 'originals' not found in {base_dir}. Checking current working directory...")
        base_dir = Path(".")
    
    if not (base_dir / "originals").exists():
        print(f"CRITICAL ERROR: 'originals' folder not found.")
        print("Root directory contents:")
        for item in base_dir.iterdir(): print(f" - {item.name}")
        sys.exit(1)
    
    rng = random.Random()
    bundle_dir = args.output_root / f"merged_bundle_{args.bundle_id}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    
    unified_pools = {}
    
    # We define search roots
    originals_root = base_dir / "originals"
    z_extra_root = base_dir / "Z +100"

    search_targets = [
        (originals_root, True), # (Path, is_primary)
        (z_extra_root, False)
    ]

    for root_path, is_primary in search_targets:
        if not root_path.exists(): 
            print(f"Path not found (skipping): {root_path}")
            continue
        
        print(f"Scanning: {root_path}")
        for p in root_path.rglob("*.json"):
            if "output" in p.parts or p.name.startswith('.') or "manifest" in p.name.lower(): continue
            if any(x in p.name.lower() for x in ["click_zones", "first", "last"]): continue
            
            rel_parts = p.parent.relative_to(root_path).parts
            
            if len(rel_parts) >= 2:
                super_parent_name = rel_parts[0]
                child_name_raw = rel_parts[1]
            elif len(rel_parts) == 1:
                super_parent_name = "General"
                child_name_raw = rel_parts[0]
            else:
                super_parent_name = "General"
                child_name_raw = "Root"
            
            child_name_clean = clean_folder_name(child_name_raw)
            identity_key = (super_parent_name, child_name_clean)
            
            if identity_key not in unified_pools:
                target_rel_path = Path(super_parent_name) / child_name_clean
                unified_pools[identity_key] = {
                    "target_rel_path": target_rel_path,
                    "files": [],
                    "is_ts": "time sensitive" in child_name_clean.lower(),
                    "source_folders": [],
                    "has_primary": False # Tracking if this pool exists in 'originals'
                }
            
            if is_primary:
                unified_pools[identity_key]["has_primary"] = True
            
            if p not in unified_pools[identity_key]["files"]:
                unified_pools[identity_key]["files"].append(p)
            if p.parent not in unified_pools[identity_key]["source_folders"]:
                unified_pools[identity_key]["source_folders"].append(p.parent)

    # FILTER: Only keep pools that actually exist in 'originals'
    # This prevents 'Z +100' from generating its own separate output folders
    pools_to_process = {k: v for k, v in unified_pools.items() if v["has_primary"]}

    if not pools_to_process:
        print("CRITICAL ERROR: No valid macro files found in 'originals' to merge!")
        sys.exit(1)

    print(f"Found {len(pools_to_process)} unique macro groups to process (Pooled with Z +100 where applicable).")

    for (super_name, child_name), data in pools_to_process.items():
        mergeable_files = data["files"]
        if not mergeable_files: continue
        
        rel_path = data["target_rel_path"]
        out_folder = bundle_dir / rel_path
        out_folder.mkdir(parents=True, exist_ok=True)
        
        is_ts = data["is_ts"]
        
        for src_folder in data["source_folders"]:
            for item in src_folder.iterdir():
                if item.is_file() and item not in mergeable_files:
                    if "click_zones" in item.name or not item.name.endswith(".json"):
                        target_file = out_folder / item.name
                        if not target_file.exists():
                            shutil.copy2(item, target_file)
        
        selector = QueueFileSelector(rng, mergeable_files)
        folder_manifest = [f"MANIFEST FOR UNIFIED FOLDER: {super_name} / {child_name}\n{'='*40}\n"]
        folder_manifest.append(f"Total files in pool: {len(mergeable_files)}\n")

        for v_num in range(1, args.versions + 1):
            afk_multiplier = rng.choice([1.0, 1.2, 1.5]) if is_ts else rng.choice([1, 2, 3])
            selected_paths = selector.get_sequence(args.target_minutes)
            if not selected_paths: continue
            
            MAX_MS = 60 * 60 * 1000
            speed = rng.uniform(s_min, s_max)
            
            while True:
                temp_total_dur = 0
                for i, p_str in enumerate(selected_paths):
                    p = Path(p_str)
                    dur = get_file_duration_ms(p) * speed
                    gap = round_to_sec((rng.randint(500, 2500) if i > 0 else 0) * afk_multiplier)
                    afk_pct = rng.choices([0, 0.12, 0.20, 0.28], weights=[55, 20, 15, 10])[0]
                    afk_val = round_to_sec((int(dur * afk_pct) if "screensharelink" not in p.name.lower() else 0) * afk_multiplier)
                    dba_val = 0
                    if rng.random() < 0.40:
                        dba_val = round_to_sec((max(0, args.delay_before_action_ms + rng.randint(-118, 119))) * afk_multiplier)
                    temp_total_dur += dur + gap + afk_val + dba_val

                if temp_total_dur <= MAX_MS or len(selected_paths) <= 1:
                    break
                else:
                    selected_paths.pop()

            merged_events = []
            timeline_ms = 0
            total_dba = 0
            total_gaps = 0
            total_afk_pool = 0
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
                total_gaps += gap
                
                dba_val = 0
                split_idx = -1
                if rng.random() < 0.40:
                    dba_val = round_to_sec((max(0, args.delay_before_action_ms + rng.randint(-118, 119))) * afk_multiplier)
                    if len(raw) > 1: split_idx = rng.randint(1, len(raw) - 1)
                total_dba += dba_val
                
                start_in_merge = len(merged_events)
                for ev_idx, e in enumerate(raw):
                    ne = deepcopy(e)
                    off = (int(e.get("Time", 0)) - base_t) * speed
                    if ev_idx >= split_idx and split_idx != -1: off += dba_val
                    ne["Time"] = int(off + timeline_ms)
                    merged_events.append(ne)
                
                file_segments.append({"name": p.name, "start_idx": start_in_merge, "end_idx": len(merged_events)-1})
                
                if "screensharelink" not in p.name.lower():
                    pct = rng.choices([0, 0.12, 0.20, 0.28], weights=[55, 20, 15, 10])[0]
                    total_afk_pool += round_to_sec((int(dur * speed * pct)) * afk_multiplier)
                
                timeline_ms = merged_events[-1]["Time"]

            if total_afk_pool > 0:
                if is_ts: merged_events[-1]["Time"] += total_afk_pool
                else:
                    target_idx = rng.randint(1, len(file_segments)-1) if len(file_segments) > 1 else 0
                    split_pt = file_segments[target_idx]["start_idx"] if len(file_segments) > 1 else len(merged_events)-1
                    for k in range(split_pt, len(merged_events)): merged_events[k]["Time"] += total_afk_pool

            v_code = number_to_letters(v_num)
            final_dur = merged_events[-1]["Time"]
            fname = f"{v_code}_{int(final_dur / 60000)}m.json"
            (out_folder / fname).write_text(json.dumps(merged_events, indent=2))
            
            total_human_pause = total_dba + total_gaps + total_afk_pool
            v_title = f"Version {v_code} (Multiplier: x{afk_multiplier}):"
            manifest_entry = [v_title]
            manifest_entry.append(f"  TOTAL DURATION: {format_ms_precise(final_dur)}")
            manifest_entry.append(f"  total PAUSE: {format_ms_precise(total_human_pause)} +BREAKDOWN:")
            manifest_entry.append(f"    - Micro-pauses: {format_ms_precise(total_dba)}")
            manifest_entry.append(f"    - Inter-file Gaps: {format_ms_precise(total_gaps)}")
            manifest_entry.append(f"    - AFK Pool: {format_ms_precise(total_afk_pool)}")
            
            manifest_entry.append("")
            for i, seg in enumerate(file_segments):
                bullet = "*" if i < 11 else "-"
                end_time_str = format_ms_precise(merged_events[seg['end_idx']]['Time'])
                manifest_entry.append(f"  {bullet} {seg['name']} (Ends at {end_time_str})")
            
            manifest_entry.append("-" * 30)
            folder_manifest.append("\n".join(manifest_entry))

        (out_folder / "manifest.txt").write_text("\n\n".join(folder_manifest))

    print(f"--- SUCCESS: Bundle {args.bundle_id} created ---")

if __name__ == "__main__":
    main()
