#!/usr/bin/env python3
"""merge_macros.py - AFK Priority Logic: Normal x3, Ineff x3, TS x1, ¬¬¬ prefix, and 60m cap"""

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
    if ms < 1000 and ms > 0:
        return f"{ms}ms"
    total_seconds = int(ms / 1000)
    minutes = total_seconds // 60
    seconds = total_seconds % 60
    if minutes == 0:
        return f"{seconds}s"
    return f"{minutes}m {seconds}s"

def number_to_letters(n: int) -> str:
    res = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        res = chr(65 + rem) + res
    return res or "A"

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
        
        if not self.pool_src:
            return []

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

    search_root = args.input_root
    if not search_root.exists():
        search_root = Path(".")

    rng = random.Random()
    bundle_dir = args.output_root / f"merged_bundle_{args.bundle_id}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    
    folders_with_json = []
    seen_folders = set()
    
    for p in search_root.rglob("*.json"):
        if "output" in p.parts or p.name.startswith('.'): continue
        is_special = any(x in p.name.lower() for x in ["click_zones", "first", "last"])
        if not is_special and p.parent not in seen_folders:
            folder = p.parent
            mergeable_jsons = sorted([
                f for f in folder.glob("*.json") 
                if not any(x in f.name.lower() for x in ["click_zones", "first", "last"])
            ])
            if mergeable_jsons:
                folders_with_json.append((folder, mergeable_jsons))
                seen_folders.add(folder)

    for folder_path, mergeable_files in folders_with_json:
        rel_path = folder_path.relative_to(search_root)
        out_folder = bundle_dir / rel_path
        out_folder.mkdir(parents=True, exist_ok=True)
        
        # Determine if this folder is "Time Sensitive"
        is_ts = "time sensitive" in str(folder_path).lower()
        
        for item in folder_path.iterdir():
            if item.is_file() and item not in mergeable_files and "click_zones" not in item.name:
                shutil.copy2(item, out_folder / item.name)
        
        selector = QueueFileSelector(rng, mergeable_files)
        folder_manifest = [f"MANIFEST FOR FOLDER: {rel_path}\n{'='*40}\n"]

        versions_to_process = []
        for i in range(1, args.versions + 1):
            versions_to_process.append(False) 
            if i % 3 == 0:
                versions_to_process.append(True) 
        
        for idx, is_inefficient in enumerate(versions_to_process):
            v_num = idx + 1
            
            # RULE UPDATES:
            # 1. Normal files now get x3 multiplier.
            # 2. Inefficient files stay at x3 multiplier.
            # 3. Time Sensitive folders get x1 multiplier (no scaling).
            if is_ts:
                afk_multiplier = 1
            else:
                afk_multiplier = 3
            
            selected_paths = selector.get_sequence(args.target_minutes)
            if not selected_paths: continue
            
            massive_p1 = rng.randint(5 * 60 * 1000, 10 * 60 * 1000) if is_inefficient else 0
            massive_p2 = rng.randint(10 * 60 * 1000, 17 * 60 * 1000) if is_inefficient else 0
            
            MAX_MS = 60 * 60 * 1000
            speed = rng.uniform(s_min, s_max)
            
            while True:
                temp_total_dur = massive_p1 + massive_p2
                for i, p_str in enumerate(selected_paths):
                    p = Path(p_str)
                    dur = get_file_duration_ms(p) * speed
                    gap = (rng.randint(500, 2500) if i > 0 else 0) * afk_multiplier
                    afk_pct = rng.choices([0, 0.12, 0.20, 0.28], weights=[55, 20, 15, 10])[0]
                    afk_val = (int(dur * afk_pct) if "screensharelink" not in p.name.lower() else 0) * afk_multiplier
                    dba_val = 0
                    if rng.random() < 0.40:
                        dba_val = (max(0, args.delay_before_action_ms + rng.randint(-118, 119))) * afk_multiplier
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
                
                gap = (rng.randint(500, 2500) if i > 0 else 0) * afk_multiplier
                timeline_ms += gap
                total_gaps += gap
                
                dba_val = 0
                split_idx = -1
                if rng.random() < 0.40:
                    dba_val = (max(0, args.delay_before_action_ms + rng.randint(-118, 119))) * afk_multiplier
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
                    total_afk_pool += (int(dur * speed * pct)) * afk_multiplier
                
                timeline_ms = merged_events[-1]["Time"]

            if total_afk_pool > 0:
                if is_ts: merged_events[-1]["Time"] += total_afk_pool
                else:
                    target_idx = rng.randint(1, len(file_segments)-1) if len(file_segments) > 1 else 0
                    split_pt = file_segments[target_idx]["start_idx"] if len(file_segments) > 1 else len(merged_events)-1
                    for k in range(split_pt, len(merged_events)): merged_events[k]["Time"] += total_afk_pool

            if is_inefficient:
                if is_ts: merged_events[-1]["Time"] += (massive_p1 + massive_p2)
                else:
                    if len(merged_events) > 20:
                        idx1 = rng.randint(10, len(merged_events) // 2)
                        idx2 = rng.randint(len(merged_events) // 2 + 1, len(merged_events) - 10)
                        for k in range(idx1, len(merged_events)): merged_events[k]["Time"] += massive_p1
                        for k in range(idx2, len(merged_events)): merged_events[k]["Time"] += massive_p2
                    else: merged_events[-1]["Time"] += (massive_p1 + massive_p2)

            v_code = number_to_letters(v_num)
            prefix = "¬¬¬" if is_inefficient else ""
            final_dur = merged_events[-1]["Time"]
            fname = f"{prefix}{v_code}_{int(final_dur / 60000)}m.json"
            (out_folder / fname).write_text(json.dumps(merged_events, indent=2))
            
            total_human_pause = total_dba + total_gaps + total_afk_pool + massive_p1 + massive_p2
            v_title = f"Version {v_code}{' [EXTRA - INEFFICIENT]' if is_inefficient else ''} (Multiplier: x{afk_multiplier}):"
            manifest_entry = [v_title]
            manifest_entry.append(f"  TOTAL DURATION: {format_ms_precise(final_dur)}")
            manifest_entry.append(f"  total PAUSE: {format_ms_precise(total_human_pause)} +BREAKDOWN:")
            manifest_entry.append(f"    - Micro-pauses: {format_ms_precise(total_dba)}")
            manifest_entry.append(f"    - Inter-file Gaps: {format_ms_precise(total_gaps)}")
            manifest_entry.append(f"    - AFK Pool: {format_ms_precise(total_afk_pool)}")
            if is_inefficient:
                manifest_entry.append(f"    - Massive P1: {format_ms_precise(massive_p1)}")
                manifest_entry.append(f"    - Massive P2: {format_ms_precise(massive_p2)}")
            
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
