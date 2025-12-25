#!/usr/bin/env python3
import argparse
import json
import random
import os
import sys
from pathlib import Path
from copy import deepcopy

def load_json(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if isinstance(data, dict):
                for key in ["events", "items", "entries", "records"]:
                    if key in data and isinstance(data[key], list): return data[key]
                return [data] if "Time" in data else []
            return data if isinstance(data, list) else []
    except: return []

def format_ms(ms):
    total_seconds = int(ms / 1000)
    return f"{total_seconds // 60}m {total_seconds % 60}s"

class MacroEngine:
    def __init__(self, rng, speed_range, delay_ms):
        self.rng = rng
        self.speed_range = speed_range
        self.delay_ms = delay_ms

    def apply_humanization(self, events, is_special):
        if not events: return []
        if is_special: return events
        
        # Rule 1: Micro-pauses (40% chance)
        delay = 0
        if self.rng.random() < 0.40:
            delay = max(0, self.delay_ms + self.rng.randint(-118, 119))
            
        # File-specific Speed Multiplier
        speed = self.rng.uniform(self.speed_range[0], self.speed_range[1])
        
        processed = []
        t_vals = [int(e.get("Time", 0)) for e in events]
        t_start = min(t_vals) if t_vals else 0
        
        split_idx = self.rng.randint(0, len(events)-1) if len(events) > 1 else 0
        
        for i, e in enumerate(events):
            ne = deepcopy(e)
            rel_t = (int(e.get("Time", 0)) - t_start) * speed
            if i >= split_idx: rel_t += delay
            ne["Time"] = int(rel_t)
            processed.append(ne)
            
        return processed

    def merge_sequence(self, folder, files, target_mins, v_num):
        # 1 in 4 versions is "Inefficient"
        is_inefficient = (v_num % 4 == 0)
        is_ts = "time sensitive" in folder.name.lower()
        
        firsts = sorted([f for f in files if "first" in f.name.lower()])
        lasts = sorted([f for f in files if "last" in f.name.lower()])
        pool = [f for f in files if f not in firsts and f not in lasts]
        
        self.rng.shuffle(pool)
        
        selected = list(firsts)
        current_ms = 0
        target_ms = target_mins * 60000
        
        if pool:
            while current_ms < target_ms and len(selected) < 100:
                pick = self.rng.choice(pool)
                selected.append(pick)
                # Estimate 2.5 mins per file
                current_ms += 150000 
        
        selected.extend(lasts)
        
        merged = []
        timeline = 0
        manifest_details = []
        afk_pool_ms = 0

        for i, f in enumerate(selected):
            is_special = "screensharelink" in f.name.lower()
            raw = load_json(f)
            if not raw: continue
            
            # Inter-file Gap
            gap = self.rng.randint(500, 2000) if i > 0 else 0
            timeline += gap
            
            processed = self.apply_humanization(raw, is_special)
            if not processed: continue

            for e in processed:
                e["Time"] += timeline
                merged.append(e)
            
            # Rule 2: AFK Pool
            if not is_special:
                duration = processed[-1]["Time"] - processed[0]["Time"]
                pct = self.rng.choices([0, 0.12, 0.20, 0.28], weights=[55, 20, 15, 10])[0]
                afk_pool_ms += int(duration * pct)
            
            timeline = merged[-1]["Time"]
            manifest_details.append(f"  - {f.name} (Ends: {format_ms(timeline)})")

        if is_inefficient:
            afk_pool_ms += self.rng.randint(15, 27) * 60000
            
        if is_ts:
            if merged: merged[-1]["Time"] += afk_pool_ms
        else:
            split = self.rng.randint(len(merged)//2, len(merged)-1) if len(merged) > 1 else 0
            for k in range(split, len(merged)):
                merged[k]["Time"] += afk_pool_ms

        final_dur = merged[-1]["Time"] if merged else 0
        v_tag = "¬¬¬" if is_inefficient else ""
        v_letter = chr(64 + v_num) if v_num <= 26 else str(v_num)
        filename = f"{v_tag}{v_letter}_{int(final_dur/60000)}m.json"
        
        manifest_text = "\n".join(manifest_details) + f"\n  TOTAL AFK: {format_ms(afk_pool_ms)}"
        return filename, merged, manifest_text

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--versions", type=int, default=6)
    parser.add_argument("--target-minutes", type=int, default=25)
    parser.add_argument("--delay-before-action-ms", type=int, default=10)
    parser.add_argument("--bundle-id", type=str)
    parser.add_argument("--speed-range", type=str, default="1.0 1.0")
    args = parser.parse_args()

    # Robust speed-range parsing
    try:
        parts = args.speed_range.replace(',', ' ').split()
        s_min = float(parts[0])
        s_max = float(parts[1]) if len(parts) > 1 else s_min
    except:
        s_min, s_max = 1.0, 1.0

    rng = random.Random()
    engine = MacroEngine(rng, (s_min, s_max), args.delay_before_action_ms)
    
    bundle_name = f"merged_bundle_{args.bundle_id}"
    output_base = args.output / bundle_name
    
    target_folders = [d for d in args.input.rglob("*") if d.is_dir() and any(d.glob("*.json"))]
    if not target_folders and any(args.input.glob("*.json")):
        target_folders = [args.input]

    for folder in target_folders:
        json_files = sorted([f for f in folder.glob("*.json") if "click_zones" not in f.name])
        if not json_files: continue
        
        try: rel_path = folder.relative_to(args.input)
        except: rel_path = Path(".")
            
        out_folder = output_base / rel_path
        out_folder.mkdir(parents=True, exist_ok=True)
        folder_manifest = [f"Group: {rel_path}\n" + "="*20]
        
        for v in range(1, args.versions + 1):
            fname, events, m_text = engine.merge_sequence(folder, json_files, args.target_minutes, v)
            if events:
                with open(out_folder / fname, 'w', encoding='utf-8') as f:
                    json.dump(events, f, indent=2)
                folder_manifest.append(f"\n[{fname}]\n{m_text}")
            
        with open(out_folder / "manifest.txt", 'w', encoding='utf-8') as f:
            f.write("\n".join(folder_manifest))

if __name__ == "__main__":
    main()
