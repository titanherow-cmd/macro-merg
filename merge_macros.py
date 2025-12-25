#!/usr/bin/env python3
import argparse
import json
import random
import sys
from pathlib import Path
from copy import deepcopy

def load_json(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # Handle different macro formats
            if isinstance(data, dict):
                for key in ["events", "items", "entries"]:
                    if key in data: return data[key]
                return [data]
            return data
    except Exception as e:
        return []

def format_ms(ms):
    total_seconds = int(ms / 1000)
    minutes = total_seconds // 60
    seconds = total_seconds % 60
    return f"{minutes}m {seconds}s"

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
            
        # Rule: File-specific Speed Multiplier
        speed = self.rng.uniform(self.speed_range[0], self.speed_range[1])
        
        processed = []
        t_start = int(events[0].get("Time", 0))
        
        # Random split point for the micro-pause
        split_idx = self.rng.randint(0, len(events)-1) if len(events) > 1 else 0
        
        for i, e in enumerate(events):
            ne = deepcopy(e)
            # Normalize to 0 and apply speed
            rel_t = (int(e.get("Time", 0)) - t_start) * speed
            # Apply the delay if we are past the split point
            if i >= split_idx:
                rel_t += delay
            ne["Time"] = int(rel_t)
            processed.append(ne)
            
        return processed

    def merge_sequence(self, folder, files, target_mins, v_num):
        # 1 in 4 versions is "Inefficient" (massive AFK)
        is_inefficient = (v_num % 4 == 0)
        is_ts = "time sensitive" in folder.name.lower()
        
        # Separate special assets
        firsts = sorted([f for f in files if "first" in f.name.lower()])
        lasts = sorted([f for f in files if "last" in f.name.lower()])
        pool = [f for f in files if f not in firsts and f not in lasts]
        
        # Shuffle middle pool
        self.rng.shuffle(pool)
        
        selected = list(firsts)
        current_ms = 0
        target_ms = target_mins * 60000
        
        # Build file list
        if pool:
            while current_ms < target_ms:
                pick = self.rng.choice(pool)
                selected.append(pick)
                # Estimate 2 minutes per file for selection logic
                current_ms += 120000
                if len(selected) > 60: break
        
        selected.extend(lasts)
        
        merged = []
        timeline = 0
        manifest_details = []
        afk_pool_ms = 0

        for i, f in enumerate(selected):
            is_special = "screensharelink" in f.name.lower()
            raw_events = load_json(f)
            if not raw_events: continue
            
            # Rule 3: Inter-file Gaps (0.5s to 2s)
            gap = self.rng.randint(500, 2000) if i > 0 else 0
            timeline += gap
            
            # Rule 1: Humanization and Speed
            processed = self.apply_humanization(raw_events, is_special)
            
            # Shift events onto the timeline
            for e in processed:
                e["Time"] += timeline
                merged.append(e)
            
            # Rule 2: AFK Pool Calculation (Weighted probability)
            if not is_special:
                duration = processed[-1]["Time"] - processed[0]["Time"]
                # 55% None, 20% 12%, 15% 20%, 10% 28%
                pct = self.rng.choices([0, 0.12, 0.20, 0.28], weights=[55, 20, 15, 10])[0]
                afk_pool_ms += int(duration * pct)
            
            timeline = merged[-1]["Time"]
            manifest_details.append(f"  - {f.name} (Ends: {format_ms(timeline)})")

        # Inefficient Mode: Add massive 15-27 minute pause
        if is_inefficient:
            afk_pool_ms += self.rng.randint(15, 27) * 60000
            
        # Injection Logic: Time Sensitive appends to end, otherwise random seam
        if is_ts:
            if merged: merged[-1]["Time"] += afk_pool_ms
        else:
            # Inject pause at a random point in the second half of the sequence
            split_point = self.rng.randint(len(merged)//2, len(merged)-1) if len(merged) > 1 else 0
            for k in range(split_point, len(merged)):
                merged[k]["Time"] += afk_pool_ms

        final_duration = merged[-1]["Time"] if merged else 0
        v_prefix = "¬¬¬" if is_inefficient else ""
        v_letter = chr(64 + v_num) if v_num <= 26 else str(v_num)
        filename = f"{v_prefix}{v_letter}_{int(final_duration/60000)}m.json"
        
        manifest_text = "\n".join(manifest_details) + f"\n  TOTAL AFK/HUMAN PAUSE: {format_ms(afk_pool_ms)}"
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

    # Parse speed range
    try:
        s_min, s_max = map(float, args.speed_range.split())
    except:
        s_min, s_max = 1.0, 1.0

    rng = random.Random()
    engine = MacroEngine(rng, (s_min, s_max), args.delay_before_action_ms)
    
    bundle_name = f"merged_bundle_{args.bundle_id}"
    output_base = args.output / bundle_name
    
    # Scan for folders containing JSON
    target_folders = [d for d in args.input.rglob("*") if d.is_dir()]
    if not any(folder.glob("*.json") for folder in target_folders):
        # If no subfolders, check the root input
        target_folders = [args.input]

    for folder in target_folders:
        json_files = sorted(list(folder.glob("*.json")))
        if not json_files: continue
        
        # Mirror relative path structure
        try:
            rel_path = folder.relative_to(args.input)
        except:
            rel_path = Path(".")
            
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
