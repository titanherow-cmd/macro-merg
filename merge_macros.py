#!/usr/bin/env python3
"""
merge_macros.py

Merges macro JSON files into multiple versions with exclusion, duplication,
extra copies, random pauses, event time shifting, and detailed logging.
Supports multiple groups in subfolders.
"""

import argparse
import json
import os
import glob
import random
from pathlib import Path
from copy import deepcopy
from zipfile import ZipFile

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True, help="Parent directory containing macro groups")
    parser.add_argument("--output-dir", required=True, help="Directory to write merged versions and ZIP")
    parser.add_argument("--versions", type=int, default=5, help="Number of versions to generate per group")
    parser.add_argument("--seed", type=int, default=None, help="Random seed")
    return parser.parse_args()

def load_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"WARNING: Cannot parse {path}: {e}")
        return None

def normalize_json(data):
    if isinstance(data, dict) and "events" in data:
        return data["events"]
    elif isinstance(data, list):
        return data
    elif isinstance(data, dict):
        # Try to find list under common keys
        for k in ["items","entries","records","actions","eventsList","events_array"]:
            if k in data and isinstance(data[k], list):
                return data[k]
        # Single event dict
        return [data]
    return []

def find_groups(input_dir):
    p = Path(input_dir)
    return [x for x in p.iterdir() if x.is_dir()]

def find_json_files(group_path):
    return sorted(glob.glob(str(group_path / "*.json")))

def apply_shifts(events, shift_ms):
    return [{"Type": e.get("Type"), "Time": e.get("Time",0)+shift_ms, "X": e.get("X"), "Y": e.get("Y"), "Delta": e.get("Delta"), "KeyCode": e.get("KeyCode")} for e in events]

def generate_version(files, seed, global_pause_set):
    r = random.Random(seed)
    # Exclude one file randomly
    exclude_idx = r.randrange(len(files))
    included_files = [f for i,f in enumerate(files) if i != exclude_idx]

    # Duplicate 2 files randomly
    dup_files = r.sample(included_files, 2)
    final_files = deepcopy(included_files) + dup_files

    # Add 1 or 2 extra copies randomly
    extra_count = r.choice([1,2])
    extra_candidates = [f for f in included_files if f not in dup_files]
    extra_files = r.sample(extra_candidates, k=min(extra_count,len(extra_candidates)))

    # Insert extra copies in positions not adjacent to the same file
    for f in extra_files:
        insert_pos = r.randrange(len(final_files)+1)
        if insert_pos>0 and final_files[insert_pos-1]==f:
            insert_pos +=1
        if insert_pos>len(final_files):
            insert_pos=len(final_files)
        final_files.insert(insert_pos, f)

    # Shuffle final_files randomly
    r.shuffle(final_files)

    # Compute event times with 30ms gaps and random pauses
    merged_events = []
    pause_log = []
    time_cursor = 0
    for f in final_files:
        events = normalize_json(load_json(f))
        # Decide pause
        add_pause = r.random()<0.6
        pause_duration = None
        if add_pause:
            while True:
                pause_duration = r.randint(2*60*1000, 13*60*1000) # 2-13 min in ms
                if p
