#!/usr/bin/env python3

"""
merge_macros.py - Merges JSON macro files with randomized pauses between and within files.
Handles nested subfolders in the input directory.
"""

import os
import json
import argparse
import random
from pathlib import Path
from datetime import timedelta

def parse_time_string(time_str):
    try:
        if 'm' in time_str:
            minutes, seconds = time_str.split('m')
            seconds = seconds.replace('s', '') if 's' in seconds else seconds
            return int(minutes) * 60 + int(seconds or 0)
        return int(time_str)
    except:
        raise ValueError(f"Invalid time format: {time_str}")

def find_json_files(root_dir):
    return sorted([f for f in Path(root_dir).rglob("*.json") if f.is_file()])

def load_events(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        if isinstance(data, list):
            return data
        for key in ('events', 'items', 'entries'):
            if key in data and isinstance(data[key], list):
                return data[key]
        raise ValueError(f"Unrecognized format in {file_path}")

def zero_base_events(events):
    if not events:
        return events, 0
    min_time = min(int(e['Time']) for e in events)
    for e in events:
        e['Time'] = int(e['Time']) - min_time
    duration = max(e['Time'] for e in events)
    return events, duration

def format_duration(seconds):
    return str(timedelta(seconds=seconds)).replace(":", "m", 1).replace(":", "s").replace("m0s", "m")

def merge_macros(files, intra_max, intra_max_count, inter_max, inter_max_count):
    rng = random.Random()
    merged = []
    offset = 0
    total_duration = 0
    file_durations = []

    for i, file_path in enumerate(files):
        events = load_events(file_path)
        events, duration = zero_base_events(events)

        # Insert intra-file pauses
        if intra_max_count > 0 and duration > 0:
            insert_indices = sorted(random.sample(range(len(events)), min(intra_max_count, len(events))))
            for idx in insert_indices:
                pause = rng.randint(0, intra_max) if intra_max < 30 else rng.randint(30, intra_max)
                events[idx]['Time'] += pause * 1000
            events.sort(key=lambda e: e['Time'])

        # Shift all times by offset
        for event in events:
            event['Time'] += offset
        merged.extend(events)
        offset = max(e['Time'] for e in events)
        file_durations.append((file_path.stem, duration))

        # Add inter-file pause (skip after last)
        if i < len(files) - 1 and inter_max_count > 0:
            pause = rng.randint(0, inter_max) if inter_max < 30 else rng.randint(30, inter_max)
            offset += pause * 1000
            total_duration += pause

        total_duration += duration

    return merged, file_durations, total_duration

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-dir', required=True, help='Path to nested input directory of macro files')
    parser.add_argument('--output-dir', required=True, help='Output directory for merged file')
    parser.add_argument('--versions', type=int, default=16, help='How many versions per group')
    parser.add_argument('--intra-file-max', default='1m32s', help='Max pause inside files')
    parser.add_argument('--intra-file-max-mins', type=float, default=None)
    parser.add_argument('--intra-file-min-mins', type=float, default=None)
    parser.add_argument('--inter-file-max', default='2m37s', help='Max pause between files')
    parser.add_argument('--exclude-count', type=int, default=5, help='Max files to randomly exclude per version')
    parser.add_argument('--inter-file-count', type=int, default=1, help='Max pauses between files')
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    files = find_json_files(input_dir)
    if len(files) <= 1:
        print("Not enough files to merge.")
        return

    # Determine max pause seconds
    intra_max_secs = parse_time_string(args.intra_file_max)
    intra_max_secs = max(intra_max_secs, 0)
    inter_max_secs = parse_time_string(args.inter_file_max)
    inter_max_secs = max(inter_max_secs, 0)

    for v in range(args.versions):
        selected = random.sample(files, k=max(2, len(files) - args.exclude_count))
        selected.sort(key=lambda f: f.name)
        merged, names_durs, total_secs = merge_macros(selected, intra_max_secs, 3, inter_max_secs, args.inter_file_count)

        duration_str = format_duration(total_secs)
        base_name = f"M_{duration_str.replace(' ', '')}= " + "- ".join(f.stem for f, _ in names_durs)
        out_path = output_dir / f"{base_name}.json"
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(merged, f, indent=2)
        print(f"Wrote: {out_path}")

if __name__ == '__main__':
    main()
