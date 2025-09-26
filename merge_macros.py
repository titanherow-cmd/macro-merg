# merge_macros.py
# Script to merge macro JSON files into multiple versions
# Handles JSON files where events are under "events" or "macro"->"events"

import argparse
import json
import random
import os
import zipfile
import copy
from pathlib import Path
from collections import Counter

MIN_PAUSE_MS = 2 * 60 * 1000
MAX_PAUSE_MS = 13 * 60 * 1000
GAP_AFTER_FILE_MS = 30
DUPLICATE_PAUSE_PROB = 0.6
MAX_RESAMPLE_TRIES = 200

def load_json_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_json(obj, path):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, indent=2)

def first_three(s):
    return s[:3]

def sample_unique_pause(used_pauses, rng):
    tries = 0
    while True:
        tries += 1
        p = rng.randint(MIN_PAUSE_MS, MAX_PAUSE_MS)
        if p not in used_pauses:
            used_pauses.add(p)
            return p, None
        if tries > MAX_RESAMPLE_TRIES:
            p2 = p + 1
            while p2 in used_pauses:
                p2 += 1
            used_pauses.add(p2)
            return p2, f"adjusted from {p} to {p2} after {tries} tries"

def ensure_no_adjacent(seq):
    return all(seq[i] != seq[i+1] for i in range(len(seq)-1))

def arrange_no_adjacent(freq_map):
    counts = dict(freq_map)
    arranged = []
    total = sum(counts.values())
    for _ in range(total):
        choices = [fn for fn, c in counts.items() if c > 0 and (not arranged or fn != arranged[-1])]
        if not choices:
            choices = [fn for fn, c in counts.items() if c > 0]
        pick = max(choices, key=lambda x: counts[x])
        arranged.append(pick)
        counts[pick] -= 1
    return arranged

def build_versions(original_dir, out_dir, versions=5, seed=None):
    rng = random.Random(seed)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    files = sorted([p for p in Path(original_dir).iterdir() if p.is_file() and p.suffix.lower() == '.json'])
    if not files:
        raise SystemExit("No JSON files found in input directory.")

    originals = {}
    for p in files:
        data = load_json_file(p)
        # Adapt to your JSON schema
        if 'events' in data:
            events = data['events']
        elif 'macro' in data and 'events' in data['macro']:
            events = data['macro']['events']
        else:
            raise SystemExit(f"File {p.name} missing 'events' key. Edit script if your schema differs.")
        duration = events[-1].get('time', 0) if events else 0
        originals[p.name] = {"path": p, "events": events, "duration": duration}

    filenames = list(originals.keys())
    used_pauses = set()
    bundle_log = {
        "versions": [],
        "global": {"min_pause_ms": MIN_PAUSE_MS, "max_pause_ms": MAX_PAUSE_MS, "gap_after_file_ms": GAP_AFTER_FILE_MS},
        "original_filenames": filenames.copy(),
        "notes": []
    }
    merged_filenames = []

    for v in range(1, versions + 1):
        base_seq = filenames.copy()
        dup_two = rng.sample(filenames, 2)
        base_seq += dup_two[:]
        extra_count = rng.choice([1, 2])
        extra_files = rng.sample(filenames, extra_count)
        base_seq += extra_files[:]

        seq = base_seq.copy()
        attempts = 0
        success = False
        while attempts < 2000:
            attempts += 1
            rng.shuffle(seq)
            if ensure_no_adjacent(seq):
                success = True
                break
        if not success:
            seq = arrange_no_adjacent(Counter(base_seq))
            bundle_log["notes"].append(f"forced greedy arrange for version {v} after {attempts} shuffles")

        merged_events = []
        current_offset = 0
        pauses = []
        indices_by_name = {}
        for idx, name in enumerate(seq):
            indices_by_name.setdefault(name, []).append(idx)

        extra_pause_insert = {}
        for name, idxs in indices_by_name.items():
            if len(idxs) >= 2:
                for pos in idxs[1:]:
                    if rng.random() < DUPLICATE_PAUSE_PROB:
                        extra_pause_insert[pos] = True

        for idx, fname in enumerate(seq):
            orig = originals[fname]
            for ev in orig["events"]:
                new_ev = copy.deepcopy(ev)
                new_ev["time"] = new_ev["time"] + current_offset
                new_ev["source"] = fname
                merged_events.append(new_ev)
            file_end = current_offset + orig["duration"]
            if idx < len(seq) - 1:
                pause_ms, fix_note = sample_unique_pause(used_pauses, rng)
                pauses.append({"after_index": idx, "from": seq[idx], "to": seq[idx + 1], "pause_ms": pause_ms, "fix": fix_note})
                current_offset = file_end + GAP_AFTER_FILE_MS + pause_ms
                if (idx + 1) in extra_pause_insert:
                    dup_pause_ms, dup_fix = sample_unique_pause(used_pauses, rng)
                    pauses.append({
                        "before_index": idx + 1,
                        "inserted_before_file": seq[idx + 1],
                        "pause_ms": dup_pause_ms,
                        "fix": dup_fix,
                        "reason": "duplicate_extra_pause"
                    })
                    current_offset += dup_pause_ms
            else:
                current_offset = file_end

        merged_name_base = "".join(first_three(fn) for fn in seq)
        merged_filename = f"{merged_name_base}_v{v}.json"
        merged_path = out_dir / merged_filename
        merged_obj = {"merged_from_order": seq.copy(), "events": merged_events}
        save_json(merged_obj, merged_path)
        merged_filenames.append(merged_path.name)

        originals_present = all(fn in seq for fn in filenames)
        adjacents = [(i, seq[i], seq[i + 1]) for i in range(len(seq) - 1) if seq[i] == seq[i + 1]]
        schedule = []
        cur = 0
        ok_monotonic = True
        for i, fname in enumerate(seq):
            dur = originals[fname]["duration"]
            start = cur
            end = start + dur
            schedule.append({"index": i, "file": fname, "start": start, "end": end})
            if i < len(seq) - 1:
                total_pause_after = sum(p["pause_ms"] for p in pauses if p.get("after_index") == i)
                total_pause_after += sum(p["pause_ms"] for p in pauses if p.get("before_index") == i + 1)
                expected_next_start = end + GAP_AFTER_FILE_MS + total_pause_after
                cur = expected_next_start
        for j in range(len(schedule) - 1):
            if schedule[j]["end"] > schedule[j + 1]["start"]:
                ok_monotonic = False
                break

        version_log = {
            "version": v,
            "merged_filename": merged_filename,
            "file_order": seq.copy(),
            "duplicated_files_selected": dup_two.copy(),
            "extra_copies_added": extra_files.copy(),
            "pauses": pauses,
            "originals_present": originals_present,
            "adjacent_duplicates": adjacents,
            "time_schedule_sample": schedule[:3] + (schedule[-3:] if len(schedule) > 6 else []),
            "monotonic_ok": ok_monotonic
        }
        bundle_log["versions"].append(version_log)

    bundle_log_path = out_dir / "bundle_log.json"
    save_json(bundle_log, bundle_log_path)
    zip_path = out_dir / "merged_bundle.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(bundle_log_path, arcname="bundle_log.json")
        for name in merged_filenames:
            zf.write(Path(out_dir) / name, arcname=name)
    return {"out_dir": str(out_dir), "merged_files": merged_filenames, "bundle_log": "bundle_log.json", "zip": str(zip_path)}

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", "-i", required=True)
    parser.add_argument("--output-dir", "-o", required=True)
    parser.add_argument("--versions", "-n", default=5, type=int)
    parser.add_argument("--seed", "-s", default=None, type=int)
    args = parser.parse_args()
    res = build_versions(args.input_dir, args.output_dir, versions=args.versions, seed=args.seed)
    print("Done. ZIP:", res["zip"])
