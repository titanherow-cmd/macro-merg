#!/usr/bin/env python3
"""merge_macros.py - OSRS Anti-Detection with Zone Awareness (FIXED)"""

from pathlib import Path
import argparse, json, random, re, sys, os, math
from copy import deepcopy
from zipfile import ZipFile
from itertools import combinations

COUNTER_PATH = Path(".github/merge_bundle_counter.txt")
SPECIAL_FILENAME = "close reopen mobile screensharelink.json"
SPECIAL_KEYWORD = "screensharelink"

def parse_time_to_seconds(s: str) -> int:
    if s is None or not str(s).strip():
        raise ValueError("Empty time string")
    s = str(s).strip()
    if re.match(r'^\d+$', s):
        return int(s)
    m = re.match(r'^(\d+):(\d{1,2})$', s)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    m = re.match(r'^(\d+)\.(\d{1,2})$', s)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    m = re.match(r'^(?:(\d+)m)?(?:(\d+)s)?$', s)
    if m and (m.group(1) or m.group(2)):
        minutes = int(m.group(1)) if m.group(1) else 0
        seconds = int(m.group(2)) if m.group(2) else 0
        return minutes * 60 + seconds
    raise ValueError(f"Cannot parse time: {s!r}")

def read_counter_file():
    try:
        if COUNTER_PATH.exists():
            txt = COUNTER_PATH.read_text(encoding="utf-8").strip()
            return int(txt) if txt else 0
    except:
        pass
    return 0

def write_counter_file(n: int):
    try:
        COUNTER_PATH.parent.mkdir(parents=True, exist_ok=True)
        COUNTER_PATH.write_text(str(n), encoding="utf-8")
    except:
        pass

def load_click_zones(folder_path: Path):
    search_paths = [
        folder_path / "click_zones.json",
        folder_path.parent / "click_zones.json",
        Path.cwd() / "click_zones.json"
    ]
    for zone_file in search_paths:
        if zone_file.exists():
            try:
                data = json.loads(zone_file.read_text(encoding="utf-8"))
                return data.get("target_zones", []), data.get("excluded_zones", [])
            except Exception as e:
                print(f"WARNING: Failed to load {zone_file}: {e}", file=sys.stderr)
    return [], []

def is_click_in_zone(x: int, y: int, zone: dict) -> bool:
    try:
        return zone['x1'] <= x <= zone['x2'] and zone['y1'] <= y <= zone['y2']
    except:
        return False

def find_all_dirs_with_json(input_root: Path):
    if not input_root.exists() or not input_root.is_dir():
        return []
    found = set()
    for p in sorted(input_root.rglob("*")):
        if p.is_dir():
            try:
                has = any(child.is_file() and child.suffix.lower() == ".json" for child in p.iterdir())
                if has:
                    found.add(p)
            except:
                pass
    return sorted(found)

def find_json_files_in_dir(dirpath: Path):
    try:
        return sorted([p for p in dirpath.glob("*.json") if p.is_file() and not p.name.startswith("click_zones")])
    except:
        return []

def load_json_events(path: Path):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"WARNING: Failed to read {path}: {e}", file=sys.stderr)
        return []
    if isinstance(data, dict):
        for k in ("events", "items", "entries", "records", "actions"):
            if k in data and isinstance(data[k], list):
                return deepcopy(data[k])
        if "Time" in data:
            return [deepcopy(data)]
        return []
    return deepcopy(data) if isinstance(data, list) else []

def zero_base_events(events):
    if not events:
        return [], 0
    events_with_time = []
    for e in events:
        try:
            t = int(e.get("Time", 0))
        except:
            try:
                t = int(float(e.get("Time", 0)))
            except:
                t = 0
        events_with_time.append((e, t))
    try:
        events_with_time.sort(key=lambda x: x[1])
    except Exception as e:
        print(f"WARNING: Could not sort events: {e}", file=sys.stderr)
    if not events_with_time:
        return [], 0
    min_t = events_with_time[0][1]
    shifted = [{"Time": t - min_t, **deepcopy(e)} if isinstance(e, dict) else {**deepcopy(e), "Time": t - min_t} for (e, t) in events_with_time]
    for i, (_, t) in enumerate(events_with_time):
        shifted[i] = deepcopy(events_with_time[i][0])
        shifted[i]["Time"] = t - min_t
    duration_ms = shifted[-1]["Time"] if shifted else 0
    return shifted, duration_ms

def part_from_filename(fname: str):
    stem = Path(fname).stem
    letters = [ch for ch in stem if ch.isalpha()]
    digits = [ch for ch in stem if ch.isdigit()]
    token_chars = [ch.lower() for ch in letters[:4]]
    if len(token_chars) < 4:
        token_chars.extend(digits[:4 - len(token_chars)])
    if not token_chars:
        alnum = [ch.lower() for ch in stem if ch.isalnum()]
        token_chars = alnum[:4]
    return ''.join(token_chars)

def compute_minutes_from_ms(ms: int):
    return math.ceil(ms / 60000) if ms > 0 else 0

def number_to_letters(n: int) -> str:
    if n <= 0:
        return ""
    letters = ""
    while n > 0:
        n -= 1
        letters = chr(ord('A') + (n % 26)) + letters
        n //= 26
    return letters

def add_desktop_mouse_paths(events, rng):
    enhanced = []
    last_x, last_y = None, None
    for e in deepcopy(events):
        is_click = e.get('Type') in ['Click', 'LeftClick', 'RightClick'] or 'button' in e or 'Button' in e
        if is_click and 'X' in e and 'Y' in e and e['X'] is not None and e['Y'] is not None:
            try:
                target_x, target_y = int(e['X']), int(e['Y'])
                click_time = int(e.get('Time', 0))
                if last_x is not None and last_y is not None:
                    distance = ((target_x - last_x)**2 + (target_y - last_y)**2)**0.5
                    if distance > 30:
                        num_points = min(rng.randint(2, 4), int(distance / 50) + 1)
                        movement_duration = min(int(distance * rng.uniform(0.5, 1.0)), 400)
                        for i in range(num_points):
                            t = (i + 1) / (num_points + 1)
                            curve_offset_x = rng.randint(-10, 10) * (1 - abs(2*t - 1))
                            curve_offset_y = rng.randint(-10, 10) * (1 - abs(2*t - 1))
                            inter_x = int(last_x + (target_x - last_x) * t + curve_offset_x)
                            inter_y = int(last_y + (target_y - last_y) * t + curve_offset_y)
                            point_time = click_time - movement_duration + int(movement_duration * t)
                            enhanced.append({
                                'Time': max(0, point_time),
                                'Type': 'MouseMove',
                                'X': inter_x,
                                'Y': inter_y
                            })
                        enhanced.append({
                            'Time': max(0, click_time - rng.randint(10, 30)),
                            'Type': 'MouseMove',
                            'X': target_x,
                            'Y': target_y
                        })
                last_x, last_y = target_x, target_y
            except Exception as ex:
                print(f"Warning: Mouse path error: {ex}", file=sys.stderr)
        enhanced.append(e)
        if e.get('Type') == 'MouseMove' and 'X' in e and 'Y' in e:
            try:
                last_x, last_y = int(e['X']), int(e['Y'])
            except:
                pass
    return enhanced

def add_micro_pauses(events, rng, micropause_chance=0.15):
    return [{**deepcopy(e), 'Time': int(e.get('Time', 0)) + rng.randint(50, 250) if rng.random() < micropause_chance else int(e.get('Time', 0))} for e in events]

def add_reaction_variance(events, rng):
    varied = []
    for i, e in enumerate(deepcopy(events)):
        is_click = e.get('Type') in ['Click', 'RightClick'] or 'button' in e or 'Button' in e
        if is_click and i > 0 and rng.random() < 0.3:
            e['Time'] = int(e.get('Time', 0)) + rng.randint(200, 600)
        varied.append(e)
    return varied

def add_mouse_jitter(events, rng, is_desktop=False, target_zones=None, excluded_zones=None):
    if target_zones is None:
        target_zones = []
    if excluded_zones is None:
        excluded_zones = []
    jittered = []
    jitter_range = [-1, 0, 1]
    for e in deepcopy(events):
        is_click = e.get('Type') in ['Click', 'LeftClick', 'RightClick'] or 'button' in e or 'Button' in e
        if is_click and 'X' in e and 'Y' in e and e['X'] is not None and e['Y'] is not None:
            try:
                original_x, original_y = int(e['X']), int(e['Y'])
                in_excluded = any(is_click_in_zone(original_x, original_y, zone) for zone in excluded_zones)
                if not in_excluded:
                    in_target = any(is_click_in_zone(original_x, original_y, zone) for zone in target_zones)
                    if in_target or not target_zones:
                        e['X'] = original_x + rng.choice(jitter_range)
                        e['Y'] = original_y + rng.choice(jitter_range)
            except:
                pass
        jittered.append(e)
    return jittered

def add_time_of_day_fatigue(events, rng):
    try:
        from datetime import datetime
        now = datetime.now()
        hour, is_weekend = now.hour, now.weekday() >= 5
        if 23 <= hour or hour < 5:
            fatigue_min, fatigue_max = 1.15, 1.30
        elif 6 <= hour < 12:
            fatigue_min, fatigue_max = 0.95, 1.05
        elif 18 <= hour < 23:
            fatigue_min, fatigue_max = 1.0, 1.1
        else:
            fatigue_min, fatigue_max = 1.05, 1.15
        if is_weekend:
            fatigue_min += 0.05
            fatigue_max += 0.1
        fatigue_multiplier = rng.uniform(fatigue_min, fatigue_max)
        fatigued = [{**deepcopy(e), 'Time': int(int(e.get('Time', 0)) * fatigue_multiplier)} for e in events]
        return fatigued, 0.0
    except:
        return events, 0.0

def insert_intra_pauses(events, rng, max_pauses, min_s, max_s):
    if not events or max_pauses <= 0:
        return deepcopy(events), []
    evs = deepcopy(events)
    n = len(evs)
    if n < 2:
        return evs, []
    k = rng.randint(1, min(max_pauses, n-1))
    chosen = rng.sample(range(n-1), k)
    pauses_info = []
    for gap_idx in sorted(chosen):
        pause_ms = rng.randint(min_s * 1000, max_s * 1000)
        for j in range(gap_idx+1, n):
            evs[j]["Time"] = int(evs[j].get("Time", 0)) + pause_ms
        pauses_info.append({"after_event_index": gap_idx, "pause_ms": pause_ms})
    return evs, pauses_info

def apply_shifts(events, shift_ms):
    return [{**deepcopy(e), 'Time': int(e.get('Time', 0)) + int(shift_ms)} for e in events]

class NonRepeatingSelector:
    def __init__(self, rng):
        self.rng, self.used_combos = rng, set()
    def select_files(self, files, exclude_count):
        if not files:
            return [], []
        n = len(files)
        file_indices = list(range(n))
        max_exclude = min(exclude_count, max(0, n - 1))
        all_possible = [frozenset(combo) for exclude_k in range(0, max_exclude + 1) for combo in combinations(file_indices, exclude_k)]
        available = [c for c in all_possible if c not in self.used_combos]
        if not available:
            self.used_combos.clear()
            available = all_possible
        chosen_exclude_indices = self.rng.choice(available)
        self.used_combos.add(chosen_exclude_indices)
        excluded = [files[i] for i in chosen_exclude_indices]
        included = [files[i] for i in file_indices if i not in chosen_exclude_indices]
        return included if included else files.copy(), excluded
    def shuffle_with_memory(self, items):
        if not items or len(items) <= 1:
            return items
        if len(items) > 8:
            shuffled = items.copy()
            self.rng.shuffle(shuffled)
            return shuffled
        from itertools import permutations
        all_perms = list(permutations(items))
        available = [p for p in all_perms if p not in self.used_combos]
        if not available:
            self.used_combos.clear()
            available = all_perms
        chosen = self.rng.choice(available)
        self.used_combos.add(chosen)
        return list(chosen)

def locate_special_file(folder: Path, input_root: Path):
    for cand in [folder / SPECIAL_FILENAME, input_root / SPECIAL_FILENAME]:
        if cand.exists():
            return cand.resolve()
    keyword = SPECIAL_KEYWORD.lower()
    for p in Path.cwd().rglob("*"):
        if p.is_file() and keyword in p.name.lower():
            return p.resolve()
    return None

def generate_version_for_folder(files, rng, version_num, exclude_count, within_max_s, within_max_pauses, between_max_s, folder_path: Path, input_root: Path, selector):
    if not files:
        return None, [], [], {"inter_file_pauses": [], "intra_file_pauses": []}, [], 0
    included, excluded = selector.select_files(files, exclude_count)
    if not included:
        included = files.copy()
    always_first_file = next((f for f in included if Path(f).name.lower().startswith("always first")), None)
    always_last_file = next((f for f in included if Path(f).name.lower().startswith("always last")), None)
    included = [f for f in included if f not in [always_first_file, always_last_file]]
    use_always_first = (version_num % 2 == 1) if always_first_file and always_last_file else (version_num % 3 == 1) if always_first_file else False
    use_always_last = (version_num % 2 == 0) if always_first_file and always_last_file else (version_num % 3 == 2) if always_last_file else False
    final_files = selector.shuffle_with_memory(included)
    if use_always_first and always_first_file:
        final_files.insert(0, always_first_file)
    if use_always_last and always_last_file:
        final_files.append(always_last_file)
    special_path = locate_special_file(folder_path, input_root)
    is_mobile_group = any("mobile" in part.lower() for part in folder_path.parts)
    if is_mobile_group and special_path:
        final_files = [f for f in final_files if Path(f).resolve() != special_path]
        if final_files:
            mid_idx = len(final_files) // 2
            final_files.insert(min(mid_idx + 1, len(final_files)), str(special_path))
        else:
            final_files.insert(0, str(special_path))
        final_files.append(str(special_path))
    target_zones, excluded_zones = load_click_zones(folder_path)
    merged, pause_info, time_cursor = [], {"inter_file_pauses": [], "intra_file_pauses": []}, 0
    per_file_event_ms, per_file_inter_ms = {}, {}
    for idx, fpath in enumerate(final_files):
        fpath_obj, is_special = Path(fpath), special_path is not None and Path(fpath).resolve() == special_path.resolve()
        evs, file_duration_ms = load_json_events(fpath_obj), 0
        zb_evs, file_duration_ms = zero_base_events(evs)
        if not is_special:
            is_desktop = "deskt" in str(folder_path).lower()
            if not is_desktop:
                zb_evs, _ = add_time_of_day_fatigue(zb_evs, rng)
                zb_evs = add_micro_pauses(zb_evs, rng)
                zb_evs = add_reaction_variance(zb_evs, rng)
                zb_evs = add_mouse_jitter(zb_evs, rng, is_desktop=False, target_zones=target_zones, excluded_zones=excluded_zones)
            else:
                zb_evs = add_desktop_mouse_paths(zb_evs, rng)
                zb_evs, _ = add_time_of_day_fatigue(zb_evs, rng)
                zb_evs = add_micro_pauses(zb_evs, rng)
                zb_evs = add_reaction_variance(zb_evs, rng)
                zb_evs = add_mouse_jitter(zb_evs, rng, is_desktop=True, target_zones=target_zones, excluded_zones=excluded_zones)
            zb_evs, file_duration_ms = zero_base_events(zb_evs)
        intra_evs = zb_evs if is_special else (insert_intra_pauses(zb_evs, rng, within_max_pauses, 0, int(within_max_s))[0] if within_max_pauses > 0 else zb_evs)
        per_file_event_ms[str(fpath_obj)] = intra_evs[-1]["Time"] if intra_evs else 0
        shifted = apply_shifts(intra_evs, time_cursor)
        merged.extend(shifted)
        time_cursor = shifted[-1]["Time"] if shifted else time_cursor
        if idx < len(final_files) - 1:
            pause_ms = 1000 if is_special else rng.randint(0, int(between_max_s * 1000))
            time_cursor += pause_ms
            per_file_inter_ms[str(fpath_obj)] = pause_ms
            pause_info["inter_file_pauses"].append({"after_file": fpath_obj.name, "pause_ms": pause_ms})
        else:
            per_file_inter_ms[str(fpath_obj)] = 1000
            time_cursor += 1000
    total_ms, total_minutes = time_cursor if merged else 0, compute_minutes_from_ms(time_cursor if merged else 0)
    parts = [f"{part_from_filename(Path(f).name)}[{compute_minutes_from_ms(per_file_event_ms.get(str(f), 0) + per_file_inter_ms.get(str(f), 0))}m]" for f in final_files]
    letters = number_to_letters(version_num or 1)
    base_name = f"{'always last - ' if use_always_last else ''}{letters}_{total_minutes}m= {' - '.join(parts)}"
    safe_name = ''.join(ch for ch in base_name if ch not in '/\\:*?"<>|')
    return f"{safe_name}.json", merged, [str(p) for p in final_files], pause_info, [str(p) for p in excluded], total_minutes

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", default="originals")
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--versions", type=int, default=26)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--exclude-count", type=int, default=10)
    parser.add_argument("--within-max-time", default="33")
    parser.add_argument("--within-max-pauses", type=int, default=2)
    parser.add_argument("--between-max-time", default="18")
    args = parser.parse_args()
    rng = random.Random(args.seed) if args.seed is not None else random.Random()
    input_root, output_parent = Path(args.input_dir), Path(args.output_dir)
    output_parent.mkdir(parents=True, exist_ok=True)
    counter = int(os.environ.get("BUNDLE_SEQ", "").strip() or read_counter_file() or 1)
    if not os.environ.get("BUNDLE_SEQ"):
        write_counter_file(counter + 1)
    output_base_name, output_root = f"merged_bundle_{counter}", output_parent / f"merged_bundle_{counter}"
    output_root.mkdir(parents=True, exist_ok=True)
    folder_dirs = find_all_dirs_with_json(input_root)
    if not folder_dirs:
        print(f"No JSON files found in {input_root}", file=sys.stderr)
        return
    try:
        within_max_s = parse_time_to_seconds(args.within_max_time)
        between_max_s = parse_time_to_seconds(args.between_max_time)
    except Exception as e:
        print(f"ERROR parsing time: {e}", file=sys.stderr)
        return
    all_written_paths = []
    for folder in folder_dirs:
        files = find_json_files_in_dir(folder)
        if not files:
            continue
        try:
            rel_folder = folder.relative_to(input_root)
        except:
            rel_folder = Path(folder.name)
        out_folder_for_group = output_root / rel_folder
        out_folder_for_group.mkdir(parents=True, exist_ok=True)
        selector = NonRepeatingSelector(rng)
        for v in range(1, max(1, args.versions) + 1):
            merged_fname, merged_events, finals, pauses, excluded, total_minutes = generate_version_for_folder(files, rng, v, args.exclude_count, within_max_s, args.within_max_pauses, between_max_s, folder, input_root, selector)
            if not merged_fname:
                continue
            out_path = out_folder_for_group / merged_fname
            try:
                out_path.write_text(json.dumps(merged_events, indent=2, ensure_ascii=False), encoding="utf-8")
                print(f"WROTE: {out_path}")
                all_written_paths.append(out_path)
            except Exception as e:
                print(f"ERROR writing {out_path}: {e}", file=sys.stderr)
    zip_path = output_parent / f"{output_base_name}.zip"
    with ZipFile(zip_path, "w") as zf:
        for fpath in all_written_paths:
            try:
                arcname = str(fpath.relative_to(output_parent))
            except:
                arcname = f"{output_base_name}/{fpath.name}"
            zf.write(fpath, arcname=arcname)
    print(f"DONE. Created: {zip_path}")

if __name__ == "__main__":
    main()
