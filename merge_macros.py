#!/usr/bin/env python3
"""
merge_macros.py - Merge and augment macro JSONs

Features:
- Safe filename generation (truncation + hash)
- Fallback write on OSError (short hashed filename)
- Manifest mapping attempted descriptive names -> actual written filename
- CLI flags: --group, --max-files, --batch-index / --batch-count
- --top-level-only to only treat immediate children under input root as groups
- --dry-run prints discovered folders and exits
- Signal handling to write manifest on interrupt
"""
from pathlib import Path
import argparse, json, random, re, sys, os, math, hashlib, time, signal
from copy import deepcopy
from zipfile import ZipFile
from itertools import combinations, permutations
from collections import defaultdict

COUNTER_PATH = Path(".github/merge_bundle_counter.txt")
SPECIAL_FILENAME = "close reopen mobile screensharelink.json"
SPECIAL_KEYWORD = "screensharelink"

CANCELED = False
def _signal_handler(signum, frame):
    global CANCELED
    CANCELED = True
    raise KeyboardInterrupt(f"Received signal {signum}")

signal.signal(signal.SIGINT, _signal_handler)
try:
    signal.signal(signal.SIGTERM, _signal_handler)
except Exception:
    pass

def make_filename_safe(name: str, max_filename_len: int = 120) -> str:
    if name is None:
        name = ""
    cleaned = ''.join(ch for ch in name if ch not in '/\\:*?"<>|')
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    if len(cleaned) <= max_filename_len:
        return cleaned
    h = hashlib.sha1(cleaned.encode('utf-8')).hexdigest()[:8]
    keep = max_filename_len - len(h) - 1
    if keep <= 0:
        return h
    prefix = cleaned[:keep].rstrip()
    return f"{prefix}_{h}"

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

# --- JSON/event utilities (kept intentionally conservative) ---
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
    for idx, e in enumerate(events):
        try:
            t = int(e.get("Time", 0))
        except:
            try:
                t = int(float(e.get("Time", 0)))
            except:
                t = 0
        events_with_time.append((e, t, idx))
    try:
        events_with_time.sort(key=lambda x: (x[1], x[2]))
    except Exception as ex:
        print(f"WARNING: Could not sort events: {ex}", file=sys.stderr)
    if not events_with_time:
        return [], 0
    min_t = events_with_time[0][1]
    shifted = []
    for (e, t, _) in events_with_time:
        ne = deepcopy(e)
        ne["Time"] = t - min_t
        shifted.append(ne)
    duration_ms = shifted[-1]["Time"] if shifted else 0
    return shifted, duration_ms

def preserve_click_integrity(events):
    preserved = []
    for e in events:
        ne = deepcopy(e)
        event_type = e.get('Type', '')
        if any(t in event_type for t in ['MouseDown', 'MouseUp', 'LeftDown', 'LeftUp', 'RightDown', 'RightUp']):
            ne['Time'] = int(e.get('Time', 0))
            ne['PROTECTED'] = True
        preserved.append(ne)
    return preserved

def is_protected_event(e):
    return e.get('PROTECTED', False)

def is_click_in_zone(x, y, zone):
    try:
        return zone['x1'] <= x <= zone['x2'] and zone['y1'] <= y <= zone['y2']
    except:
        return False

# --- discovery ---
def find_all_dirs_with_json(input_root: Path, top_level_only: bool = True):
    if not input_root.exists() or not input_root.is_dir():
        return []
    groups = []
    try:
        if top_level_only:
            for child in sorted(input_root.iterdir()):
                if child.is_dir():
                    found_any = any(p.suffix.lower() == ".json" for p in child.rglob("*.json"))
                    if found_any:
                        groups.append(child)
        else:
            found = set()
            for p in sorted(input_root.rglob("*")):
                if p.is_dir():
                    try:
                        has = any(child.is_file() and child.suffix.lower() == ".json" for child in p.iterdir())
                        if has:
                            found.add(p)
                    except:
                        pass
            groups = sorted(found)
    except Exception:
        pass
    return groups

def find_json_files_in_dir(dirpath: Path):
    try:
        return sorted([p for p in dirpath.glob("*.json") if p.is_file() and not p.name.startswith("click_zones")])
    except:
        return []

# --- selector class (unchanged behavior) ---
class NonRepeatingSelector:
    def __init__(self, rng):
        self.rng = rng
        self.used_combos = set()
        self.special_files_used = set()
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
        all_perms = list(permutations(items))
        available = [p for p in all_perms if p not in self.used_combos]
        if not available:
            self.used_combos.clear()
            available = all_perms
        chosen = self.rng.choice(available)
        self.used_combos.add(chosen)
        return list(chosen)
    def mark_special_used(self, fname):
        self.special_files_used.add(fname)
    def is_special_used(self, fname):
        return fname in self.special_files_used

def locate_special_file(folder: Path, input_root: Path):
    for cand in [folder / SPECIAL_FILENAME, input_root / SPECIAL_FILENAME]:
        if cand.exists():
            return cand.resolve()
    keyword = SPECIAL_KEYWORD.lower()
    for p in Path.cwd().rglob("*"):
        if p.is_file() and keyword in p.name.lower():
            return p.resolve()
    return None

# --- core merging (kept conservative and similar to your original) ---
def generate_version_for_folder(files, rng, version_num, exclude_count, within_max_s, within_max_pauses, between_max_s, folder_path: Path, input_root: Path, selector, exemption_config=None):
    if not files:
        return None, None, [], [], {"inter_file_pauses": [], "intra_file_pauses": []}, [], 0
    always_first_file = next((f for f in files if Path(f).name.lower().startswith("always first")), None)
    always_last_file = next((f for f in files if Path(f).name.lower().startswith("always last")), None)
    regular_files = [f for f in files if f not in [always_first_file, always_last_file]]
    if not regular_files:
        return None, None, [], [], {"inter_file_pauses": [], "intra_file_pauses": []}, [], 0
    included, excluded = selector.select_files(regular_files, exclude_count)
    if not included:
        included = regular_files.copy()

    use_special_file = None
    if always_first_file and always_last_file:
        if version_num == 1 and not selector.is_special_used(str(always_first_file)):
            use_special_file = always_first_file; selector.mark_special_used(str(always_first_file))
        elif version_num == 2 and not selector.is_special_used(str(always_last_file)):
            use_special_file = always_last_file; selector.mark_special_used(str(always_last_file))
    elif always_first_file and not always_last_file:
        if version_num == 1 and not selector.is_special_used(str(always_first_file)):
            use_special_file = always_first_file; selector.mark_special_used(str(always_first_file))
    elif always_last_file and not always_first_file:
        if version_num == 1 and not selector.is_special_used(str(always_last_file)):
            use_special_file = always_last_file; selector.mark_special_used(str(always_last_file))

    final_files = selector.shuffle_with_memory(included)
    if use_special_file == always_first_file:
        final_files.insert(0, always_first_file)
    elif use_special_file == always_last_file:
        final_files.append(always_last_file)

    special_path = locate_special_file(folder_path, input_root)
    is_mobile_group = any("mobile" in part.lower() for part in folder_path.parts)
    if is_mobile_group and special_path:
        final_files = [f for f in final_files if f is not None and Path(f).resolve() != special_path.resolve()]
        if final_files:
            mid_idx = len(final_files) // 2
            final_files.insert(min(mid_idx + 1, len(final_files)), str(special_path))
        else:
            final_files.insert(0, str(special_path))
        final_files.append(str(special_path))
    final_files = [f for f in final_files if f is not None]

    target_zones, excluded_zones = load_click_zones(folder_path)
    merged, pause_info, time_cursor = [], {"inter_file_pauses": [], "intra_file_pauses": []}, 0
    per_file_event_ms, per_file_inter_ms = {}, {}

    for idx, fpath in enumerate(final_files):
        if fpath is None:
            continue
        fpath_obj = Path(fpath)
        is_special = special_path is not None and fpath_obj.resolve() == special_path.resolve()
        evs = load_json_events(fpath_obj)
        zb_evs, file_duration_ms = zero_base_events(evs)

        if not is_special:
            is_desktop = "deskt" in str(folder_path).lower()
            exemption_config = exemption_config or {"exempted_folders": set(), "disable_intra_pauses": False, "disable_afk": False}
            is_exempted = exemption_config["exempted_folders"] and is_folder_exempted(folder_path, exemption_config["exempted_folders"])
            zb_evs = preserve_click_integrity(zb_evs)
            if not is_desktop:
                zb_evs = add_micro_pauses(zb_evs, rng)
                zb_evs = add_reaction_variance(zb_evs, rng)
                zb_evs = add_mouse_jitter(zb_evs, rng, is_desktop=False, target_zones=target_zones, excluded_zones=excluded_zones)
                zb_evs, _ = zero_base_events(zb_evs)
                zb_evs, _ = add_time_of_day_fatigue(zb_evs, rng, is_exempted=is_exempted, max_pause_ms=0)
            else:
                zb_evs = add_desktop_mouse_paths(zb_evs, rng)
                zb_evs, _ = zero_base_events(zb_evs)
                zb_evs = add_micro_pauses(zb_evs, rng)
                zb_evs = add_reaction_variance(zb_evs, rng)
                zb_evs = add_mouse_jitter(zb_evs, rng, is_desktop=True, target_zones=target_zones, excluded_zones=excluded_zones)
                zb_evs, _ = zero_base_events(zb_evs)
                zb_evs, _ = add_time_of_day_fatigue(zb_evs, rng, is_exempted=is_exempted, max_pause_ms=0)
            zb_evs, file_duration_ms = zero_base_events(zb_evs)
            if is_exempted:
                if not exemption_config.get("disable_intra_pauses", False):
                    intra_evs, _ = insert_intra_pauses(zb_evs, rng, is_exempted=True, max_pause_s=within_max_s, max_num_pauses=within_max_pauses)
                else:
                    intra_evs = zb_evs
                if not exemption_config.get("disable_afk", False) and rng.random() < 0.5:
                    intra_evs = add_afk_pause(intra_evs, rng)
            else:
                intra_evs = zb_evs
                if rng.random() < 0.5:
                    intra_evs = add_afk_pause(intra_evs, rng)
        else:
            intra_evs = zb_evs

        per_file_event_ms[str(fpath_obj)] = intra_evs[-1]["Time"] if intra_evs else 0
        shifted = apply_shifts(intra_evs, time_cursor)
        merged.extend(shifted)
        time_cursor = shifted[-1]["Time"] if shifted else time_cursor

        if idx < len(final_files) - 1:
            exemption_config = exemption_config or {"exempted_folders": set(), "disable_intra_pauses": False, "disable_afk": False}
            is_exempted = exemption_config["exempted_folders"] and is_folder_exempted(folder_path, exemption_config["exempted_folders"])
            if is_exempted:
                pause_ms = rng.randint(0, int(between_max_s * 1000))
            else:
                pause_ms = rng.randint(1000, 12000)
            time_cursor += pause_ms
            per_file_inter_ms[str(fpath_obj)] = pause_ms
            pause_info["inter_file_pauses"].append({"after_file": fpath_obj.name, "pause_ms": pause_ms})
        else:
            per_file_inter_ms[str(fpath_obj)] = 1000
            time_cursor += 1000

    total_ms = time_cursor if merged else 0
    total_minutes = compute_minutes_from_ms(total_ms)
    parts = []
    for f in final_files:
        if f is None:
            continue
        fname = Path(f).name.lower()
        if fname.startswith("always first"):
            part_name = "first"
        elif fname.startswith("always last"):
            part_name = "last"
        else:
            part_name = part_from_filename(f)
        minutes = compute_minutes_from_ms(per_file_event_ms.get(str(f), 0) + per_file_inter_ms.get(str(f), 0))
        parts.append(f"{part_name}[{minutes}m]")

    letters = number_to_letters(version_num or 1)
    tag = ""
    if use_special_file == always_first_file and always_first_file is not None:
        tag = "FIRST"
    elif use_special_file == always_last_file and always_last_file is not None:
        tag = "LAST"

    base_name = f"{tag + ' - ' if tag else ''}{letters}_{total_minutes}m= {' - '.join(parts)}"
    safe_name = make_filename_safe(base_name, max_filename_len=120)
    return f"{safe_name}.json", base_name, merged, [str(p) for p in final_files], pause_info, [str(p) for p in excluded], total_minutes

# --- manifest writer ---
def write_manifest(manifest, output_root: Path, counter: int):
    try:
        output_root.mkdir(parents=True, exist_ok=True)
        path = output_root / f"manifest_{counter}.json"
        path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"WROTE MANIFEST: {path}")
    except Exception as e:
        print(f"WARNING: Failed writing manifest: {e}", file=sys.stderr)

# --- main CLI ---
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
    parser.add_argument("--max-files", type=int, default=0)
    parser.add_argument("--batch-index", type=int, default=0)
    parser.add_argument("--batch-count", type=int, default=1)
    parser.add_argument("--group", choices=["mobile", "desktop", "all"], default="all")
    parser.add_argument("--top-level-only", action="store_true", default=True, help="Only treat immediate children of input-dir as groups")
    parser.add_argument("--dry-run", action="store_true", help="List discovered folders and exit")
    args = parser.parse_args()

    rng = random.Random(args.seed) if args.seed is not None else random.Random()
    input_root = Path(args.input_dir)
    output_parent = Path(args.output_dir)
    output_parent.mkdir(parents=True, exist_ok=True)
    counter = int(os.environ.get("BUNDLE_SEQ", "").strip() or read_counter_file() or 1)
    if not os.environ.get("BUNDLE_SEQ"):
        write_counter_file(counter + 1)
    output_root = output_parent / f"merged_bundle_{counter}"
    output_root.mkdir(parents=True, exist_ok=True)

    folder_dirs = find_all_dirs_with_json(input_root, top_level_only=args.top_level_only)
    if not folder_dirs:
        print(f"No JSON-containing folders found under {input_root}", file=sys.stderr)
        return

    if args.group != "all":
        def is_group_folder(p: Path) -> bool:
            try:
                rel = p.relative_to(input_root)
                first = rel.parts[0].lower() if rel.parts else str(p).lower()
            except Exception:
                first = str(p).lower()
            if args.group == "mobile":
                return "mobile" in first
            else:
                return "desk" in first or "desktop" in first
        folder_dirs = [p for p in folder_dirs if is_group_folder(p)]
        print(f"Group filter '{args.group}' selected {len(folder_dirs)} folders")

    if args.batch_count > 1:
        if args.batch_index < 0 or args.batch_index >= args.batch_count:
            print("Invalid batch-index", file=sys.stderr); return
        folder_dirs = [p for i,p in enumerate(folder_dirs) if i % args.batch_count == args.batch_index]
        print(f"Batch {args.batch_index}/{args.batch_count}: {len(folder_dirs)} folders")

    try:
        within_max_s = parse_time_to_seconds(args.within_max_time)
        between_max_s = parse_time_to_seconds(args.between_max_time)
    except Exception as e:
        print(f"ERROR parsing times: {e}", file=sys.stderr); return

    if args.dry_run:
        print("DRY RUN: discovered folders:")
        for d in folder_dirs:
            count = sum(1 for _ in d.glob("*.json"))
            depth = len(d.relative_to(input_root).parts) if d != input_root else 0
            print(f"  {d}  json_count={count}  depth={depth}")
        return

    manifest = []
    all_written = []
    written_count = 0
    max_files = args.max_files or 0
    exemption_config = load_exemption_config()

    try:
        for folder in folder_dirs:
            if CANCELED:
                raise KeyboardInterrupt("Canceled")
            files = find_json_files_in_dir(folder)
            if not files:
                continue
            try:
                rel_folder = folder.relative_to(input_root)
            except:
                rel_folder = Path(folder.name)
            out_folder = output_root / rel_folder
            out_folder.mkdir(parents=True, exist_ok=True)
            selector = NonRepeatingSelector(rng)
            for v in range(1, max(1, args.versions) + 1):
                if CANCELED:
                    raise KeyboardInterrupt("Canceled")
                merged_fname, base_name, merged_events, finals, pauses, excluded, total_minutes = generate_version_for_folder(
                    files, rng, v, args.exclude_count, within_max_s, args.within_max_pauses, between_max_s, folder, input_root, selector, exemption_config
                )
                if not merged_fname:
                    continue
                out_path = out_folder / merged_fname
                entry = {"attempted_name": base_name or merged_fname, "attempted_safe_name": merged_fname, "actual_name": None, "folder": str(rel_folder), "version": v, "seed": args.seed, "wrote": False, "error": None, "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
                try:
                    out_path.write_text(json.dumps(merged_events, indent=2, ensure_ascii=False), encoding="utf-8")
                    entry["actual_name"] = str(out_path.name); entry["wrote"] = True
                    all_written.append(out_path); written_count += 1
                    print(f"WROTE: {out_path}")
                except OSError as e:
                    entry["error"] = f"OSError: {e}"
                    print(f"WARNING: Failed to write {out_path}: {e}", file=sys.stderr)
                    try:
                        content_hash = hashlib.sha1(json.dumps(merged_events, ensure_ascii=False).encode('utf-8')).hexdigest()[:12]
                        fallback_name = f"merged_{content_hash}.json"
                        fallback_path = out_folder / fallback_name
                        fallback_path.write_text(json.dumps(merged_events, indent=2, ensure_ascii=False), encoding="utf-8")
                        entry["actual_name"] = str(fallback_path.name); entry["wrote"] = True; entry["fallback"] = True
                        all_written.append(fallback_path); written_count += 1
                        print(f"WROTE (fallback): {fallback_path}")
                    except Exception as e2:
                        entry["error"] += f" | fallback_error: {e2}"
                        print(f"ERROR: fallback write failed: {e2}", file=sys.stderr)
                except Exception as e:
                    entry["error"] = f"Exception: {e}"
                    print(f"ERROR writing {out_path}: {e}", file=sys.stderr)
                manifest.append(entry)

                if max_files and written_count >= max_files:
                    print(f"INFO: reached --max-files={max_files}, stopping early", file=sys.stderr)
                    raise KeyboardInterrupt("Reached max-files")
            write_manifest(manifest, output_root, counter)

    except KeyboardInterrupt as ki:
        print(f"INFO: interrupted: {ki}", file=sys.stderr)
        write_manifest(manifest, output_root, counter)
        return
    except Exception as e:
        print(f"ERROR: unexpected: {e}", file=sys.stderr)
        write_manifest(manifest, output_root, counter)
        raise

    write_manifest(manifest, output_root, counter)

    # Create per-group ZIPs at repository root so CI upload step can find them reliably
    groups = defaultdict(list)
    for p in all_written:
        try:
            rel = p.relative_to(output_root)
            first = rel.parts[0] if rel.parts else "root"
        except Exception:
            first = p.parent.name or "root"
        groups[first].append(p)

    for group_name, paths in groups.items():
        zip_name = f"merged_bundle_{counter}_{group_name}.zip"
        zip_path = output_parent.parent.joinpath(zip_name) if output_parent.parent else Path(zip_name)
        # ensure zip path is repo root
        zip_path = Path(zip_name).resolve() if False else Path(zip_name)
        try:
            with ZipFile(zip_path, "w") as zf:
                for p in paths:
                    try:
                        arcname = str(p.relative_to(output_parent.parent)) if p.exists() else p.name
                    except Exception:
                        arcname = p.name
                    zf.write(p, arcname=arcname)
            print(f"CREATED ZIP: {zip_path}")
        except Exception as e:
            print(f"WARNING: failed to create zip for group {group_name}: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
