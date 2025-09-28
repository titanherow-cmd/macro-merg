#!/usr/bin/env python3
import argparse
import os
import random
import zipfile
from collections import defaultdict

def parse_args():
    parser = argparse.ArgumentParser(description="Merge macros into a bundle.")
    parser.add_argument("--input-dir", required=True, help="Directory containing original files")
    parser.add_argument("--output-dir", required=True, help="Directory to save merged files")
    parser.add_argument("--versions", type=int, default=1, help="Number of versions to create")
    parser.add_argument("--seed", type=int, default=None, help="Random seed")
    parser.add_argument("--intra-file-enabled", action="store_true", help="Enable intra-file processing")
    parser.add_argument("--force", action="store_true", help="Force processing even if previously done")
    return parser.parse_args()

def merge_files(input_dir, output_dir, versions, intra_enabled, force):
    if versions < 1:
        raise ValueError("Versions must be >= 1")
    os.makedirs(output_dir, exist_ok=True)

    for i in range(1, versions + 1):
        version_folder = os.path.join(output_dir, f"version_{i}")
        os.makedirs(version_folder, exist_ok=True)

        for root, _, files in os.walk(input_dir):
            for filename in files:
                input_path = os.path.join(root, filename)

                # Compute relative path from input_dir so we can reconstruct structure
                rel_dir = os.path.relpath(root, input_dir)  # '.' if root==input_dir
                if rel_dir == ".":
                    rel_dir = ""  # will write directly under version folder (grouping handles root)
                output_subdir = os.path.join(version_folder, rel_dir) if rel_dir else version_folder
                os.makedirs(output_subdir, exist_ok=True)

                base, ext = os.path.splitext(filename)
                output_file = os.path.join(output_subdir, f"{base}_v{i}{ext}")

                if not force and os.path.exists(output_file):
                    print(f"Skipping {output_file}, already exists.")
                    continue

                print(f"Processing {input_path} -> {output_file}")

                # Write in chunks to avoid truncation
                with open(input_path, 'rb') as f_in, open(output_file, 'wb') as f_out:
                    while chunk := f_in.read(1024 * 1024):
                        f_out.write(chunk)
                    # context managers ensure flush/close

                # Optional intra-file processing
                if intra_enabled:
                    with open(output_file, 'ab') as f_out:
                        f_out.write(b"\n# Intra-file processing done\n")

                print(f"Merged file size: {os.path.getsize(output_file)} bytes")

def _top_level_group_from_relpath(relpath):
    """
    Given a relative path like:
        'folderA/sub/f.txt' -> 'folderA'
        'file_at_root.txt'  -> '__root__'
        ''                  -> '__root__' (edge)
    """
    if not relpath or relpath == ".":
        return "__root__"
    parts = relpath.split(os.sep)
    return parts[0] if parts[0] else "__root__"

def create_grouped_zip(input_dir, output_dir, zip_name="merged_bundle.zip"):
    """
    Create a zip that groups merged files by the original top-level subfolder.
    Only write groups that have at least one file (so __root__ won't appear if empty).
    """
    zip_path = os.path.join(output_dir, zip_name)
    print(f"Creating grouped zip: {zip_path}")

    # Collect files grouped by top-level original folder
    groups = defaultdict(list)  # group_name -> list of (file_path, arcname)
    for root, _, files in os.walk(output_dir):
        for file in files:
            if file == zip_name:
                continue  # skip the zip itself
            file_path = os.path.join(root, file)
            # Path relative to output_dir, e.g. 'version_1/folderA/sub/file_v1.ext'
            rel_to_out = os.path.relpath(file_path, output_dir)
            parts = rel_to_out.split(os.sep)

            if len(parts) >= 2 and parts[0].startswith("version_"):
                # parts[1:] correspond to original paths inside that version
                orig_rel_path = os.path.join(*parts[1:]) if len(parts) > 1 else ""
                orig_rel_dir = os.path.dirname(orig_rel_path)
            else:
                # Unexpected placement (file directly under out/): treat as root-origin
                orig_rel_path = rel_to_out
                orig_rel_dir = os.path.dirname(orig_rel_path)

            top_group = _top_level_group_from_relpath(orig_rel_dir)
            # Arcname within zip should be: <top_group>/<rel_to_out>, preserving versions and subpaths
            arcname = os.path.join(top_group, rel_to_out)
            groups[top_group].append((file_path, arcname))

    files_added = 0
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Write only groups that have files (so empty __root__ won't be created)
        for group_name in sorted(groups.keys()):
            entries = groups[group_name]
            if not entries:
                continue
            for file_path, arcname in entries:
                zipf.write(file_path, arcname)
                files_added += 1

    if files_added == 0:
        print("Warning: No files were added to the zip!")
    else:
        print(f"Grouped zip created successfully with {files_added} files, size: {os.path.getsize(zip_path)} bytes")

    return zip_path

def main():
    args = parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    merge_files(args.input_dir, args.output_dir, args.versions, args.intra_file_enabled, args.force)

    print("Files in output directory before zipping:")
    for root, _, files in os.walk(args.output_dir):
        for f in files:
            print(f" - {os.path.relpath(os.path.join(root, f), args.output_dir)}")

    zip_file = create_grouped_zip(args.input_dir, args.output_dir)

    # Debug: list contents of zip
    print("Contents of grouped merged zip:")
    with zipfile.ZipFile(zip_file, 'r') as zipf:
        for f in zipf.namelist():
            print(f" - {f}")

if __name__ == "__main__":
    main()
