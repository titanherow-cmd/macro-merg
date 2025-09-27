#!/usr/bin/env python3
import argparse
import os
import random
import zipfile

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
    os.makedirs(output_dir, exist_ok=True)

    for i in range(1, versions + 1):
        for filename in os.listdir(input_dir):
            input_path = os.path.join(input_dir, filename)
            base, ext = os.path.splitext(filename)
            output_file = os.path.join(output_dir, f"{base}_v{i}{ext}")

            if not force and os.path.exists(output_file):
                print(f"Skipping {output_file}, already exists.")
                continue

            print(f"Processing {input_path} -> {output_file}")

            # Read and write in chunks to avoid truncation
            with open(input_path, 'rb') as f_in, open(output_file, 'wb') as f_out:
                while chunk := f_in.read(1024 * 1024):  # 1 MB chunks
                    f_out.write(chunk)

            # Optional intra-file processing
            if intra_enabled:
                with open(output_file, 'ab') as f_out:
                    f_out.write(b"\n# Intra-file processing done\n")

            print(f"Merged file size: {os.path.getsize(output_file)} bytes")

def create_zip(output_dir, zip_name="merged_bundle.zip"):
    zip_path = os.path.join(output_dir, zip_name)
    print(f"Creating zip: {zip_path}")

    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for file in os.listdir(output_dir):
            file_path = os.path.join(output_dir, file)
            if os.path.isfile(file_path) and file != zip_name:
                zipf.write(file_path, file)  # store at root of zip

    print(f"Zip created successfully, size: {os.path.getsize(zip_path)} bytes")
    return zip_path

def main():
    args = parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    merge_files(args.input_dir, args.output_dir, args.versions, args.intra_file_enabled, args.force)
    zip_file = create_zip(args.output_dir)

    # Debug: list contents of zip
    print("Contents of merged zip:")
    with zipfile.ZipFile(zip_file, 'r') as zipf:
        for f in zipf.namelist():
            print(f" - {f}")

if __name__ == "__main__":
    main()
