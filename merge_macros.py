name: Merge macros

on:
  workflow_dispatch:

jobs:
  merge-macros:
    runs-on: ubuntu-latest

    steps:
      # Step 1: Checkout the repository
      - name: Checkout repository
        uses: actions/checkout@v3

      # Step 2: Set up Python
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.x'

      # Step 3: Upgrade pip
      - name: Install dependencies
        run: python -m pip install --upgrade pip

      # Step 4: Run the merge_macros.py script
      - name: Run merge_macros.py
        run: python merge_macros.py --input-dir originals --output-dir out --versions 5 --seed $RANDOM

      # Step 5: Upload the resulting merged bundle
      - name: Upload merged bundle
        uses: actions/upload-artifact@v4
        with:
          name: merged-bundle
          path: out/merged_bundle.zip
