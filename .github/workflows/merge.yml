name: Merge macros (persisted counter, scan all folders, zip top-level)

on:
  workflow_dispatch:
    inputs:
      versions:
        description: 'How many versions per group'
        required: true
        default: '26'
      within_max_time:
        description: 'Intra-file max pause time (seconds) - Used for EXEMPTED folders only'
        required: true
        default: '33'
      within_max_pauses:
        description: 'Max intra-file pauses (0-3 randomly chosen)'
        required: true
        default: '3'
      between_max_time:
        description: 'Inter-file max pause time (seconds)'
        required: true
        default: '18'
      exclude_count:
        description: 'Max files to randomly exclude per version'
        required: true
        default: '10'
      exempted_folders:
        description: 'Folders to exempt from pause/AFK rules (comma-separated folder paths, or leave empty for none)'
        required: false
        default: ''

jobs:
  merge:
    runs-on: ubuntu-latest
    permissions:
      contents: write

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4
        with:
          fetch-depth: 0
          persist-credentials: true

      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Bump run counter (expose BUNDLE_SEQ)
        id: bump
        run: |
          set -euo pipefail
          COUNTER_FILE=".github/merge_bundle_counter.txt"
          
          if [ -f "$COUNTER_FILE" ]; then
            PREV=$(cat "$COUNTER_FILE" | tr -d ' \t\n\r' || echo "")
            PREV=${PREV:-0}
          else
            PREV=0
          fi
          
          NEXT=$((PREV + 1))
          mkdir -p "$(dirname "$COUNTER_FILE")"
          echo "$NEXT" > "$COUNTER_FILE"
          
          echo "BUNDLE_SEQ=$NEXT" >> "$GITHUB_ENV"
          echo "BUMPED counter: $PREV -> $NEXT"

      - name: Commit and push bumped counter back to repo
        if: always()
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: |
          set -euo pipefail
          COUNTER_FILE=".github/merge_bundle_counter.txt"
          
          if [ ! -f "$COUNTER_FILE" ]; then
            echo "Counter file not present; nothing to commit."
            exit 0
          fi
          
          git config user.name "github-actions[bot]" || true
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com" || true
          git add "$COUNTER_FILE"
          
          if git diff --staged --quiet; then
            echo "No changes to counter to commit."
          else
            NEXT=$(cat "$COUNTER_FILE" || echo "")
            git commit -m "CI: bump merged bundle counter to ${NEXT}" || true
            git push origin HEAD || (echo "git push failed" && false)
            echo "Pushed updated counter: ${NEXT}"
          fi

      - name: Create exemption config from UI input
        run: |
          python3 << 'PYTHON_EOF'
          import json
          
          exempted_input = "${{ github.event.inputs.exempted_folders }}"
          folders = [f.strip() for f in exempted_input.split(',') if f.strip()]
          
          config = {
              "exempted_folders": folders,
              "disable_intra_pauses": len(folders) > 0,
              "disable_afk": len(folders) > 0
          }
          
          with open("exemption_config.json", "w") as f:
              json.dump(config, f, indent=2)
          
          print("Created exemption_config.json:")
          print(json.dumps(config, indent=2))
          PYTHON_EOF

      - name: Show originals (debug)
        run: |
          echo "PWD: $PWD"
          echo "Listing originals (top-level):"
          ls -la originals || true
          echo "Recursive sample (json files):"
          find originals -type f -name '*.json' -print || true

      - name: Run merge_macros.py with UI inputs
        env:
          PYTHONUNBUFFERED: '1'
        run: |
          set -euo pipefail
          
          VERSIONS="${{ github.event.inputs.versions }}"
          WITHIN_MAX_TIME="${{ github.event.inputs.within_max_time }}"
          WITHIN_MAX_PAUSES="${{ github.event.inputs.within_max_pauses }}"
          BETWEEN_MAX_TIME="${{ github.event.inputs.between_max_time }}"
          EXCLUDE_COUNT="${{ github.event.inputs.exclude_count }}"
          
          echo "Inputs:"
          echo "  versions=${VERSIONS}"
          echo "  within_max_time='${WITHIN_MAX_TIME}'"
          echo "  within_max_pauses=${WITHIN_MAX_PAUSES}"
          echo "  between_max_time='${BETWEEN_MAX_TIME}'"
          echo "  exclude_count=${EXCLUDE_COUNT}"
          echo "BUNDLE_SEQ='${BUNDLE_SEQ:-(none)}'"
          
          python3 merge_macros.py \
            --input-dir "originals" \
            --output-dir "output" \
            --versions "${VERSIONS}" \
            --within-max-time "${WITHIN_MAX_TIME}" \
            --within-max-pauses "${WITHIN_MAX_PAUSES}" \
            --between-max-time "${BETWEEN_MAX_TIME}" \
            --exclude-count "${EXCLUDE_COUNT}"

      - name: Zip merged outputs
        if: always()
        run: |
          set -euo pipefail
          
          BUNDLE="${BUNDLE_SEQ:-}"
          if [ -z "$BUNDLE" ]; then
            BUNDLE=$(cat .github/merge_bundle_counter.txt 2>/dev/null || echo "")
            BUNDLE=${BUNDLE:-1}
          fi
          
          OUTPUT_BASE="merged_bundle_${BUNDLE}"
          echo "Creating zip: ${OUTPUT_BASE}.zip from output/${OUTPUT_BASE}"
          
          if [ -d "output/${OUTPUT_BASE}" ]; then
            (cd output && zip -r "../${OUTPUT_BASE}.zip" "${OUTPUT_BASE}") || true
            ls -la "${OUTPUT_BASE}.zip" || true
            echo "ZIP_NAME=${OUTPUT_BASE}.zip" >> "$GITHUB_ENV"
          else
            echo "No 'output/${OUTPUT_BASE}' found; creating empty zip fallback"
            zip -r "${OUTPUT_BASE}.zip" || true
            echo "ZIP_NAME=${OUTPUT_BASE}.zip" >> "$GITHUB_ENV"
          fi

      - name: Upload merged ZIP artifact
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: merged-bundle
          path: ${{ env.ZIP_NAME }}
