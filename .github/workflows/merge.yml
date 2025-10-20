name: Merge macros (preserve UI inputs)

on:
  workflow_dispatch:
    inputs:
      input_dir:
        description: 'Parent directory containing group subfolders (default: originals)'
        required: false
        default: 'originals'
      output_dir:
        description: 'Directory to write merged files and ZIP (default: output)'
        required: false
        default: 'output'
      versions:
        description: 'Number of versions per group'
        required: false
        default: '5'
      seed:
        description: 'Optional RNG seed (leave empty for random)'
        required: false
        default: ''
      force:
        description: 'Process groups even if previously processed? (true/false)'
        required: false
        default: 'false'
      exclude_count:
        description: 'How many files to randomly exclude per version (0-3)'
        required: false
        default: '1'
      intra_file_enabled:
        description: 'Enable intra-file random pauses? (true/false)'
        required: false
        default: 'false'
      intra_file_max:
        description: 'Max intra-file pauses per file'
        required: false
        default: '4'
      intra_file_min_mins:
        description: 'Min intra-file pause minutes (default 1)'
        required: false
        default: '1'
      intra_file_max_mins:
        description: 'Max intra-file pause minutes (default 3)'
        required: false
        default: '3'

jobs:
  merge:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Run merge_macros with UI inputs passed through
        run: |
          set -euo pipefail
          INPUT_DIR="${{ github.event.inputs.input_dir }}"
          OUTPUT_DIR="${{ github.event.inputs.output_dir }}"
          VERSIONS="${{ github.event.inputs.versions }}"
          SEED="${{ github.event.inputs.seed }}"
          FORCE="${{ github.event.inputs.force }}"
          EXC="${{ github.event.inputs.exclude_count }}"
          INTRA_EN="${{ github.event.inputs.intra_file_enabled }}"
          INTRA_MAX="${{ github.event.inputs.intra_file_max }}"
          INTRA_MIN="${{ github.event.inputs.intra_file_min_mins }}"
          INTRA_MAXM="${{ github.event.inputs.intra_file_max_mins }}"

          ARGS="--input-dir \"${INPUT_DIR}\" --output-dir \"${OUTPUT_DIR}\" --versions ${VERSIONS} --exclude-count ${EXC} --intra-file-max ${INTRA_MAX} --intra-file-min-mins ${INTRA_MIN} --intra-file-max-mins ${INTRA_MAXM}"

          if [ -n "${SEED}" ]; then ARGS="${ARGS} --seed ${SEED}"; fi
          if [ "${FORCE}" = 'true' ]; then ARGS="${ARGS} --force"; fi
          if [ "${INTRA_EN}" = 'true' ]; then ARGS="${ARGS} --intra-file-enabled"; fi

          echo "Running: python merge_macros.py ${ARGS}"
          eval python merge_macros.py ${ARGS}

      - name: Upload merged ZIP
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: merged-zip
          path: ${{ github.event.inputs.output_dir }}/merged_bundle.zip
