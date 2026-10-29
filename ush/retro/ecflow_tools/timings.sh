#!/bin/bash
# Report retro wall time, Slurm queue wait, and job execution time.
set -euo pipefail

tools_dir=$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)
repo=$(cd "${tools_dir}/../../.." && pwd)
# Use the same experiment settings as the suite generation and control tools.
# shellcheck source=/dev/null
. "${repo}/ecf/defs/retro/load_config.sh"

exec python3 "${tools_dir}/timings.py" \
  --suite "${RRFS_SUITE}" \
  --ecf-home "${ECF_HOME}" \
  "$@"
