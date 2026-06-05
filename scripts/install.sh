#!/usr/bin/env bash
# Build the hiveq-sdk wheel and install it into the active Python environment.
#
# Usage:
#   scripts/install.sh            # build wheel into dist/ and pip-install it
#   scripts/install.sh --dev      # editable install (pip install -e .) for local dev
#   scripts/install.sh --wheel    # build the wheel only, don't install
#
# The active `python` is used (respects conda/venv). Run from anywhere.
set -euo pipefail

# Resolve repo root regardless of where this is invoked from.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT}"

PY="${PYTHON:-python}"

mode="wheel-install"
for arg in "$@"; do
  case "${arg}" in
    --dev|-e)   mode="dev" ;;
    --wheel)    mode="wheel-only" ;;
    -h|--help)
      sed -n '2,12p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) echo "unknown arg: ${arg}" >&2; exit 2 ;;
  esac
done

echo ">> python: $(${PY} -c 'import sys; print(sys.executable)')"

if [[ "${mode}" == "dev" ]]; then
  echo ">> editable install (pip install -e .)"
  "${PY}" -m pip install -e .
  echo ">> done (editable)."
  exit 0
fi

# Fresh wheel: clear stale artifacts so we never install an old build.
echo ">> cleaning dist/ build/ *.egg-info"
rm -rf dist build src/*.egg-info

echo ">> building wheel + sdist"
"${PY}" -m build

WHEEL="$(ls -t dist/*.whl 2>/dev/null | head -1 || true)"
if [[ -z "${WHEEL}" ]]; then
  echo "!! no wheel produced in dist/" >&2
  exit 1
fi
echo ">> built ${WHEEL}"

if [[ "${mode}" == "wheel-only" ]]; then
  echo ">> wheel-only: skipping install."
  exit 0
fi

echo ">> installing ${WHEEL}"
# Two-step install so we always refresh the SDK's own code without forcing a
# full dependency re-resolution. The private `hiveq-orchestrator` dep is not on
# PyPI (installed separately from the platform index), so a plain
# --force-reinstall would try to re-fetch it and fail.
#   1) force-reinstall ONLY hiveq-sdk itself (--no-deps) -> always fresh code.
#   2) plain install -> pull any *missing* public deps; satisfied ones (incl.
#      the private orchestrator) are left untouched.
"${PY}" -m pip install --force-reinstall --no-deps "${WHEEL}"
"${PY}" -m pip install "${WHEEL}"

echo ">> verifying import"
"${PY}" -c "import hiveq.flow as hf; print('hiveq.flow OK ->', hf.__file__)"
echo ">> done."
