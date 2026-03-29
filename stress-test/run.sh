#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run.sh – Orchestration script for the crash-safety stress test
#
# Steps:
#  1. Clean up any previous test data.
#  2. Start the writer process (runs indefinitely until killed).
#  3. Let it write for a configurable number of seconds.
#  4. Kill the writer process with SIGKILL (simulates a hard crash).
#  5. Run the recovery script, which:
#       a. Opens the store with LOCK_RECLAIM.
#       b. Checks readability and writability.
#       c. Reports data loss and verifies it is within the expected bounds.
#
# Exit code: 0 on success, 1 on failure.
# ---------------------------------------------------------------------------

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

DATA_DIR="${SCRIPT_DIR}/stress-data"
STATS_FILE="${SCRIPT_DIR}/writer-stats.json"

# How long (seconds) to let the writer run before killing it.
# Keep it short enough for CI, but long enough to accumulate meaningful data.
WRITE_DURATION="${WRITE_DURATION:-5}"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log()  { echo "[stress-test] $*"; }
fail() { echo "[stress-test] FAILED: $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 1. Clean up previous test data
# ---------------------------------------------------------------------------
log "Cleaning up previous test data in ${DATA_DIR} ..."
rm -rf "${DATA_DIR}"
rm -f  "${STATS_FILE}"

# ---------------------------------------------------------------------------
# 2. Start the writer in the background
# ---------------------------------------------------------------------------
log "Starting writer (will run for ${WRITE_DURATION}s before being killed) ..."
node "${SCRIPT_DIR}/writer.js" "${DATA_DIR}" "${STATS_FILE}" &
WRITER_PID=$!
log "Writer PID: ${WRITER_PID}"

# ---------------------------------------------------------------------------
# 3. Wait for the write duration
# ---------------------------------------------------------------------------
sleep "${WRITE_DURATION}"

# ---------------------------------------------------------------------------
# 4. Kill the writer forcefully (SIGKILL = hard crash, no cleanup)
# ---------------------------------------------------------------------------
if kill -0 "${WRITER_PID}" 2>/dev/null; then
    log "Sending SIGKILL to writer (PID ${WRITER_PID}) ..."
    kill -9 "${WRITER_PID}"
    wait "${WRITER_PID}" 2>/dev/null || true
    log "Writer killed."
else
    fail "Writer process exited on its own before being killed – test inconclusive."
fi

# Verify writer did produce some stats before dying
if [ ! -f "${STATS_FILE}" ]; then
    fail "Stats file not found after killing writer – writer may not have written anything."
fi

TOTAL_WRITTEN=$(node -e "process.stdout.write(String(require('${STATS_FILE}').totalWritten))")
log "Writer reported ${TOTAL_WRITTEN} events committed before crash."

if [ "${TOTAL_WRITTEN}" -lt 1 ]; then
    fail "Writer did not commit any events before being killed."
fi

# ---------------------------------------------------------------------------
# 5. Run the recovery script
# ---------------------------------------------------------------------------
log "Running recovery script ..."
node "${SCRIPT_DIR}/recovery.js" "${DATA_DIR}" "${STATS_FILE}"
RECOVERY_EXIT=$?

# ---------------------------------------------------------------------------
# Final result
# ---------------------------------------------------------------------------
if [ "${RECOVERY_EXIT}" -ne 0 ]; then
    fail "Recovery script exited with code ${RECOVERY_EXIT}."
fi

log "Stress test completed successfully."
exit 0
