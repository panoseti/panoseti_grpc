#!/bin/bash
# run-all-parallel.sh — Run all PANOSETI CI test suites concurrently

echo "🚀 Starting all CI test suites in parallel..."
echo "------------------------------------------------"

# 1. Kick off each script in the background and capture its PID
bash scripts/run-ci-tests/run-daq-control-test.sh &
PID_DAQ_CTRL=$!

bash scripts/run-ci-tests/run-daq-data-ci-test.sh &
PID_DAQ_DATA=$!

bash scripts/run-ci-tests/run-hashpipe-daq-data-ci.sh &
PID_HASHPIPE=$!

bash scripts/run-ci-tests/run-telemetry-ci-test.sh &
PID_TELEMETRY=$!

# bash scripts/run-ci-tests/run-ublox-ci-test.sh &
# PID_UBLOX=$!

# 2. Wait for all processes to finish and capture their exit statuses
FAIL_COUNT=0

wait $PID_DAQ_CTRL || { echo "❌ DAQ Control tests FAILED"; ((FAIL_COUNT++)); }
wait $PID_DAQ_DATA || { echo "❌ DAQ Data tests FAILED"; ((FAIL_COUNT++)); }
wait $PID_HASHPIPE || { echo "❌ Hashpipe DAQ Data tests FAILED"; ((FAIL_COUNT++)); }
wait $PID_TELEMETRY || { echo "❌ Telemetry tests FAILED"; ((FAIL_COUNT++)); }
# wait $PID_UBLOX || { echo "❌ U-blox tests FAILED"; ((FAIL_COUNT++)); }

echo "------------------------------------------------"
# 3. Report final status to the CI runner
if [ $FAIL_COUNT -eq 0 ]; then
    echo "✅ All test suites passed successfully!"
    exit 0
else
    echo "🚨 Pipeline Failed: $FAIL_COUNT test suite(s) encountered errors."
    exit 1
fi