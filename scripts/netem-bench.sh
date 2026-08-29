#!/usr/bin/env bash
# XDE controlled-network benchmark matrix (Linux, requires root for tc/netem).
#
# Establishes the pre-v1 performance baseline under realistic network
# conditions: loopback, ~20ms RTT, ~100ms RTT, bandwidth-limited, and mild
# loss - via tc/netem on a loopback device (protocol semantics preserved;
# this is real queuing/delay/loss in the kernel, not application sleeps).
#
# Usage:   sudo ./scripts/netem-bench.sh [scenario]
# Examples:
#   sudo ./scripts/netem-bench.sh            # full matrix
#   sudo ./scripts/netem-bench.sh 20ms       # single latency scenario
#
# Results append to bench-results/<timestamp>-<scenario>.txt
set -euo pipefail

SCENARIO="${1:-all}"
SIZE_MIB=128
REPS=5
OUTDIR="bench-results/$(date -u +%Y%m%d-%H%M%S)"
mkdir -p "$OUTDIR"

IFACE="lo"
cleanup() {
    tc qdisc del dev "$IFACE" root 2>/dev/null || true
}
trap cleanup EXIT

apply_netem() {
    cleanup
    local delay="$1" rate="$2" loss="$3"
    if [ "$delay" = "0" ] && [ "$rate" = "0" ] && [ "$loss" = "0" ]; then
        return
    fi
    local args=()
    [ "$delay" != "0" ] && args+=(delay "${delay}")
    if [ "$rate" != "0" ]; then
        args+=(rate "${rate}mbit")
        # Large burst so token-bucket pacing doesn't drop packets.
        args+=(burst 4mbit latency 50ms)
    fi
    [ "$loss" != "0" ] && args+=(loss "${loss}%")
    tc qdisc add dev "$IFACE" root netem "${args[@]:-}"
}

run_benches() {
    local label="$1"
    echo "=== scenario: $label (size ${SIZE_MIB}MiB × ${REPS} reps) ===" | tee "$OUTDIR/$label.txt"

    # curl references
    echo "--- curl ---" | tee -a "$OUTDIR/$label.txt"
    cargo run --release -p xde-bench --bin h2paired -- \
        --size-mib "$SIZE_MIB" --trials "$REPS" 2>&1 | tail -8 | tee -a "$OUTDIR/$label.txt" || true

    # XDE fixed + adaptive H1/H2 aggregation
    echo "--- xde aggregate (H1) ---" | tee -a "$OUTDIR/$label.txt"
    cargo run --release -p xde-bench --bin aggregate -- \
        --size-mib "$SIZE_MIB" --per-conn-mib-s 100 --conns 1,2,4 \
        2>&1 | grep -v '^\[' | tee -a "$OUTDIR/$label.txt"

    # Multi-source: equal mirrors / fast+slow / corrupt source
    echo "--- xde multi-source ---" | tee -a "$OUTDIR/$label.txt"
    cargo run --release -p xde-bench --bin multisource -- \
        --size-mib "$SIZE_MIB" 2>&1 | grep -v '^\[' | tee -a "$OUTDIR/$label.txt"

    echo "(complete)" | tee -a "$OUTDIR/$label.txt"
}

run_scenario() {
    local name="$1" delay="$2" rate="$3" loss="$4"
    echo ">>> applying netem: delay=${delay} rate=${rate}mbit loss=${loss}%"
    apply_netem "$delay" "$rate" "$loss"
    sleep 1
    run_benches "$name"
}

case "$SCENARIO" in
    all)
        run_scenario "loopback"        0     0     0
        run_scenario "rtt20ms"         10ms  0     0
        run_scenario "rtt100ms"        50ms  0     0
        run_scenario "bw50mbit"        0     50    0
        run_scenario "bw20mbit-rtt20"  10ms  20    0
        run_scenario "mild-loss"       0     0     0.5
        ;;
    loopback)  run_scenario "loopback" 0    0  0  ;;
    rtt20ms)   run_scenario "rtt20ms"  10ms 0  0  ;;
    rtt100ms)  run_scenario "rtt100ms" 50ms 0  0  ;;
    bw50mbit)  run_scenario "bw50mbit" 0    50 0  ;;
    mild-loss) run_scenario "mild-loss" 0   0  0.5 ;;
    *) echo "unknown scenario: $SCENARIO"; exit 1 ;;
esac

echo "results in $OUTDIR/"
