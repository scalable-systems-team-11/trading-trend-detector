#!/usr/bin/env bash
# Runs the benchmark matrix for the report: same machine, same input, same settings for
# every pipeline. Each run gets its own results/benchmarks/<time>-<kind>-<label>/ folder
# with result.json (written by the pipeline), docker-stats.txt and specs.txt (written here).
#
#   bench/run_matrix.sh throughput   # TCP, Kafka 1/2/4/8 workers, Flink 2/4/8, 3 runs each
#   bench/run_matrix.sh latency      # paced runs (TCP, Kafka 4w) over a busy slice
#   bench/run_matrix.sh week         # whole week, TCP
#   bench/run_matrix.sh one tcp|kafka|flink <label> [workers|parallelism]   # single run
#
# Input and repeats can be changed with env vars: INPUT, LIMIT, REPEATS, PREFIX.
set -euo pipefail
cd "$(dirname "$0")/.."

INPUT="${INPUT:-/data/prices-08-11-21.jsonl}"   # Monday, path inside the containers
LIMIT="${LIMIT:-2000000}"                       # first N price events of the day
REPEATS="${REPEATS:-3}"
PREFIX="${PREFIX:-clean}"
RESULTS=results/benchmarks
LOGS="${LOGS:-$RESULTS/logs}"
mkdir -p "$LOGS"

log() { echo "[$(date +%H:%M:%S)] $*" >&2; }

# Common settings; individual runs override what they need.
common_env() {
    export REPLAY_FILES="$INPUT" REPLAY_LIMIT="$LIMIT" REPLAY_SPEED=0 REPLAY_START= REPLAY_END=
    export REPLAY_ONLY_PRICES=1 RECEIVER_ALLOWED_LATENESS_SECONDS=0 RECEIVER_SYMBOLS=
    export RECEIVER_FLUSH_PARTIAL=0 FLINK_CONF=
}

stats_start() {  # sample docker stats of this project's containers until stats_stop
    STATS_FILE=$(mktemp)
    ( while true; do
        docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' 2>/dev/null \
          | grep trading-trend-detector | sed "s/^/$(date +%H:%M:%S) /"
      done > "$STATS_FILE" ) &
    STATS_PID=$!
}

stats_stop() { kill "$STATS_PID" 2>/dev/null || true; wait "$STATS_PID" 2>/dev/null || true; }

# Move docker stats, specs and the run log into the newest result folder of this label.
finish_run() {
    local label=$1 runlog=$2 desc=$3
    local dir
    dir=$(ls -td "$RESULTS"/*-"$label"/ 2>/dev/null | head -1 || true)
    if [[ -z "$dir" ]]; then
        log "ERROR: no result folder for $label (log: $runlog)"; return 1
    fi
    docker run --rm -v "$PWD/$RESULTS:/r" -v "$PWD/output:/o" python:3.12-slim \
        chown -R "$(id -u):$(id -g)" /r /o >/dev/null
    cp "$STATS_FILE" "$dir/docker-stats.txt"; rm -f "$STATS_FILE"
    cp "$runlog" "$dir/run.log"
    cat > "$dir/specs.txt" <<EOF
Machine:  Jens's laptop, AMD Ryzen 7 5825U (8 cores / 16 threads), 15.3 GB RAM
WSL2:     processors=8, memory=10GB (.wslconfig) -> $(nproc) vCPU visible
Docker:   $(docker compose version --short), no per-container CPU/memory limits
Run:      $desc
Input:    REPLAY_FILES=$REPLAY_FILES REPLAY_LIMIT=$REPLAY_LIMIT REPLAY_SPEED=$REPLAY_SPEED
          REPLAY_START=$REPLAY_START REPLAY_END=$REPLAY_END REPLAY_MAX_GAP=${REPLAY_MAX_GAP:-600}
          lateness ${RECEIVER_ALLOWED_LATENESS_SECONDS}s, only price events
Script:   bench/run_matrix.sh ($PREFIX)
EOF
    log "done: $dir"
}

run_tcp() {
    local label=$1 runlog="$LOGS/$1.log"
    export BENCH_LABEL=$label
    log "tcp $label"
    docker compose down --remove-orphans >/dev/null 2>&1 || true
    stats_start
    docker compose up -d receiver replayer >/dev/null 2>&1
    docker compose wait replayer >/dev/null
    # The receiver serves forever; wait until it has written the benchmark of this connection.
    until docker compose logs receiver 2>/dev/null | grep -q "benchmark written"; do sleep 2; done
    stats_stop
    docker compose logs --no-color receiver replayer > "$runlog" 2>&1
    docker compose down >/dev/null 2>&1
    finish_run "$label" "$runlog" "TCP: replayer -> receiver (1 process)"
}

run_kafka() {
    local label=$1 workers=$2 runlog="$LOGS/$1.log"
    export BENCH_LABEL=$label KAFKA_WORKERS=$workers KAFKA_PARTITIONS=$workers KAFKA_TOPIC=ticks
    log "kafka $label ($workers workers)"
    docker compose --profile kafka --profile flink down --remove-orphans >/dev/null 2>&1 || true
    stats_start
    docker compose --profile kafka up -d producer worker >/dev/null 2>&1
    docker compose --profile kafka wait worker >/dev/null
    stats_stop
    docker compose --profile kafka logs --no-color producer worker > "$runlog" 2>&1
    docker compose --profile kafka down >/dev/null 2>&1
    finish_run "$label" "$runlog" "Kafka: producer -> topic ($workers partitions, key=symbol) -> $workers Python workers"
}

run_flink() {
    local label=$1 par=$2 runlog="$LOGS/$1.log"
    export BENCH_LABEL=$label KAFKA_WORKERS=$par KAFKA_PARTITIONS=$par FLINK_PARALLELISM=$par KAFKA_TOPIC=ticks
    log "flink $label (parallelism $par)"
    docker compose --profile kafka --profile flink down --remove-orphans >/dev/null 2>&1 || true
    # Fill the topic first (Flink reads a bounded topic), then sample stats for the job only.
    docker compose --profile flink up -d producer >/dev/null 2>&1
    docker compose --profile flink wait producer >/dev/null
    stats_start
    docker compose --profile flink run --rm --no-deps flink > "$runlog.job" 2>&1
    stats_stop
    { docker compose --profile flink logs --no-color producer; cat "$runlog.job"; } > "$runlog" 2>&1
    rm -f "$runlog.job"
    docker compose --profile flink down >/dev/null 2>&1
    finish_run "$label" "$runlog" "Flink: producer fills topic ($par partitions), then PyFlink job, parallelism $par (consumer only)"
}

build() {
    log "building images"
    docker compose --profile kafka --profile flink build >/dev/null
}

case "${1:-}" in
    throughput)
        common_env; build
        for r in $(seq 1 "$REPEATS"); do
            run_tcp "$PREFIX-tcp-r$r"
            for w in 1 2 4 8; do run_kafka "$PREFIX-kafka-${w}w-r$r" "$w"; done
            for p in 2 4 8; do run_flink "$PREFIX-flink-${p}p-r$r" "$p"; done
        done ;;
    latency)
        common_env; build
        export LIMIT=0 REPLAY_LIMIT=0 REPLAY_SPEED="${SPEED:-10}"
        export REPLAY_START="${START:-09:00}" REPLAY_END="${END:-10:00}"
        run_tcp "$PREFIX-lat-tcp-${REPLAY_SPEED}x"
        run_kafka "$PREFIX-lat-kafka-4w-${REPLAY_SPEED}x" 4 ;;
    week)
        common_env; build
        export REPLAY_FILES=/data/prices-*.jsonl REPLAY_LIMIT=0
        run_tcp "$PREFIX-week-tcp" ;;
    one)
        common_env
        case "$2" in
            tcp) run_tcp "$3" ;;
            kafka) run_kafka "$3" "$4" ;;
            flink) run_flink "$3" "$4" ;;
        esac ;;
    *) sed -n 2,12p "$0"; exit 1 ;;
esac
