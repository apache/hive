#!/usr/bin/env bash

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POM="$here/../../ql/pom.xml"
export JAVA_HOME="${JAVA_HOME:-/home/nous/.jdks/liberica-full-21.0.10}"
export PATH="$JAVA_HOME/bin:$PATH"

REPS="${REPS:-5}"
WARMUP="${WARMUP:-2}"
OUT="${OUT:-../data/erp}"
SCRATCH="${SCRATCH:-/home/nous/hive-scratch}"
BATCH="${BATCH:-25}"

cmd="${1:-help}"
SMOKE=""
if [[ "${2:-}" == "smoke" ]]; then SMOKE="-Dperf.smoke=true"; fi

MVN=(mvn -f "$POM" test -o -Drat.skip=true -Dperf.run=true -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false)

prep_scratch(){
  case "$SCRATCH" in ""|"/"|"/home"|"/home/nous"|"/opt"|"/tmp") echo "refusing unsafe SCRATCH=$SCRATCH"; exit 1;; esac
  rm -rf "$SCRATCH"; mkdir -p "$SCRATCH/javatmp"
}

case "$cmd" in
  clean)
    echo ">> clean /opt/hive benchmark corpus"
    "${MVN[@]}" -Dtest='TestPerf#testCleanWarehouse'
    ;;
  gen)
    echo ">> generate corpus only (smoke=${SMOKE:-off}), scratch=$SCRATCH"
    prep_scratch
    _JAVA_OPTIONS="-Djava.io.tmpdir=$SCRATCH/javatmp" \
      "${MVN[@]}" -Dtest='TestPerf#testGen' $SMOKE -Ddae.test.scratch="$SCRATCH"
    rm -rf "$SCRATCH"
    ;;
  genbatch)
    echo ">> full gen in fresh-JVM batches of $BATCH cells, scratch=$SCRATCH"
    rm -rf /opt/hive/t_* /opt/hive/ix__* /opt/hive/anon_out_* 2>/dev/null || true
    rm -rf "$here"/../../ql/tez-local-cache* 2>/dev/null || true
    from=1
    while [ "$from" -le 540 ]; do
      to=$((from + BATCH - 1)); [ "$to" -gt 540 ] && to=540
      echo ">> ===== batch cells ${from}..${to} ($(date '+%F %T')) ====="
      prep_scratch
      _JAVA_OPTIONS="-Djava.io.tmpdir=$SCRATCH/javatmp" \
        "${MVN[@]}" -Dtest='TestPerf#testGen' -Dperf.from="$from" -Dperf.to="$to" -Ddae.test.scratch="$SCRATCH"
      from=$((to + 1))
    done
    rm -rf "$SCRATCH"
    echo ">> genbatch complete: $(ls -d /opt/hive/t_* 2>/dev/null | wc -l)/540 tables, free=$(df -h / | awk 'NR==2{print $4}')"
    ;;
  perf)
    echo ">> measure existing corpus, no gen (smoke=${SMOKE:-off}, cells [${PERF_FROM:-1},${PERF_TO:-end}])"
    "${MVN[@]}" -Dtest='TestPerf#testPerfOnly' $SMOKE \
        -Dperf.reps="$REPS" -Dperf.warmup="$WARMUP" -Dperf.out="$OUT" \
        ${PERF_FROM:+-Dperf.from="$PERF_FROM"} ${PERF_TO:+-Dperf.to="$PERF_TO"}
    ;;
  genperf)
    echo ">> gen + measure in one JVM (smoke=${SMOKE:-off}), scratch=$SCRATCH"
    prep_scratch
    _JAVA_OPTIONS="-Djava.io.tmpdir=$SCRATCH/javatmp" \
      "${MVN[@]}" -Dtest='TestPerf#testGenAndPerf' $SMOKE \
        -Dperf.reps="$REPS" -Dperf.warmup="$WARMUP" -Dperf.out="$OUT" -Ddae.test.scratch="$SCRATCH"
    rm -rf "$SCRATCH"
    ;;
  plot)
    echo ">> render §6.2 figures from newest microbench-perf-summary-*.csv"
    python3 "$here/microbench_perf.py"
    ;;
  *)
    sed -n '2,30p' "$0"
    exit 0
    ;;
esac
