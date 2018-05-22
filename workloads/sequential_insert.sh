#!/usr/bin/bash
set -ex

TIMEOUT_STR=""
if [[ -n $WORKLOAD_TIMEOUT ]]; then
  TIMEOUT_STR="timeout $WORKLOAD_TIMEOUT";
fi

$TIMEOUT_STR kudu perf loadgen $KUDU_MASTERS --flush_per_n_rows=1 --table_num_buckets=128 --table_num_replicas=1 --run_scan=false --keep_auto_table=true --num_rows_per_thread=0 --string_len=2048
