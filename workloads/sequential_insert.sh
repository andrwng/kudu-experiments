#!/usr/bin/bash
set -ex

TIMEOUT_STR=""
if [[ -n $WORKLOAD_TIMEOUT ]]; then
  TIMEOUT_STR="timeout $WORKLOAD_TIMEOUT";
fi

$TIMEOUT_STR kudu perf loadgen $KUDU_MASTERS --flush_per_n_rows=0 --table_num_buckets=2 --table_num_replicas=1 --run_scan=false --keep_auto_table=true --num_rows_per_thread=0 --string_len=1 --num_threads=1 --num_clients=1 --insert_order=sequential
