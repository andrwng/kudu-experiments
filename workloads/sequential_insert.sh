#!/usr/bin/env bash

timeout 600 kudu perf loadgen $KUDU_MASTERS --flush_per_n_rows=1 --table_num_buckets=2 --table_num_replicas=1 --run_scan=false --keep_auto_table=true --num_rows_per_thread=0 --string_len=1024
