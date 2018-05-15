kudu perf loadgen $KUDU_MASTER --flush_per_n_rows=1 --table_num_buckets=1 --table_num_replicas=1 --run_scan=false --keep_auto_table=true
