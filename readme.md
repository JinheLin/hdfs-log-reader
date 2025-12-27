cargo build --release

hdfs-log-reader <table_name> \
  --tidb-host localhost \
  --tidb-port 4000 \
  --batch-size 50000 \
  --max-rows 100000