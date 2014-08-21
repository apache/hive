SET hive.hbase.snapshot.name=src_hbase_snapshot;
SET hive.hbase.snapshot.restoredir=/tmp;

SELECT * FROM src_hbase LIMIT 5;
