SET hive.hbase.snapshot.name=src_hbase_snapshot;
SET hive.hbase.snapshot.restoredir=/tmp;

SELECT * FROM src_hbase LIMIT 5;

SELECT value FROM src_hbase LIMIT 5;

select count(*) from src_hbase;
