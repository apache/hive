set fs.defaultFS=${hiveconf:hbase.rootdir};

--! qt:dataset:src_hbase
set hive.stats.column.autogather=true;
SET hive.hbase.snapshot.name=src_hbase_snapshot;
SET hive.hbase.snapshot.restoredir=/tmp;

SELECT * FROM src_hbase LIMIT 5;

SELECT value FROM src_hbase LIMIT 5;

select count(*) from src_hbase;

reset fs.defaultFS;