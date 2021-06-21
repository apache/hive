set hive.stats.dbclass=fs;
set hive.stats.fetch.column.stats=true;
set datanucleus.cache.collections=false;

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;

set hive.stats.autogather=true;
set hive.stats.column.autogather=true;
set hive.compute.query.using.stats=true;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;

set hive.fetch.task.conversion=none;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.query.results.cache.enabled=false;

DROP TABLE IF EXISTS test_stats;
DROP TABLE IF EXISTS test_stats_2;

CREATE TABLE test_stats(i int, j bigint) STORED AS ORC tblproperties ("transactional"="true", "transactional_properties"="insert_only");
CREATE TABLE test_stats_2(i int) PARTITIONED BY (j bigint) STORED AS ORC tblproperties ("transactional"="true", "transactional_properties"="insert_only");

INSERT INTO test_stats VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, NULL);
SELECT * FROM test_stats;


INSERT OVERWRITE TABLE test_stats_2 partition(j) SELECT i, j FROM test_stats WHERE j IS NOT NULL;
INSERT OVERWRITE TABLE test_stats_2 partition(j) SELECT i, j FROM test_stats WHERE j IS NULL;

-- stats should be COMPLETE
EXPLAIN SELECT i, count(*) as c FROM test_stats_2 GROUP BY i ORDER BY c DESC LIMIT 10;
