SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
set hive.mapred.mode=nonstrict;
set hive.exec.reducers.max = 10;
set hive.map.groupby.sorted=true;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

-- SORT_QUERY_RESULTS

CREATE TABLE T1_mm(key STRING, val STRING) PARTITIONED BY (ds string)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE  tblproperties ("transactional"="true", "transactional_properties"="insert_only");

LOAD DATA LOCAL INPATH '../../data/files/bucket_files/000000_0' INTO TABLE T1_mm  PARTITION (ds='1');

INSERT OVERWRITE TABLE T1_mm PARTITION (ds='1') select key, val from T1_mm where ds = '1';


set hive.fetch.task.conversion=none;

select * from T1_mm;

explain
select count(distinct key) from T1_mm;
select count(distinct key) from T1_mm;

DROP TABLE T1_mm;
