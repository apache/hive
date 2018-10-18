--! qt:dataset:part
set hive.mapred.mode=nonstrict;
set hive.exec.reducers.max = 10;
set hive.map.groupby.sorted=true;

-- SORT_QUERY_RESULTS

CREATE TABLE T1_n104(key STRING, val STRING) PARTITIONED BY (ds string)
CLUSTERED BY (val) SORTED BY (key, val) INTO 2 BUCKETS STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/bucket_files/000000_0' INTO TABLE T1_n104  PARTITION (ds='1');

-- perform an insert to make sure there are 2 files
INSERT OVERWRITE TABLE T1_n104 PARTITION (ds='1') select key, val from T1_n104 where ds = '1';

CREATE TABLE outputTbl1_n26(key STRING, val STRING, cnt INT);

-- The plan should be converted to a map-side group by, since the
-- sorting columns and grouping columns match, and all the bucketing columns
-- are part of sorting columns
EXPLAIN
INSERT OVERWRITE TABLE outputTbl1_n26
SELECT key, val, count(1) FROM T1_n104 where ds = '1' GROUP BY key, val;

INSERT OVERWRITE TABLE outputTbl1_n26
SELECT key, val, count(1) FROM T1_n104 where ds = '1' GROUP BY key, val;

SELECT * FROM outputTbl1_n26;

DROP TABLE T1_n104;
