set hive.mapred.mode=nonstrict;
set hive.exec.reducers.max = 10;
set hive.map.groupby.sorted=true;

-- SORT_QUERY_RESULTS

CREATE TABLE T1(key STRING, val STRING) PARTITIONED BY (ds string);

CREATE TABLE outputTbl1(key int, cnt int);

-- The plan should not be converted to a map-side group since no partition is being accessed
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE outputTbl1
SELECT key, count(1) FROM T1 where ds = '1' GROUP BY key;

INSERT OVERWRITE TABLE outputTbl1
SELECT key, count(1) FROM T1 where ds = '1' GROUP BY key;

SELECT * FROM outputTbl1;

LOAD DATA LOCAL INPATH '../../data/files/bucket_files/000000_0' INTO TABLE T1  PARTITION (ds='2');

-- The plan should not be converted to a map-side group since no partition is being accessed
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE outputTbl1
SELECT key, count(1) FROM T1 where ds = '1' GROUP BY key;

INSERT OVERWRITE TABLE outputTbl1
SELECT key, count(1) FROM T1 where ds = '1' GROUP BY key;

SELECT * FROM outputTbl1;

-- The plan should not be converted to a map-side group since the partition being accessed
-- is neither bucketed not sorted
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE outputTbl1
SELECT key, count(1) FROM T1 where ds = '2' GROUP BY key;

INSERT OVERWRITE TABLE outputTbl1
SELECT key, count(1) FROM T1 where ds = '2' GROUP BY key;

SELECT * FROM outputTbl1;
