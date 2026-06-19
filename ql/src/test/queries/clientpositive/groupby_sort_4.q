set hive.mapred.mode=nonstrict;
set hive.exec.reducers.max = 10;
set hive.map.groupby.sorted=true;

-- SORT_QUERY_RESULTS

CREATE TABLE T1_n133(key STRING, val STRING)
CLUSTERED BY (key, val) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/bucket_files/000000_0' INTO TABLE T1_n133;

-- perform an insert to make sure there are 2 files
INSERT OVERWRITE TABLE T1_n133 select key, val from T1_n133;

CREATE TABLE outputTbl1_n31(key STRING, cnt INT);

-- The plan should not be converted to a map-side group by.
-- However, there should no hash-based aggregation on the map-side
EXPLAIN
INSERT OVERWRITE TABLE outputTbl1_n31
SELECT key, count(1) FROM T1_n133 GROUP BY key;

INSERT OVERWRITE TABLE outputTbl1_n31
SELECT key, count(1) FROM T1_n133 GROUP BY key;

SELECT * FROM outputTbl1_n31;

CREATE TABLE outputTbl2_n8(key STRING, val STRING, cnt INT);

-- The plan should not be converted to a map-side group by.
-- Hash-based aggregations should be performed on the map-side
EXPLAIN
INSERT OVERWRITE TABLE outputTbl2_n8
SELECT key, val, count(1) FROM T1_n133 GROUP BY key, val;

INSERT OVERWRITE TABLE outputTbl2_n8
SELECT key, val, count(1) FROM T1_n133 GROUP BY key, val;

SELECT * FROM outputTbl2_n8;
