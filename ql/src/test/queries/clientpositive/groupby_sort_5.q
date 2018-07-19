--! qt:dataset:part
set hive.mapred.mode=nonstrict;
set hive.exec.reducers.max = 10;
set hive.map.groupby.sorted=true;

-- SORT_QUERY_RESULTS

CREATE TABLE T1_n6(key STRING, val STRING)
CLUSTERED BY (val) SORTED BY (key, val) INTO 2 BUCKETS STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/bucket_files/000000_0' INTO TABLE T1_n6;

-- perform an insert to make sure there are 2 files
INSERT OVERWRITE TABLE T1_n6 select key, val from T1_n6;

CREATE TABLE outputTbl1_n5(key STRING, val STRING, cnt INT);

-- The plan should be converted to a map-side group by, since the
-- sorting columns and grouping columns match, and all the bucketing columns
-- are part of sorting columns
EXPLAIN
INSERT OVERWRITE TABLE outputTbl1_n5
SELECT key, val, count(1) FROM T1_n6 GROUP BY key, val;

INSERT OVERWRITE TABLE outputTbl1_n5
SELECT key, val, count(1) FROM T1_n6 GROUP BY key, val;

SELECT * FROM outputTbl1_n5;

DROP TABLE T1_n6;

CREATE TABLE T1_n6(key STRING, val STRING)
CLUSTERED BY (val, key) SORTED BY (key, val) INTO 2 BUCKETS STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/bucket_files/000000_0' INTO TABLE T1_n6;

-- perform an insert to make sure there are 2 files
INSERT OVERWRITE TABLE T1_n6 select key, val from T1_n6;

-- The plan should be converted to a map-side group by, since the
-- sorting columns and grouping columns match, and all the bucketing columns
-- are part of sorting columns
EXPLAIN
INSERT OVERWRITE TABLE outputTbl1_n5
SELECT key, val, count(1) FROM T1_n6 GROUP BY key, val;

INSERT OVERWRITE TABLE outputTbl1_n5
SELECT key, val, count(1) FROM T1_n6 GROUP BY key, val;

SELECT * FROM outputTbl1_n5;

DROP TABLE T1_n6;

CREATE TABLE T1_n6(key STRING, val STRING)
CLUSTERED BY (val) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/bucket_files/000000_0' INTO TABLE T1_n6;

-- perform an insert to make sure there are 2 files
INSERT OVERWRITE TABLE T1_n6 select key, val from T1_n6;

CREATE TABLE outputTbl2_n1(key STRING, cnt INT);

-- The plan should not be converted to a map-side group by, since although the
-- sorting columns and grouping columns match, all the bucketing columns
-- are not part of sorting columns. However, no hash map aggregation is required
-- on the mapside.
EXPLAIN
INSERT OVERWRITE TABLE outputTbl2_n1
SELECT key, count(1) FROM T1_n6 GROUP BY key;

INSERT OVERWRITE TABLE outputTbl2_n1
SELECT key, count(1) FROM T1_n6 GROUP BY key;

SELECT * FROM outputTbl2_n1;

DROP TABLE T1_n6;
