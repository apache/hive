set hive.mapred.mode=nonstrict;
set hive.exec.reducers.max = 10;
set hive.map.groupby.sorted=true;

-- SORT_QUERY_RESULTS

CREATE TABLE T1_n89(key STRING, val STRING)
CLUSTERED BY (key) SORTED BY (key, val) INTO 2 BUCKETS STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/bucket_files/000000_0' INTO TABLE T1_n89;

-- perform an insert to make sure there are 2 files
INSERT OVERWRITE TABLE T1_n89 select key, val from T1_n89;

CREATE TABLE outputTbl1_n20(key string, val string, cnt int);

-- The plan should be converted to a map-side group by
EXPLAIN
INSERT OVERWRITE TABLE outputTbl1_n20
SELECT key, val, count(1) FROM T1_n89 GROUP BY key, val;

INSERT OVERWRITE TABLE outputTbl1_n20
SELECT key, val, count(1) FROM T1_n89 GROUP BY key, val;

SELECT * FROM outputTbl1_n20;

CREATE TABLE outputTbl2_n7(key string, cnt int);

-- The plan should be converted to a map-side group by
EXPLAIN
INSERT OVERWRITE TABLE outputTbl2_n7
SELECT key, count(1) FROM T1_n89 GROUP BY key;

INSERT OVERWRITE TABLE outputTbl2_n7
SELECT key, count(1) FROM T1_n89 GROUP BY key;

SELECT * FROM outputTbl2_n7;
