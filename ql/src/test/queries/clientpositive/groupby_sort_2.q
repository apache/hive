set hive.mapred.mode=nonstrict;
set hive.exec.reducers.max = 10;
set hive.map.groupby.sorted=true;

-- SORT_QUERY_RESULTS

CREATE TABLE T1_n51(key STRING, val STRING)
CLUSTERED BY (key) SORTED BY (val) INTO 2 BUCKETS STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/bucket_files/000000_0' INTO TABLE T1_n51;

-- perform an insert to make sure there are 2 files
INSERT OVERWRITE TABLE T1_n51 select key, val from T1_n51;

CREATE TABLE outputTbl1_n10(val string, cnt int);

-- The plan should not be converted to a map-side group by even though the group by key
-- matches the sorted key.
EXPLAIN
INSERT OVERWRITE TABLE outputTbl1_n10
SELECT val, count(1) FROM T1_n51 GROUP BY val;

INSERT OVERWRITE TABLE outputTbl1_n10
SELECT val, count(1) FROM T1_n51 GROUP BY val;

SELECT * FROM outputTbl1_n10;
