set hive.mapred.mode=nonstrict;
set hive.exec.reducers.max = 10;
set hive.map.groupby.sorted=true;

CREATE TABLE T1_n164(key STRING, val STRING)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/bucket_files/000000_0' INTO TABLE T1_n164;

-- perform an insert to make sure there are 2 files
INSERT OVERWRITE TABLE T1_n164 select key, val from T1_n164;

CREATE TABLE outputTbl1_n35(key int, cnt int);

-- The plan should be converted to a map-side group by if the group by key
-- matches the sorted key. However, in test mode, the group by wont be converted.
EXPLAIN
INSERT OVERWRITE TABLE outputTbl1_n35
SELECT key, count(1) FROM T1_n164 GROUP BY key;
