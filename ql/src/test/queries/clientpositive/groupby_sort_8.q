SET hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
set hive.exec.reducers.max = 10;
set hive.map.groupby.sorted=true;

-- SORT_QUERY_RESULTS

CREATE TABLE T1_n45(key STRING, val STRING) PARTITIONED BY (ds string)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/bucket_files/000000_0' INTO TABLE T1_n45  PARTITION (ds='1');

-- perform an insert to make sure there are 2 files
INSERT OVERWRITE TABLE T1_n45 PARTITION (ds='1') select key, val from T1_n45 where ds = '1';

-- The plan is not converted to a map-side, since although the sorting columns and grouping
-- columns match, the user is issueing a distinct.
-- However, after HIVE-4310, partial aggregation is performed on the mapper
EXPLAIN
select count(distinct key) from T1_n45;
select count(distinct key) from T1_n45;

DROP TABLE T1_n45;
