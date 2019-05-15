--! qt:dataset:src
SET hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
set hive.exec.reducers.max = 1;
set hive.map.groupby.sorted=true;

-- SORT_QUERY_RESULTS

CREATE TABLE T1_n18(key STRING, val STRING) PARTITIONED BY (ds string)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;

-- perform an insert to make sure there are 2 files
INSERT OVERWRITE TABLE T1_n18 PARTITION (ds='1')
SELECT * from src where key < 10;

-- The plan is optimized to perform partial aggregation on the mapper
EXPLAIN select count(distinct key) from T1_n18;
select count(distinct key) from T1_n18;

-- The plan is optimized to perform partial aggregation on the mapper
EXPLAIN select count(distinct key), count(1), count(key), sum(distinct key) from T1_n18;
select count(distinct key), count(1), count(key), sum(distinct key) from T1_n18;

-- The plan is not changed in the presence of a grouping key
EXPLAIN select count(distinct key), count(1), count(key), sum(distinct key) from T1_n18 group by key;
select count(distinct key), count(1), count(key), sum(distinct key) from T1_n18 group by key;

-- The plan is not changed in the presence of a grouping key
EXPLAIN select key, count(distinct key), count(1), count(key), sum(distinct key) from T1_n18 group by key;
select key, count(distinct key), count(1), count(key), sum(distinct key) from T1_n18 group by key;

-- The plan is not changed in the presence of a grouping key expression
EXPLAIN select count(distinct key+key) from T1_n18;
select count(distinct key+key) from T1_n18;

EXPLAIN select count(distinct 1) from T1_n18;
select count(distinct 1) from T1_n18;

set hive.map.aggr=false;

-- no plan change if map aggr is turned off
EXPLAIN select count(distinct key) from T1_n18;
select count(distinct key) from T1_n18;
