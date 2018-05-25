SET hive.vectorized.execution.enabled=false;
set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;
set hive.spark.dynamic.partition.pruning=true;
set hive.optimize.metadataonly=false;
set hive.optimize.index.filter=true;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask = true;
set hive.auto.convert.join.noconditionaltask.size = 10000000;

-- SORT_QUERY_RESULTS

create table dim_shops_n0 (id int, label string) row format delimited fields terminated by ',' stored as textfile;
load data local inpath '../../data/files/dim_shops.txt' into table dim_shops_n0;

create table agg_01_n0 (amount decimal) partitioned by (dim_shops_id int) row format delimited fields terminated by ',' stored as textfile;
alter table agg_01_n0 add partition (dim_shops_id = 1);
alter table agg_01_n0 add partition (dim_shops_id = 2);
alter table agg_01_n0 add partition (dim_shops_id = 3);

load data local inpath '../../data/files/agg_01-p1.txt' into table agg_01_n0 partition (dim_shops_id=1);
load data local inpath '../../data/files/agg_01-p2.txt' into table agg_01_n0 partition (dim_shops_id=2);
load data local inpath '../../data/files/agg_01-p3.txt' into table agg_01_n0 partition (dim_shops_id=3);

analyze table dim_shops_n0 compute statistics;
analyze table agg_01_n0 partition (dim_shops_id) compute statistics;

select * from dim_shops_n0;
select * from agg_01_n0;

EXPLAIN SELECT d1.label, count(*), sum(agg.amount)
FROM agg_01_n0 agg,
dim_shops_n0 d1
WHERE agg.dim_shops_id = d1.id
and
d1.label in ('foo', 'bar')
GROUP BY d1.label
ORDER BY d1.label;

SELECT d1.label, count(*), sum(agg.amount)
FROM agg_01_n0 agg,
dim_shops_n0 d1
WHERE agg.dim_shops_id = d1.id
and
d1.label in ('foo', 'bar')
GROUP BY d1.label
ORDER BY d1.label;

set hive.spark.dynamic.partition.pruning.max.data.size=1;

EXPLAIN SELECT d1.label, count(*), sum(agg.amount)
FROM agg_01_n0 agg,
dim_shops_n0 d1
WHERE agg.dim_shops_id = d1.id
and
d1.label in ('foo', 'bar')
GROUP BY d1.label
ORDER BY d1.label;

SELECT d1.label, count(*), sum(agg.amount)
FROM agg_01_n0 agg,
dim_shops_n0 d1
WHERE agg.dim_shops_id = d1.id
and
d1.label in ('foo', 'bar')
GROUP BY d1.label
ORDER BY d1.label;

EXPLAIN SELECT d1.label
FROM agg_01_n0 agg,
dim_shops_n0 d1
WHERE agg.dim_shops_id = d1.id;

SELECT d1.label
FROM agg_01_n0 agg,
dim_shops_n0 d1
WHERE agg.dim_shops_id = d1.id;

EXPLAIN SELECT agg.amount
FROM agg_01_n0 agg,
dim_shops_n0 d1
WHERE agg.dim_shops_id = d1.id
and agg.dim_shops_id = 1;

SELECT agg.amount
FROM agg_01_n0 agg,
dim_shops_n0 d1
WHERE agg.dim_shops_id = d1.id
and agg.dim_shops_id = 1;

set hive.spark.dynamic.partition.pruning.max.data.size=1000000;

EXPLAIN SELECT d1.label, count(*), sum(agg.amount)
FROM agg_01_n0 agg,
dim_shops_n0 d1
WHERE agg.dim_shops_id = d1.id
and
d1.label in ('foo', 'bar')
GROUP BY d1.label
ORDER BY d1.label;

SELECT d1.label, count(*), sum(agg.amount)
FROM agg_01_n0 agg,
dim_shops_n0 d1
WHERE agg.dim_shops_id = d1.id
and
d1.label in ('foo', 'bar')
GROUP BY d1.label
ORDER BY d1.label;


EXPLAIN 
SELECT amount FROM agg_01_n0, dim_shops_n0 WHERE dim_shops_id = id AND label = 'foo'
UNION ALL
SELECT amount FROM agg_01_n0, dim_shops_n0 WHERE dim_shops_id = id AND label = 'bar';

SELECT amount FROM agg_01_n0, dim_shops_n0 WHERE dim_shops_id = id AND label = 'foo'
UNION ALL
SELECT amount FROM agg_01_n0, dim_shops_n0 WHERE dim_shops_id = id AND label = 'bar';

set hive.spark.dynamic.partition.pruning.max.data.size=10000;
-- Dynamic partition pruning will be removed as data size exceeds the limit;
-- and for self join on partitioning column, it should not fail (HIVE-10559).
explain
select count(*)
from srcpart s1,
     srcpart s2
where s1.ds = s2.ds
;

select count(*)
from srcpart s1,
     srcpart s2
where s1.ds = s2.ds
;