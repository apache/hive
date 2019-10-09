--! qt:dataset:src1
set hive.compute.query.using.stats=false;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;
set hive.tez.dynamic.partition.pruning=true;
set hive.tez.dynamic.semijoin.reduction=true;
set hive.optimize.metadataonly=false;
set hive.optimize.index.filter=true;
set hive.tez.bigtable.minsize.semijoin.reduction=1;
set hive.tez.min.bloom.filter.entries=1;
set hive.tez.dynamic.semijoin.reduction.threshold=-999999999999;

CREATE TABLE `table_1_n2`(
  `bigint_col_7` bigint,
  `decimal2016_col_26` decimal(20,16),
  `tinyint_col_3` tinyint,
  `decimal2612_col_77` decimal(26,12),
  `timestamp_col_9` timestamp);

CREATE TABLE `table_18_n2`(
  `tinyint_col_15` tinyint,
  `decimal2709_col_9` decimal(27,9),
  `tinyint_col_20` tinyint,
  `smallint_col_19` smallint,
  `decimal1911_col_16` decimal(19,11),
  `timestamp_col_18` timestamp);

-- HIVE-15904
EXPLAIN
SELECT
COUNT(*)
FROM table_1_n2 t1

INNER JOIN table_18_n2 t2 ON (((t2.tinyint_col_15) = (t1.bigint_col_7)) AND
((t2.decimal2709_col_9) = (t1.decimal2016_col_26))) AND
((t2.tinyint_col_20) = (t1.tinyint_col_3))
WHERE (t2.smallint_col_19) IN (SELECT
COALESCE(-92, -994) AS int_col
FROM table_1_n2 tt1
INNER JOIN table_18_n2 tt2 ON (tt2.decimal1911_col_16) = (tt1.decimal2612_col_77)
WHERE (t1.timestamp_col_9) = (tt2.timestamp_col_18));

drop table table_1_n2;
drop table table_18_n2;

-- Hive 15699
CREATE TABLE srcbucket_mapjoin_n20(key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;

CREATE TABLE src2_n7 as select * from src1;
insert into src2_n7 select * from src2_n7;
insert into src2_n7 select * from src2_n7;

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_n20 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj1/000001_0' INTO TABLE srcbucket_mapjoin_n20 partition(ds='2008-04-08');

set hive.strict.checks.bucketing=false;
set hive.join.emit.interval=2;
set hive.stats.fetch.column.stats=true;
set hive.optimize.bucketingsorting=false;
set hive.stats.autogather=true;

CREATE TABLE tab_n12(key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab_n12 partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_n20;

set hive.convert.join.bucket.mapjoin.tez = true;
set hive.auto.convert.sortmerge.join = true;

set hive.auto.convert.join.noconditionaltask.size=0;
set hive.mapjoin.hybridgrace.minwbsize=125;
set hive.mapjoin.hybridgrace.minnumpartitions=4;

set hive.llap.memory.oversubscription.max.executors.per.query=3;

CREATE TABLE tab2_n6 (key int, value string, ds string);

insert into tab2_n6 select key, value, ds from tab_n12;
analyze table tab2_n6 compute statistics;
analyze table tab2_n6 compute statistics for columns;


explain
select
  count(*)
  from
  (select x.key as key, min(x.value) as value from tab2_n6 x group by x.key) a
  join
  (select x.key as key, min(x.value) as value from tab2_n6 x group by x.key) b
  on
  a.key = b.key join src1 c on a.value = c.value where c.key < 0;
