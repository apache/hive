--! qt:dataset:srcpart
SET hive.vectorized.execution.enabled=true;
set hive.compute.query.using.stats=false;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;
set hive.tez.dynamic.partition.pruning=true;
set hive.optimize.metadataonly=false;
set hive.optimize.index.filter=true;
set hive.tez.min.bloom.filter.entries=1;
set hive.tez.bigtable.minsize.semijoin.reduction=1;

select distinct ds from srcpart;
select distinct hr from srcpart;

CREATE TABLE srcpart_iceberg (key STRING, value STRING)
PARTITIONED BY (ds STRING, hr STRING) STORED BY ICEBERG STORED AS ORC;
INSERT INTO srcpart_iceberg select * from srcpart;

EXPLAIN create table srcpart_date_n2 as select ds as ds, ds as `date`  from srcpart group by ds;
create table srcpart_date_n2 as select ds as ds, ds as `date` from srcpart group by ds;
create table srcpart_hour_n0 as select hr as hr, hr as hour from srcpart group by hr;
create table srcpart_date_hour_n0 as select ds as ds, ds as `date`, hr as hr, hr as hour from srcpart group by ds, hr;
create table srcpart_double_hour_n0 as select (hr*2) as hr, hr as hour from srcpart group by hr;

-- single column, single key
EXPLAIN select count(*) from srcpart_iceberg join srcpart_date_n2 on (srcpart_iceberg.ds = srcpart_date_n2.ds) where srcpart_date_n2.`date` = '2008-04-08';
select count(*) from srcpart_iceberg join srcpart_date_n2 on (srcpart_iceberg.ds = srcpart_date_n2.ds) where srcpart_date_n2.`date` = '2008-04-08';
set hive.tez.dynamic.partition.pruning=false;
EXPLAIN select count(*) from srcpart_iceberg join srcpart_date_n2 on (srcpart_iceberg.ds = srcpart_date_n2.ds) where srcpart_date_n2.`date` = '2008-04-08';
select count(*) from srcpart_iceberg join srcpart_date_n2 on (srcpart_iceberg.ds = srcpart_date_n2.ds) where srcpart_date_n2.`date` = '2008-04-08';
set hive.tez.dynamic.partition.pruning=true;
select count(*) from srcpart where ds = '2008-04-08';

-- multiple sources, single key
EXPLAIN select count(*) from srcpart_iceberg join srcpart_date_n2 on (srcpart_iceberg.ds = srcpart_date_n2.ds) join srcpart_hour_n0 on (srcpart_iceberg.hr = srcpart_hour_n0.hr)
where srcpart_date_n2.`date` = '2008-04-08' and srcpart_hour_n0.hour = 11;
select count(*) from srcpart_iceberg join srcpart_date_n2 on (srcpart_iceberg.ds = srcpart_date_n2.ds) join srcpart_hour_n0 on (srcpart_iceberg.hr = srcpart_hour_n0.hr)
where srcpart_date_n2.`date` = '2008-04-08' and srcpart_hour_n0.hour = 11;
set hive.tez.dynamic.partition.pruning=false;
EXPLAIN select count(*) from srcpart_iceberg join srcpart_date_n2 on (srcpart_iceberg.ds = srcpart_date_n2.ds) join srcpart_hour_n0 on (srcpart_iceberg.hr = srcpart_hour_n0.hr)
where srcpart_date_n2.`date` = '2008-04-08' and srcpart_hour_n0.hour = 11;
select count(*) from srcpart_iceberg join srcpart_date_n2 on (srcpart_iceberg.ds = srcpart_date_n2.ds) join srcpart_hour_n0 on (srcpart_iceberg.hr = srcpart_hour_n0.hr)
where srcpart_date_n2.`date` = '2008-04-08' and srcpart_hour_n0.hour = 11;
set hive.tez.dynamic.partition.pruning=true;
select count(*) from srcpart where hr = 11 and ds = '2008-04-08';

-- multiple columns single source
EXPLAIN select count(*) from srcpart_iceberg join srcpart_date_hour_n0 on (srcpart_iceberg.ds = srcpart_date_hour_n0.ds and srcpart_iceberg.hr = srcpart_date_hour_n0.hr) where srcpart_date_hour_n0.`date` = '2008-04-08' and srcpart_date_hour_n0.hour = 11;
select count(*) from srcpart_iceberg join srcpart_date_hour_n0 on (srcpart_iceberg.ds = srcpart_date_hour_n0.ds and srcpart_iceberg.hr = srcpart_date_hour_n0.hr) where srcpart_date_hour_n0.`date` = '2008-04-08' and srcpart_date_hour_n0.hour = 11;
set hive.tez.dynamic.partition.pruning=false;
EXPLAIN select count(*) from srcpart_iceberg join srcpart_date_hour_n0 on (srcpart_iceberg.ds = srcpart_date_hour_n0.ds and srcpart_iceberg.hr = srcpart_date_hour_n0.hr) where srcpart_date_hour_n0.`date` = '2008-04-08' and srcpart_date_hour_n0.hour = 11;
select count(*) from srcpart_iceberg join srcpart_date_hour_n0 on (srcpart_iceberg.ds = srcpart_date_hour_n0.ds and srcpart_iceberg.hr = srcpart_date_hour_n0.hr) where srcpart_date_hour_n0.`date` = '2008-04-08' and srcpart_date_hour_n0.hour = 11;
set hive.tez.dynamic.partition.pruning=true;
select count(*) from srcpart where ds = '2008-04-08' and hr = 11;

-- empty set
EXPLAIN select count(*) from srcpart_iceberg join srcpart_date_n2 on (srcpart_iceberg.ds = srcpart_date_n2.ds) where srcpart_date_n2.`date` = 'I DONT EXIST';
select count(*) from srcpart_iceberg join srcpart_date_n2 on (srcpart_iceberg.ds = srcpart_date_n2.ds) where srcpart_date_n2.`date` = 'I DONT EXIST';
set hive.tez.dynamic.partition.pruning=false;
EXPLAIN select count(*) from srcpart_iceberg join srcpart_date_n2 on (srcpart_iceberg.ds = srcpart_date_n2.ds) where srcpart_date_n2.`date` = 'I DONT EXIST';
select count(*) from srcpart_iceberg join srcpart_date_n2 on (srcpart_iceberg.ds = srcpart_date_n2.ds) where srcpart_date_n2.`date` = 'I DONT EXIST';
set hive.tez.dynamic.partition.pruning=true;
select count(*) from srcpart where ds = 'I DONT EXIST';

-- expressions
EXPLAIN select count(*) from srcpart_iceberg join srcpart_double_hour_n0 on (srcpart_iceberg.hr = cast(cast(srcpart_double_hour_n0.hr/2 as int) as string)) where srcpart_double_hour_n0.hour = 11;
select count(*) from srcpart_iceberg join srcpart_double_hour_n0 on (srcpart_iceberg.hr = cast(cast(srcpart_double_hour_n0.hr/2 as int) as string)) where srcpart_double_hour_n0.hour = 11;
set hive.tez.dynamic.partition.pruning=false;
EXPLAIN select count(*) from srcpart_iceberg join srcpart_double_hour_n0 on (srcpart_iceberg.hr = cast(cast(srcpart_double_hour_n0.hr/2 as int) as string)) where srcpart_double_hour_n0.hour = 11;
select count(*) from srcpart_iceberg join srcpart_double_hour_n0 on (srcpart_iceberg.hr = cast(cast(srcpart_double_hour_n0.hr/2 as int) as string)) where srcpart_double_hour_n0.hour = 11;
set hive.tez.dynamic.partition.pruning=true;

-- old style join syntax
EXPLAIN select count(*) from srcpart_iceberg, srcpart_date_hour_n0 where srcpart_date_hour_n0.`date` = '2008-04-08' and srcpart_date_hour_n0.hour = 11 and srcpart_iceberg.ds = srcpart_date_hour_n0.ds and srcpart_iceberg.hr = srcpart_date_hour_n0.hr;
select count(*) from srcpart_iceberg, srcpart_date_hour_n0 where srcpart_date_hour_n0.`date` = '2008-04-08' and srcpart_date_hour_n0.hour = 11 and srcpart_iceberg.ds = srcpart_date_hour_n0.ds and srcpart_iceberg.hr = srcpart_date_hour_n0.hr;

-- with static pruning
EXPLAIN select count(*) from srcpart_iceberg join srcpart_date_n2 on (srcpart_iceberg.ds = srcpart_date_n2.ds) join srcpart_hour_n0 on (srcpart_iceberg.hr = srcpart_hour_n0.hr)
where srcpart_date_n2.`date` = '2008-04-08' and srcpart_hour_n0.hour = 11 and srcpart_iceberg.hr = 11;
select count(*) from srcpart_iceberg join srcpart_date_n2 on (srcpart_iceberg.ds = srcpart_date_n2.ds) join srcpart_hour_n0 on (srcpart_iceberg.hr = srcpart_hour_n0.hr)
where srcpart_date_n2.`date` = '2008-04-08' and srcpart_hour_n0.hour = 11 and srcpart_iceberg.hr = 11;
EXPLAIN select count(*) from srcpart_iceberg join srcpart_date_n2 on (srcpart_iceberg.ds = srcpart_date_n2.ds) join srcpart_hour_n0 on (srcpart_iceberg.hr = srcpart_hour_n0.hr)
where srcpart_date_n2.`date` = '2008-04-08' and srcpart_iceberg.hr = 13;
select count(*) from srcpart_iceberg join srcpart_date_n2 on (srcpart_iceberg.ds = srcpart_date_n2.ds) join srcpart_hour_n0 on (srcpart_iceberg.hr = srcpart_hour_n0.hr)
where srcpart_date_n2.`date` = '2008-04-08' and srcpart_iceberg.hr = 13;

-- union + subquery
EXPLAIN select count(*) from srcpart_iceberg where srcpart_iceberg.ds in (select max(srcpart_iceberg.ds) from srcpart_iceberg union all select min(srcpart_iceberg.ds) from srcpart_iceberg);
select count(*) from srcpart_iceberg where srcpart_iceberg.ds in (select max(srcpart_iceberg.ds) from srcpart_iceberg union all select min(srcpart_iceberg.ds) from srcpart_iceberg);
EXPLAIN select distinct(ds) from srcpart_iceberg where srcpart_iceberg.ds in (select max(srcpart_iceberg.ds) from srcpart_iceberg union all select min(srcpart_iceberg.ds) from srcpart_iceberg);
select distinct(ds) from srcpart_iceberg where srcpart_iceberg.ds in (select max(srcpart_iceberg.ds) from srcpart_iceberg union all select min(srcpart_iceberg.ds) from srcpart_iceberg);
EXPLAIN select ds from (select distinct(ds) as ds from srcpart_iceberg union all select distinct(ds) as ds from srcpart_iceberg) s where s.ds in (select max(srcpart_iceberg.ds) from srcpart_iceberg union all select min(srcpart_iceberg.ds) from srcpart_iceberg);
select ds from (select distinct(ds) as ds from srcpart_iceberg union all select distinct(ds) as ds from srcpart_iceberg) s where s.ds in (select max(srcpart_iceberg.ds) from srcpart_iceberg union all select min(srcpart_iceberg.ds) from srcpart_iceberg);

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask = true;
set hive.auto.convert.join.noconditionaltask.size = 10000000;

-- single column, single key
EXPLAIN select count(*) from srcpart_iceberg join srcpart_date_n2 on (srcpart_iceberg.ds = srcpart_date_n2.ds) where srcpart_date_n2.`date` = '2008-04-08';
select count(*) from srcpart_iceberg join srcpart_date_n2 on (srcpart_iceberg.ds = srcpart_date_n2.ds) where srcpart_date_n2.`date` = '2008-04-08';
select count(*) from srcpart where ds = '2008-04-08';

-- multiple sources, single key
EXPLAIN select count(*) from srcpart_iceberg join srcpart_date_n2 on (srcpart_iceberg.ds = srcpart_date_n2.ds) join srcpart_hour_n0 on (srcpart_iceberg.hr = srcpart_hour_n0.hr)
where srcpart_date_n2.`date` = '2008-04-08' and srcpart_hour_n0.hour = 11;
select count(*) from srcpart_iceberg join srcpart_date_n2 on (srcpart_iceberg.ds = srcpart_date_n2.ds) join srcpart_hour_n0 on (srcpart_iceberg.hr = srcpart_hour_n0.hr)
where srcpart_date_n2.`date` = '2008-04-08' and srcpart_hour_n0.hour = 11;
select count(*) from srcpart where hr = 11 and ds = '2008-04-08';

-- multiple columns single source
EXPLAIN select count(*) from srcpart_iceberg join srcpart_date_hour_n0 on (srcpart_iceberg.ds = srcpart_date_hour_n0.ds and srcpart_iceberg.hr = srcpart_date_hour_n0.hr) where srcpart_date_hour_n0.`date` = '2008-04-08' and srcpart_date_hour_n0.hour = 11;
select count(*) from srcpart_iceberg join srcpart_date_hour_n0 on (srcpart_iceberg.ds = srcpart_date_hour_n0.ds and srcpart_iceberg.hr = srcpart_date_hour_n0.hr) where srcpart_date_hour_n0.`date` = '2008-04-08' and srcpart_date_hour_n0.hour = 11;
select count(*) from srcpart where ds = '2008-04-08' and hr = 11;

-- empty set
EXPLAIN select count(*) from srcpart_iceberg join srcpart_date_n2 on (srcpart_iceberg.ds = srcpart_date_n2.ds) where srcpart_date_n2.`date` = 'I DONT EXIST';
select count(*) from srcpart_iceberg join srcpart_date_n2 on (srcpart_iceberg.ds = srcpart_date_n2.ds) where srcpart_date_n2.`date` = 'I DONT EXIST';

-- expressions
EXPLAIN select count(*) from srcpart_iceberg join srcpart_double_hour_n0 on (srcpart_iceberg.hr = cast(cast(srcpart_double_hour_n0.hr/2 as int) as string)) where srcpart_double_hour_n0.hour = 11;
select count(*) from srcpart_iceberg join srcpart_double_hour_n0 on (srcpart_iceberg.hr = cast(cast(srcpart_double_hour_n0.hr/2 as int) as string)) where srcpart_double_hour_n0.hour = 11;
select count(*) from srcpart where hr = 11;

-- with static pruning
EXPLAIN select count(*) from srcpart_iceberg join srcpart_date_n2 on (srcpart_iceberg.ds = srcpart_date_n2.ds) join srcpart_hour_n0 on (srcpart_iceberg.hr = srcpart_hour_n0.hr)
where srcpart_date_n2.`date` = '2008-04-08' and srcpart_hour_n0.hour = 11 and srcpart_iceberg.hr = 11;
select count(*) from srcpart_iceberg join srcpart_date_n2 on (srcpart_iceberg.ds = srcpart_date_n2.ds) join srcpart_hour_n0 on (srcpart_iceberg.hr = srcpart_hour_n0.hr)
where srcpart_date_n2.`date` = '2008-04-08' and srcpart_hour_n0.hour = 11 and srcpart_iceberg.hr = 11;
EXPLAIN select count(*) from srcpart_iceberg join srcpart_date_n2 on (srcpart_iceberg.ds = srcpart_date_n2.ds) join srcpart_hour_n0 on (srcpart_iceberg.hr = srcpart_hour_n0.hr)
where srcpart_date_n2.`date` = '2008-04-08' and srcpart_iceberg.hr = 13;

select count(*) from srcpart_iceberg join srcpart_date_n2 on (srcpart_iceberg.ds = srcpart_date_n2.ds) join srcpart_hour_n0 on (srcpart_iceberg.hr = srcpart_hour_n0.hr)
where srcpart_date_n2.`date` = '2008-04-08' and srcpart_iceberg.hr = 13;

-- union + subquery
EXPLAIN select distinct(ds) from srcpart_iceberg where srcpart_iceberg.ds in (select max(srcpart_iceberg.ds) from srcpart_iceberg union all select min(srcpart_iceberg.ds) from srcpart_iceberg);
select distinct(ds) from srcpart_iceberg where srcpart_iceberg.ds in (select max(srcpart_iceberg.ds) from srcpart_iceberg union all select min(srcpart_iceberg.ds) from srcpart_iceberg);


-- Two iceberg tables

create table srcpart_date_hour_n0_iceberg (ds string, `date` string, hr string, hour string)
STORED BY ICEBERG STORED AS ORC;
INSERT INTO srcpart_date_hour_n0_iceberg select ds as ds, ds as `date`, hr as hr, hr as hour from srcpart group by ds, hr;

set hive.vectorized.execution.enabled=false;
set hive.exec.max.dynamic.partitions=1000;

EXPLAIN select count(*) from srcpart_iceberg join srcpart_date_hour_n0_iceberg on (srcpart_iceberg.ds = srcpart_date_hour_n0_iceberg.ds and srcpart_iceberg.hr = srcpart_date_hour_n0_iceberg.hr) where srcpart_date_hour_n0_iceberg.hour = 11 and (srcpart_date_hour_n0_iceberg.`date` = '2008-04-08' or srcpart_date_hour_n0_iceberg.`date` = '2008-04-09');
select count(*) from srcpart_iceberg join srcpart_date_hour_n0_iceberg on (srcpart_iceberg.ds = srcpart_date_hour_n0_iceberg.ds and srcpart_iceberg.hr = srcpart_date_hour_n0_iceberg.hr) where srcpart_date_hour_n0_iceberg.hour = 11 and (srcpart_date_hour_n0_iceberg.`date` = '2008-04-08' or srcpart_date_hour_n0_iceberg.`date` = '2008-04-09');
select count(*) from srcpart where (ds = '2008-04-08' or ds = '2008-04-09') and hr = 11;

drop table srcpart_iceberg;
drop table srcpart_date_hour_n0_iceberg;
drop table srcpart_date_n2;
drop table srcpart_hour_n0;
drop table srcpart_date_hour_n0;
drop table srcpart_double_hour_n0;
