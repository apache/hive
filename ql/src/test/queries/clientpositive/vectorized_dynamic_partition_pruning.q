set hive.compute.query.using.stats=false;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;
set hive.tez.dynamic.partition.pruning=true;
set hive.optimize.metadataonly=false;
set hive.optimize.index.filter=true;
set hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;
set hive.tez.bigtable.minsize.semijoin.reduction=1;
set hive.tez.min.bloom.filter.entries=1;

select distinct ds from srcpart;
select distinct hr from srcpart;

EXPLAIN VECTORIZATION create table srcpart_date_n8 as select ds as ds, ds as `date` from srcpart group by ds;
create table srcpart_date_n8 stored as orc as select ds as ds, ds as `date` from srcpart group by ds;
create table srcpart_hour_n2 stored as orc as select hr as hr, hr as hour from srcpart group by hr;
create table srcpart_date_hour_n2 stored as orc as select ds as ds, ds as `date`, hr as hr, hr as hour from srcpart group by ds, hr;
create table srcpart_double_hour_n2 stored as orc as select (hr*2) as hr, hr as hour from srcpart group by hr;

-- single column, single key
EXPLAIN VECTORIZATION select count(*) from srcpart join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) where srcpart_date_n8.`date` = '2008-04-08';
select count(*) from srcpart join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) where srcpart_date_n8.`date` = '2008-04-08';
set hive.tez.dynamic.partition.pruning=false;
EXPLAIN VECTORIZATION select count(*) from srcpart join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) where srcpart_date_n8.`date` = '2008-04-08';
select count(*) from srcpart join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) where srcpart_date_n8.`date` = '2008-04-08';
set hive.tez.dynamic.partition.pruning=true;
select count(*) from srcpart where ds = '2008-04-08';

-- multiple sources, single key
EXPLAIN VECTORIZATION select count(*) from srcpart join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) join srcpart_hour_n2 on (srcpart.hr = srcpart_hour_n2.hr) 
where srcpart_date_n8.`date` = '2008-04-08' and srcpart_hour_n2.hour = 11;
select count(*) from srcpart join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) join srcpart_hour_n2 on (srcpart.hr = srcpart_hour_n2.hr) 
where srcpart_date_n8.`date` = '2008-04-08' and srcpart_hour_n2.hour = 11;
set hive.tez.dynamic.partition.pruning=false;
EXPLAIN VECTORIZATION select count(*) from srcpart join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) join srcpart_hour_n2 on (srcpart.hr = srcpart_hour_n2.hr) 
where srcpart_date_n8.`date` = '2008-04-08' and srcpart_hour_n2.hour = 11;
select count(*) from srcpart join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) join srcpart_hour_n2 on (srcpart.hr = srcpart_hour_n2.hr) 
where srcpart_date_n8.`date` = '2008-04-08' and srcpart_hour_n2.hour = 11;
set hive.tez.dynamic.partition.pruning=true;
select count(*) from srcpart where hr = 11 and ds = '2008-04-08';

-- multiple columns single source
EXPLAIN VECTORIZATION select count(*) from srcpart join srcpart_date_hour_n2 on (srcpart.ds = srcpart_date_hour_n2.ds and srcpart.hr = srcpart_date_hour_n2.hr) where srcpart_date_hour_n2.`date` = '2008-04-08' and srcpart_date_hour_n2.hour = 11;
select count(*) from srcpart join srcpart_date_hour_n2 on (srcpart.ds = srcpart_date_hour_n2.ds and srcpart.hr = srcpart_date_hour_n2.hr) where srcpart_date_hour_n2.`date` = '2008-04-08' and srcpart_date_hour_n2.hour = 11;
set hive.tez.dynamic.partition.pruning=false;
EXPLAIN VECTORIZATION select count(*) from srcpart join srcpart_date_hour_n2 on (srcpart.ds = srcpart_date_hour_n2.ds and srcpart.hr = srcpart_date_hour_n2.hr) where srcpart_date_hour_n2.`date` = '2008-04-08' and srcpart_date_hour_n2.hour = 11;
select count(*) from srcpart join srcpart_date_hour_n2 on (srcpart.ds = srcpart_date_hour_n2.ds and srcpart.hr = srcpart_date_hour_n2.hr) where srcpart_date_hour_n2.`date` = '2008-04-08' and srcpart_date_hour_n2.hour = 11;
set hive.tez.dynamic.partition.pruning=true;
select count(*) from srcpart where ds = '2008-04-08' and hr = 11;

-- empty set
EXPLAIN VECTORIZATION select count(*) from srcpart join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) where srcpart_date_n8.`date` = 'I DONT EXIST';
select count(*) from srcpart join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) where srcpart_date_n8.`date` = 'I DONT EXIST';
set hive.tez.dynamic.partition.pruning=false;
EXPLAIN VECTORIZATION select count(*) from srcpart join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) where srcpart_date_n8.`date` = 'I DONT EXIST';
select count(*) from srcpart join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) where srcpart_date_n8.`date` = 'I DONT EXIST';
set hive.tez.dynamic.partition.pruning=true;
select count(*) from srcpart where ds = 'I DONT EXIST';

-- expressions
EXPLAIN VECTORIZATION select count(*) from srcpart join srcpart_double_hour_n2 on (srcpart.hr = cast(srcpart_double_hour_n2.hr/2 as int)) where srcpart_double_hour_n2.hour = 11;
select count(*) from srcpart join srcpart_double_hour_n2 on (srcpart.hr = cast(srcpart_double_hour_n2.hr/2 as int)) where srcpart_double_hour_n2.hour = 11;
EXPLAIN VECTORIZATION select count(*) from srcpart join srcpart_double_hour_n2 on (srcpart.hr*2 = srcpart_double_hour_n2.hr) where srcpart_double_hour_n2.hour = 11;
select count(*) from srcpart join srcpart_double_hour_n2 on (srcpart.hr*2 = srcpart_double_hour_n2.hr) where srcpart_double_hour_n2.hour = 11;
set hive.tez.dynamic.partition.pruning=false;
EXPLAIN VECTORIZATION select count(*) from srcpart join srcpart_double_hour_n2 on (srcpart.hr = cast(srcpart_double_hour_n2.hr/2 as int)) where srcpart_double_hour_n2.hour = 11;
select count(*) from srcpart join srcpart_double_hour_n2 on (srcpart.hr = cast(srcpart_double_hour_n2.hr/2 as int)) where srcpart_double_hour_n2.hour = 11;
EXPLAIN VECTORIZATION select count(*) from srcpart join srcpart_double_hour_n2 on (srcpart.hr*2 = srcpart_double_hour_n2.hr) where srcpart_double_hour_n2.hour = 11;
select count(*) from srcpart join srcpart_double_hour_n2 on (srcpart.hr*2 = srcpart_double_hour_n2.hr) where srcpart_double_hour_n2.hour = 11;
set hive.tez.dynamic.partition.pruning=true;
select count(*) from srcpart where hr = 11;
EXPLAIN VECTORIZATION select count(*) from srcpart join srcpart_double_hour_n2 on (cast(srcpart.hr*2 as string) = cast(srcpart_double_hour_n2.hr as string)) where srcpart_double_hour_n2.hour = 11;
select count(*) from srcpart join srcpart_double_hour_n2 on (cast(srcpart.hr*2 as string) = cast(srcpart_double_hour_n2.hr as string)) where srcpart_double_hour_n2.hour = 11;
set hive.tez.dynamic.partition.pruning=true;
select count(*) from srcpart where cast(hr as string) = 11;


-- parent is reduce tasks
EXPLAIN VECTORIZATION select count(*) from srcpart join (select ds as ds, ds as `date` from srcpart group by ds) s on (srcpart.ds = s.ds) where s.`date` = '2008-04-08';
select count(*) from srcpart join (select ds as ds, ds as `date` from srcpart group by ds) s on (srcpart.ds = s.ds) where s.`date` = '2008-04-08';
select count(*) from srcpart where ds = '2008-04-08';

-- non-equi join
EXPLAIN VECTORIZATION select count(*) from srcpart, srcpart_date_hour_n2 where (srcpart_date_hour_n2.`date` = '2008-04-08' and srcpart_date_hour_n2.hour = 11) and (srcpart.ds = srcpart_date_hour_n2.ds or srcpart.hr = srcpart_date_hour_n2.hr);
select count(*) from srcpart, srcpart_date_hour_n2 where (srcpart_date_hour_n2.`date` = '2008-04-08' and srcpart_date_hour_n2.hour = 11) and (srcpart.ds = srcpart_date_hour_n2.ds or srcpart.hr = srcpart_date_hour_n2.hr);

-- old style join syntax
EXPLAIN VECTORIZATION select count(*) from srcpart, srcpart_date_hour_n2 where srcpart_date_hour_n2.`date` = '2008-04-08' and srcpart_date_hour_n2.hour = 11 and srcpart.ds = srcpart_date_hour_n2.ds and srcpart.hr = srcpart_date_hour_n2.hr;
select count(*) from srcpart, srcpart_date_hour_n2 where srcpart_date_hour_n2.`date` = '2008-04-08' and srcpart_date_hour_n2.hour = 11 and srcpart.ds = srcpart_date_hour_n2.ds and srcpart.hr = srcpart_date_hour_n2.hr;

-- left join
EXPLAIN VECTORIZATION select count(*) from srcpart left join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) where srcpart_date_n8.`date` = '2008-04-08';
EXPLAIN VECTORIZATION select count(*) from srcpart_date_n8 left join srcpart on (srcpart.ds = srcpart_date_n8.ds) where srcpart_date_n8.`date` = '2008-04-08';

-- full outer
EXPLAIN VECTORIZATION select count(*) from srcpart full outer join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) where srcpart_date_n8.`date` = '2008-04-08';

-- with static pruning
EXPLAIN VECTORIZATION select count(*) from srcpart join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) join srcpart_hour_n2 on (srcpart.hr = srcpart_hour_n2.hr) 
where srcpart_date_n8.`date` = '2008-04-08' and srcpart_hour_n2.hour = 11 and srcpart.hr = 11;
select count(*) from srcpart join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) join srcpart_hour_n2 on (srcpart.hr = srcpart_hour_n2.hr) 
where srcpart_date_n8.`date` = '2008-04-08' and srcpart_hour_n2.hour = 11 and srcpart.hr = 11;
EXPLAIN VECTORIZATION select count(*) from srcpart join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) join srcpart_hour_n2 on (srcpart.hr = srcpart_hour_n2.hr) 
where srcpart_date_n8.`date` = '2008-04-08' and srcpart.hr = 13;
select count(*) from srcpart join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) join srcpart_hour_n2 on (srcpart.hr = srcpart_hour_n2.hr) 
where srcpart_date_n8.`date` = '2008-04-08' and srcpart.hr = 13;

-- union + subquery
EXPLAIN VECTORIZATION select count(*) from srcpart where srcpart.ds in (select max(srcpart.ds) from srcpart union all select min(srcpart.ds) from srcpart);
select count(*) from srcpart where srcpart.ds in (select max(srcpart.ds) from srcpart union all select min(srcpart.ds) from srcpart);
EXPLAIN VECTORIZATION select distinct(ds) from srcpart where srcpart.ds in (select max(srcpart.ds) from srcpart union all select min(srcpart.ds) from srcpart);
select distinct(ds) from srcpart where srcpart.ds in (select max(srcpart.ds) from srcpart union all select min(srcpart.ds) from srcpart);
EXPLAIN VECTORIZATION select ds from (select distinct(ds) as ds from srcpart union all select distinct(ds) as ds from srcpart) s where s.ds in (select max(srcpart.ds) from srcpart union all select min(srcpart.ds) from srcpart);
select ds from (select distinct(ds) as ds from srcpart union all select distinct(ds) as ds from srcpart) s where s.ds in (select max(srcpart.ds) from srcpart union all select min(srcpart.ds) from srcpart);

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask = true;
set hive.auto.convert.join.noconditionaltask.size = 10000000;

-- single column, single key
EXPLAIN VECTORIZATION select count(*) from srcpart join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) where srcpart_date_n8.`date` = '2008-04-08';
select count(*) from srcpart join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) where srcpart_date_n8.`date` = '2008-04-08';
select count(*) from srcpart where ds = '2008-04-08';

-- multiple sources, single key
EXPLAIN VECTORIZATION select count(*) from srcpart join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) join srcpart_hour_n2 on (srcpart.hr = srcpart_hour_n2.hr) 
where srcpart_date_n8.`date` = '2008-04-08' and srcpart_hour_n2.hour = 11;
select count(*) from srcpart join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) join srcpart_hour_n2 on (srcpart.hr = srcpart_hour_n2.hr) 
where srcpart_date_n8.`date` = '2008-04-08' and srcpart_hour_n2.hour = 11;
select count(*) from srcpart where hr = 11 and ds = '2008-04-08';

-- multiple columns single source
EXPLAIN VECTORIZATION select count(*) from srcpart join srcpart_date_hour_n2 on (srcpart.ds = srcpart_date_hour_n2.ds and srcpart.hr = srcpart_date_hour_n2.hr) where srcpart_date_hour_n2.`date` = '2008-04-08' and srcpart_date_hour_n2.hour = 11;
select count(*) from srcpart join srcpart_date_hour_n2 on (srcpart.ds = srcpart_date_hour_n2.ds and srcpart.hr = srcpart_date_hour_n2.hr) where srcpart_date_hour_n2.`date` = '2008-04-08' and srcpart_date_hour_n2.hour = 11;
select count(*) from srcpart where ds = '2008-04-08' and hr = 11;

-- empty set
EXPLAIN VECTORIZATION select count(*) from srcpart join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) where srcpart_date_n8.`date` = 'I DONT EXIST';
select count(*) from srcpart join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) where srcpart_date_n8.`date` = 'I DONT EXIST';

-- expressions
EXPLAIN VECTORIZATION select count(*) from srcpart join srcpart_double_hour_n2 on (srcpart.hr = cast(srcpart_double_hour_n2.hr/2 as int)) where srcpart_double_hour_n2.hour = 11;
select count(*) from srcpart join srcpart_double_hour_n2 on (srcpart.hr = cast(srcpart_double_hour_n2.hr/2 as int)) where srcpart_double_hour_n2.hour = 11;
EXPLAIN VECTORIZATION select count(*) from srcpart join srcpart_double_hour_n2 on (srcpart.hr*2 = srcpart_double_hour_n2.hr) where srcpart_double_hour_n2.hour = 11;
select count(*) from srcpart join srcpart_double_hour_n2 on (srcpart.hr*2 = srcpart_double_hour_n2.hr) where srcpart_double_hour_n2.hour = 11;
select count(*) from srcpart where hr = 11;

set hive.stats.fetch.column.stats=false;
-- parent is reduce tasks

EXPLAIN VECTORIZATION select count(*) from srcpart join (select ds as ds, ds as `date` from srcpart group by ds) s on (srcpart.ds = s.ds) where s.`date` = '2008-04-08';
select count(*) from srcpart join (select ds as ds, ds as `date` from srcpart group by ds) s on (srcpart.ds = s.ds) where s.`date` = '2008-04-08';
select count(*) from srcpart where ds = '2008-04-08';
set hive.stats.fetch.column.stats=true;

-- left join
EXPLAIN VECTORIZATION select count(*) from srcpart left join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) where srcpart_date_n8.`date` = '2008-04-08';
EXPLAIN VECTORIZATION select count(*) from srcpart_date_n8 left join srcpart on (srcpart.ds = srcpart_date_n8.ds) where srcpart_date_n8.`date` = '2008-04-08';

-- full outer
EXPLAIN VECTORIZATION select count(*) from srcpart full outer join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) where srcpart_date_n8.`date` = '2008-04-08';

-- with static pruning
EXPLAIN VECTORIZATION select count(*) from srcpart join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) join srcpart_hour_n2 on (srcpart.hr = srcpart_hour_n2.hr) 
where srcpart_date_n8.`date` = '2008-04-08' and srcpart_hour_n2.hour = 11 and srcpart.hr = 11;
select count(*) from srcpart join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) join srcpart_hour_n2 on (srcpart.hr = srcpart_hour_n2.hr) 
where srcpart_date_n8.`date` = '2008-04-08' and srcpart_hour_n2.hour = 11 and srcpart.hr = 11;
EXPLAIN VECTORIZATION select count(*) from srcpart join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) join srcpart_hour_n2 on (srcpart.hr = srcpart_hour_n2.hr) 
where srcpart_date_n8.`date` = '2008-04-08' and srcpart.hr = 13;
select count(*) from srcpart join srcpart_date_n8 on (srcpart.ds = srcpart_date_n8.ds) join srcpart_hour_n2 on (srcpart.hr = srcpart_hour_n2.hr) 
where srcpart_date_n8.`date` = '2008-04-08' and srcpart.hr = 13;

-- union + subquery
EXPLAIN VECTORIZATION select distinct(ds) from srcpart where srcpart.ds in (select max(srcpart.ds) from srcpart union all select min(srcpart.ds) from srcpart);
select distinct(ds) from srcpart where srcpart.ds in (select max(srcpart.ds) from srcpart union all select min(srcpart.ds) from srcpart);


-- different file format
create table srcpart_orc_n0 (key int, value string) partitioned by (ds string, hr int) stored as orc;


set hive.exec.dynamic.partition.mode=nonstrict;
set hive.vectorized.execution.enabled=false;
set hive.exec.max.dynamic.partitions=1000;

insert into table srcpart_orc_n0 partition (ds, hr) select key, value, ds, hr from srcpart;
EXPLAIN VECTORIZATION select count(*) from srcpart_orc_n0 join srcpart_date_hour_n2 on (srcpart_orc_n0.ds = srcpart_date_hour_n2.ds and srcpart_orc_n0.hr = srcpart_date_hour_n2.hr) where srcpart_date_hour_n2.hour = 11 and (srcpart_date_hour_n2.`date` = '2008-04-08' or srcpart_date_hour_n2.`date` = '2008-04-09');
select count(*) from srcpart_orc_n0 join srcpart_date_hour_n2 on (srcpart_orc_n0.ds = srcpart_date_hour_n2.ds and srcpart_orc_n0.hr = srcpart_date_hour_n2.hr) where srcpart_date_hour_n2.hour = 11 and (srcpart_date_hour_n2.`date` = '2008-04-08' or srcpart_date_hour_n2.`date` = '2008-04-09');
select count(*) from srcpart where (ds = '2008-04-08' or ds = '2008-04-09') and hr = 11;

drop table srcpart_orc_n0;
drop table srcpart_date_n8;
drop table srcpart_hour_n2;
drop table srcpart_date_hour_n2;
drop table srcpart_double_hour_n2;
