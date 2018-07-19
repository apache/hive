--! qt:dataset:srcpart
SET hive.vectorized.execution.enabled=false;
set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;
set hive.spark.dynamic.partition.pruning=true;
set hive.optimize.metadataonly=false;
set hive.optimize.index.filter=true;
set hive.strict.checks.cartesian.product=false;

-- SORT_QUERY_RESULTS

select distinct ds from srcpart;
select distinct hr from srcpart;

EXPLAIN create table srcpart_date_n4 as select ds as ds, ds as `date` from srcpart group by ds;
create table srcpart_date_n4 as select ds as ds, ds as `date` from srcpart group by ds;
create table srcpart_hour_n1 as select hr as hr, hr as hour from srcpart group by hr;
create table srcpart_date_hour_n1 as select ds as ds, ds as `date`, hr as hr, hr as hour from srcpart group by ds, hr;
create table srcpart_double_hour_n1 as select (hr*2) as hr, hr as hour from srcpart group by hr;

-- single column, single key -- join a partitioned table to a non-partitioned table, static filter on the non-partitioned table
EXPLAIN select count(*) from srcpart join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) where srcpart_date_n4.`date` = '2008-04-08';
select count(*) from srcpart join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) where srcpart_date_n4.`date` = '2008-04-08';
set hive.spark.dynamic.partition.pruning=false;
EXPLAIN select count(*) from srcpart join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) where srcpart_date_n4.`date` = '2008-04-08';
select count(*) from srcpart join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) where srcpart_date_n4.`date` = '2008-04-08';
set hive.spark.dynamic.partition.pruning=true;
select count(*) from srcpart where ds = '2008-04-08';

-- single column, single key, udf with typechange
EXPLAIN select count(*) from srcpart join srcpart_date_n4 on (day(srcpart.ds) = day(srcpart_date_n4.ds)) where srcpart_date_n4.`date` = '2008-04-08';
select count(*) from srcpart join srcpart_date_n4 on (day(srcpart.ds) = day(srcpart_date_n4.ds)) where srcpart_date_n4.`date` = '2008-04-08';
set hive.spark.dynamic.partition.pruning=false;
EXPLAIN select count(*) from srcpart join srcpart_date_n4 on (day(srcpart.ds) = day(srcpart_date_n4.ds)) where srcpart_date_n4.`date` = '2008-04-08';
select count(*) from srcpart join srcpart_date_n4 on (day(srcpart.ds) = day(srcpart_date_n4.ds)) where srcpart_date_n4.`date` = '2008-04-08';
set hive.spark.dynamic.partition.pruning=true;

-- multiple udfs and casts
EXPLAIN select count(*) from srcpart join srcpart_date_n4 on abs(negative(cast(concat(cast(day(srcpart.ds) as string), "0") as bigint)) + 10) = abs(negative(cast(concat(cast(day(srcpart_date_n4.ds) as string), "0") as bigint)) + 10) where srcpart_date_n4.`date` = '2008-04-08';
select count(*) from srcpart join srcpart_date_n4 on abs(negative(cast(concat(cast(day(srcpart.ds) as string), "0") as bigint)) + 10) = abs(negative(cast(concat(cast(day(srcpart_date_n4.ds) as string), "0") as bigint)) + 10) where srcpart_date_n4.`date` = '2008-04-08';

-- implicit type conversion between join columns
EXPLAIN select count(*) from srcpart join srcpart_date_n4 on cast(day(srcpart.ds) as smallint) = cast(day(srcpart_date_n4.ds) as decimal) where srcpart_date_n4.`date` = '2008-04-08';
select count(*) from srcpart join srcpart_date_n4 on cast(day(srcpart.ds) as smallint) = cast(day(srcpart_date_n4.ds) as decimal) where srcpart_date_n4.`date` = '2008-04-08';

-- multiple sources, single key -- filter partitioned table on both partitioned columns via join with non-partitioned table
EXPLAIN select count(*) from srcpart join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) join srcpart_hour_n1 on (srcpart.hr = srcpart_hour_n1.hr)
where srcpart_date_n4.`date` = '2008-04-08' and srcpart_hour_n1.hour = 11;
select count(*) from srcpart join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) join srcpart_hour_n1 on (srcpart.hr = srcpart_hour_n1.hr) 
where srcpart_date_n4.`date` = '2008-04-08' and srcpart_hour_n1.hour = 11;
set hive.spark.dynamic.partition.pruning=false;
EXPLAIN select count(*) from srcpart join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) join srcpart_hour_n1 on (srcpart.hr = srcpart_hour_n1.hr) 
where srcpart_date_n4.`date` = '2008-04-08' and srcpart_hour_n1.hour = 11;
select count(*) from srcpart join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) join srcpart_hour_n1 on (srcpart.hr = srcpart_hour_n1.hr) 
where srcpart_date_n4.`date` = '2008-04-08' and srcpart_hour_n1.hour = 11;
set hive.spark.dynamic.partition.pruning=true;
select count(*) from srcpart where hr = 11 and ds = '2008-04-08';

-- multiple columns single source -- filter partitioned table on both partitioned columns via join with non-partitioned table, filter non-partitioned table
EXPLAIN select count(*) from srcpart join srcpart_date_hour_n1 on (srcpart.ds = srcpart_date_hour_n1.ds and srcpart.hr = srcpart_date_hour_n1.hr) where srcpart_date_hour_n1.`date` = '2008-04-08' and srcpart_date_hour_n1.hour = 11;
select count(*) from srcpart join srcpart_date_hour_n1 on (srcpart.ds = srcpart_date_hour_n1.ds and srcpart.hr = srcpart_date_hour_n1.hr) where srcpart_date_hour_n1.`date` = '2008-04-08' and srcpart_date_hour_n1.hour = 11;
set hive.spark.dynamic.partition.pruning=false;
EXPLAIN select count(*) from srcpart join srcpart_date_hour_n1 on (srcpart.ds = srcpart_date_hour_n1.ds and srcpart.hr = srcpart_date_hour_n1.hr) where srcpart_date_hour_n1.`date` = '2008-04-08' and srcpart_date_hour_n1.hour = 11;
select count(*) from srcpart join srcpart_date_hour_n1 on (srcpart.ds = srcpart_date_hour_n1.ds and srcpart.hr = srcpart_date_hour_n1.hr) where srcpart_date_hour_n1.`date` = '2008-04-08' and srcpart_date_hour_n1.hour = 11;
set hive.spark.dynamic.partition.pruning=true;
select count(*) from srcpart where ds = '2008-04-08' and hr = 11;

-- empty set -- join a partitioned table to a non-partitioned table, static filter on the non-partitioned table that doesn't filter out anything
EXPLAIN select count(*) from srcpart join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) where srcpart_date_n4.`date` = 'I DONT EXIST';
select count(*) from srcpart join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) where srcpart_date_n4.`date` = 'I DONT EXIST';
set hive.spark.dynamic.partition.pruning=false;
EXPLAIN select count(*) from srcpart join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) where srcpart_date_n4.`date` = 'I DONT EXIST';
select count(*) from srcpart join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) where srcpart_date_n4.`date` = 'I DONT EXIST';
set hive.spark.dynamic.partition.pruning=true;
select count(*) from srcpart where ds = 'I DONT EXIST';

-- expressions -- triggers DPP with various expressions - e.g. cast, multiplication, division
EXPLAIN select count(*) from srcpart join srcpart_double_hour_n1 on (srcpart.hr = cast(srcpart_double_hour_n1.hr/2 as int)) where srcpart_double_hour_n1.hour = 11;
select count(*) from srcpart join srcpart_double_hour_n1 on (srcpart.hr = cast(srcpart_double_hour_n1.hr/2 as int)) where srcpart_double_hour_n1.hour = 11;
EXPLAIN select count(*) from srcpart join srcpart_double_hour_n1 on (srcpart.hr*2 = srcpart_double_hour_n1.hr) where srcpart_double_hour_n1.hour = 11;
select count(*) from srcpart join srcpart_double_hour_n1 on (srcpart.hr*2 = srcpart_double_hour_n1.hr) where srcpart_double_hour_n1.hour = 11;
set hive.spark.dynamic.partition.pruning=false;
EXPLAIN select count(*) from srcpart join srcpart_double_hour_n1 on (srcpart.hr = cast(srcpart_double_hour_n1.hr/2 as int)) where srcpart_double_hour_n1.hour = 11;
select count(*) from srcpart join srcpart_double_hour_n1 on (srcpart.hr = cast(srcpart_double_hour_n1.hr/2 as int)) where srcpart_double_hour_n1.hour = 11;
EXPLAIN select count(*) from srcpart join srcpart_double_hour_n1 on (srcpart.hr*2 = srcpart_double_hour_n1.hr) where srcpart_double_hour_n1.hour = 11;
select count(*) from srcpart join srcpart_double_hour_n1 on (srcpart.hr*2 = srcpart_double_hour_n1.hr) where srcpart_double_hour_n1.hour = 11;
set hive.spark.dynamic.partition.pruning=true;
select count(*) from srcpart where hr = 11;
EXPLAIN select count(*) from srcpart join srcpart_double_hour_n1 on (cast(srcpart.hr*2 as string) = cast(srcpart_double_hour_n1.hr as string)) where srcpart_double_hour_n1.hour = 11;
select count(*) from srcpart join srcpart_double_hour_n1 on (cast(srcpart.hr*2 as string) = cast(srcpart_double_hour_n1.hr as string)) where srcpart_double_hour_n1.hour = 11;
set hive.spark.dynamic.partition.pruning=true;
select count(*) from srcpart where cast(hr as string) = 11;

-- parent is reduce tasks -- join a partitioned table to a non-partitioned table, where the non-partitioned table is a subquery, static filter on the subquery
EXPLAIN select count(*) from srcpart join (select ds as ds, ds as `date` from srcpart group by ds) s on (srcpart.ds = s.ds) where s.`date` = '2008-04-08';
select count(*) from srcpart join (select ds as ds, ds as `date` from srcpart group by ds) s on (srcpart.ds = s.ds) where s.`date` = '2008-04-08';
select count(*) from srcpart where ds = '2008-04-08';

-- non-equi join
EXPLAIN select count(*) from srcpart, srcpart_date_hour_n1 where (srcpart_date_hour_n1.`date` = '2008-04-08' and srcpart_date_hour_n1.hour = 11) and (srcpart.ds = srcpart_date_hour_n1.ds or srcpart.hr = srcpart_date_hour_n1.hr);
select count(*) from srcpart, srcpart_date_hour_n1 where (srcpart_date_hour_n1.`date` = '2008-04-08' and srcpart_date_hour_n1.hour = 11) and (srcpart.ds = srcpart_date_hour_n1.ds or srcpart.hr = srcpart_date_hour_n1.hr);

-- old style join syntax
EXPLAIN select count(*) from srcpart, srcpart_date_hour_n1 where srcpart_date_hour_n1.`date` = '2008-04-08' and srcpart_date_hour_n1.hour = 11 and srcpart.ds = srcpart_date_hour_n1.ds and srcpart.hr = srcpart_date_hour_n1.hr;
select count(*) from srcpart, srcpart_date_hour_n1 where srcpart_date_hour_n1.`date` = '2008-04-08' and srcpart_date_hour_n1.hour = 11 and srcpart.ds = srcpart_date_hour_n1.ds and srcpart.hr = srcpart_date_hour_n1.hr;

-- left join
EXPLAIN select count(*) from srcpart left join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) where srcpart_date_n4.`date` = '2008-04-08';
EXPLAIN select count(*) from srcpart_date_n4 left join srcpart on (srcpart.ds = srcpart_date_n4.ds) where srcpart_date_n4.`date` = '2008-04-08';

-- full outer
EXPLAIN select count(*) from srcpart full outer join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) where srcpart_date_n4.`date` = '2008-04-08';

-- with static pruning
EXPLAIN select count(*) from srcpart join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) join srcpart_hour_n1 on (srcpart.hr = srcpart_hour_n1.hr) 
where srcpart_date_n4.`date` = '2008-04-08' and srcpart_hour_n1.hour = 11 and srcpart.hr = 11;
select count(*) from srcpart join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) join srcpart_hour_n1 on (srcpart.hr = srcpart_hour_n1.hr) 
where srcpart_date_n4.`date` = '2008-04-08' and srcpart_hour_n1.hour = 11 and srcpart.hr = 11;
EXPLAIN select count(*) from srcpart join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) join srcpart_hour_n1 on (srcpart.hr = srcpart_hour_n1.hr) 
where srcpart_date_n4.`date` = '2008-04-08' and srcpart.hr = 13;
select count(*) from srcpart join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) join srcpart_hour_n1 on (srcpart.hr = srcpart_hour_n1.hr) 
where srcpart_date_n4.`date` = '2008-04-08' and srcpart.hr = 13;

-- union + subquery
EXPLAIN select count(*) from srcpart where srcpart.ds in (select max(srcpart.ds) from srcpart union all select min(srcpart.ds) from srcpart);
select count(*) from srcpart where srcpart.ds in (select max(srcpart.ds) from srcpart union all select min(srcpart.ds) from srcpart);
EXPLAIN select distinct(ds) from srcpart where srcpart.ds in (select max(srcpart.ds) from srcpart union all select min(srcpart.ds) from srcpart);
select distinct(ds) from srcpart where srcpart.ds in (select max(srcpart.ds) from srcpart union all select min(srcpart.ds) from srcpart);
EXPLAIN select ds from (select distinct(ds) as ds from srcpart union all select distinct(ds) as ds from srcpart) s where s.ds in (select max(srcpart.ds) from srcpart union all select min(srcpart.ds) from srcpart);
select ds from (select distinct(ds) as ds from srcpart union all select distinct(ds) as ds from srcpart) s where s.ds in (select max(srcpart.ds) from srcpart union all select min(srcpart.ds) from srcpart);

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask = true;
set hive.auto.convert.join.noconditionaltask.size = 10000000;

-- single column, single key
EXPLAIN select count(*) from srcpart join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) where srcpart_date_n4.`date` = '2008-04-08';
select count(*) from srcpart join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) where srcpart_date_n4.`date` = '2008-04-08';
select count(*) from srcpart where ds = '2008-04-08';

-- single column, single key, udf with typechange
EXPLAIN select count(*) from srcpart join srcpart_date_n4 on (day(srcpart.ds) = day(srcpart_date_n4.ds)) where srcpart_date_n4.`date` = '2008-04-08';
select count(*) from srcpart join srcpart_date_n4 on (day(srcpart.ds) = day(srcpart_date_n4.ds)) where srcpart_date_n4.`date` = '2008-04-08';

-- multiple sources, single key
EXPLAIN select count(*) from srcpart join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) join srcpart_hour_n1 on (srcpart.hr = srcpart_hour_n1.hr) 
where srcpart_date_n4.`date` = '2008-04-08' and srcpart_hour_n1.hour = 11;
select count(*) from srcpart join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) join srcpart_hour_n1 on (srcpart.hr = srcpart_hour_n1.hr) 
where srcpart_date_n4.`date` = '2008-04-08' and srcpart_hour_n1.hour = 11;
select count(*) from srcpart where hr = 11 and ds = '2008-04-08';

-- multiple columns single source
EXPLAIN select count(*) from srcpart join srcpart_date_hour_n1 on (srcpart.ds = srcpart_date_hour_n1.ds and srcpart.hr = srcpart_date_hour_n1.hr) where srcpart_date_hour_n1.`date` = '2008-04-08' and srcpart_date_hour_n1.hour = 11;
select count(*) from srcpart join srcpart_date_hour_n1 on (srcpart.ds = srcpart_date_hour_n1.ds and srcpart.hr = srcpart_date_hour_n1.hr) where srcpart_date_hour_n1.`date` = '2008-04-08' and srcpart_date_hour_n1.hour = 11;
select count(*) from srcpart where ds = '2008-04-08' and hr = 11;

-- empty set
EXPLAIN select count(*) from srcpart join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) where srcpart_date_n4.`date` = 'I DONT EXIST';
select count(*) from srcpart join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) where srcpart_date_n4.`date` = 'I DONT EXIST';

-- expressions
EXPLAIN select count(*) from srcpart join srcpart_double_hour_n1 on (srcpart.hr = cast(srcpart_double_hour_n1.hr/2 as int)) where srcpart_double_hour_n1.hour = 11;
select count(*) from srcpart join srcpart_double_hour_n1 on (srcpart.hr = cast(srcpart_double_hour_n1.hr/2 as int)) where srcpart_double_hour_n1.hour = 11;
EXPLAIN select count(*) from srcpart join srcpart_double_hour_n1 on (srcpart.hr*2 = srcpart_double_hour_n1.hr) where srcpart_double_hour_n1.hour = 11;
select count(*) from srcpart join srcpart_double_hour_n1 on (srcpart.hr*2 = srcpart_double_hour_n1.hr) where srcpart_double_hour_n1.hour = 11;
select count(*) from srcpart where hr = 11;

-- parent is reduce tasks
EXPLAIN select count(*) from srcpart join (select ds as ds, ds as `date` from srcpart group by ds) s on (srcpart.ds = s.ds) where s.`date` = '2008-04-08';
select count(*) from srcpart join (select ds as ds, ds as `date` from srcpart group by ds) s on (srcpart.ds = s.ds) where s.`date` = '2008-04-08';
select count(*) from srcpart where ds = '2008-04-08';

-- left join
EXPLAIN select count(*) from srcpart left join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) where srcpart_date_n4.`date` = '2008-04-08';
EXPLAIN select count(*) from srcpart_date_n4 left join srcpart on (srcpart.ds = srcpart_date_n4.ds) where srcpart_date_n4.`date` = '2008-04-08';

-- full outer
EXPLAIN select count(*) from srcpart full outer join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) where srcpart_date_n4.`date` = '2008-04-08';

-- with static pruning
EXPLAIN select count(*) from srcpart join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) join srcpart_hour_n1 on (srcpart.hr = srcpart_hour_n1.hr) 
where srcpart_date_n4.`date` = '2008-04-08' and srcpart_hour_n1.hour = 11 and srcpart.hr = 11;
select count(*) from srcpart join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) join srcpart_hour_n1 on (srcpart.hr = srcpart_hour_n1.hr) 
where srcpart_date_n4.`date` = '2008-04-08' and srcpart_hour_n1.hour = 11 and srcpart.hr = 11;
EXPLAIN select count(*) from srcpart join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) join srcpart_hour_n1 on (srcpart.hr = srcpart_hour_n1.hr) 
where srcpart_date_n4.`date` = '2008-04-08' and srcpart.hr = 13;
select count(*) from srcpart join srcpart_date_n4 on (srcpart.ds = srcpart_date_n4.ds) join srcpart_hour_n1 on (srcpart.hr = srcpart_hour_n1.hr)
where srcpart_date_n4.`date` = '2008-04-08' and srcpart.hr = 13;

-- union + subquery
EXPLAIN select distinct(ds) from srcpart where srcpart.ds in (select max(srcpart.ds) from srcpart union all select min(srcpart.ds) from srcpart);
select distinct(ds) from srcpart where srcpart.ds in (select max(srcpart.ds) from srcpart union all select min(srcpart.ds) from srcpart);

-- different file format
create table srcpart_parquet (key int, value string) partitioned by (ds string, hr int) stored as parquet;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.vectorized.execution.enabled=false;
set hive.exec.max.dynamic.partitions=1000;

insert into table srcpart_parquet partition (ds, hr) select key, value, ds, hr from srcpart;
EXPLAIN select count(*) from srcpart_parquet join srcpart_date_hour_n1 on (srcpart_parquet.ds = srcpart_date_hour_n1.ds and srcpart_parquet.hr = srcpart_date_hour_n1.hr) where srcpart_date_hour_n1.hour = 11 and (srcpart_date_hour_n1.`date` = '2008-04-08' or srcpart_date_hour_n1.`date` = '2008-04-09');
select count(*) from srcpart_parquet join srcpart_date_hour_n1 on (srcpart_parquet.ds = srcpart_date_hour_n1.ds and srcpart_parquet.hr = srcpart_date_hour_n1.hr) where srcpart_date_hour_n1.hour = 11 and (srcpart_date_hour_n1.`date` = '2008-04-08' or srcpart_date_hour_n1.`date` = '2008-04-09');
select count(*) from srcpart where (ds = '2008-04-08' or ds = '2008-04-09') and hr = 11;

drop table srcpart_parquet;
drop table srcpart_date_n4;
drop table srcpart_hour_n1;
drop table srcpart_date_hour_n1;
drop table srcpart_double_hour_n1;
