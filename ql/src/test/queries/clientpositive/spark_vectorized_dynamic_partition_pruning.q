--! qt:dataset:srcpart
set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;
set hive.spark.dynamic.partition.pruning=true;
set hive.optimize.metadataonly=false;
set hive.optimize.index.filter=true;
set hive.vectorized.execution.enabled=true;
set hive.strict.checks.cartesian.product=false;

-- SORT_QUERY_RESULTS
select distinct ds from srcpart;
select distinct hr from srcpart;

EXPLAIN VECTORIZATION DETAIL create table srcpart_date_n0 as select ds as ds, ds as `date` from srcpart group by ds;
create table srcpart_date_n0 stored as orc as select ds as ds, ds as `date` from srcpart group by ds;
create table srcpart_hour stored as orc as select hr as hr, hr as hour from srcpart group by hr;
create table srcpart_date_hour stored as orc as select ds as ds, ds as `date`, hr as hr, hr as hour from srcpart group by ds, hr;
create table srcpart_double_hour stored as orc as select (hr*2) as hr, hr as hour from srcpart group by hr;

-- single column, single key
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) where srcpart_date_n0.`date` = '2008-04-08';
select count(*) from srcpart join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) where srcpart_date_n0.`date` = '2008-04-08';
set hive.spark.dynamic.partition.pruning=false;
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) where srcpart_date_n0.`date` = '2008-04-08';
select count(*) from srcpart join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) where srcpart_date_n0.`date` = '2008-04-08';
set hive.spark.dynamic.partition.pruning=true;
select count(*) from srcpart where ds = '2008-04-08';

-- single column, single key, udf with typechange
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join srcpart_date_n0 on (day(srcpart.ds) = day(srcpart_date_n0.ds)) where srcpart_date_n0.`date` = '2008-04-08';
select count(*) from srcpart join srcpart_date_n0 on (day(srcpart.ds) = day(srcpart_date_n0.ds)) where srcpart_date_n0.`date` = '2008-04-08';
set hive.spark.dynamic.partition.pruning=false;
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join srcpart_date_n0 on (day(srcpart.ds) = day(srcpart_date_n0.ds)) where srcpart_date_n0.`date` = '2008-04-08';
select count(*) from srcpart join srcpart_date_n0 on (day(srcpart.ds) = day(srcpart_date_n0.ds)) where srcpart_date_n0.`date` = '2008-04-08';
set hive.spark.dynamic.partition.pruning=true;

-- multiple udfs and casts
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join srcpart_date_n0 on abs(negative(cast(concat(cast(day(srcpart.ds) as string), "0") as bigint)) + 10) = abs(negative(cast(concat(cast(day(srcpart_date_n0.ds) as string), "0") as bigint)) + 10) where srcpart_date_n0.`date` = '2008-04-08';
select count(*) from srcpart join srcpart_date_n0 on abs(negative(cast(concat(cast(day(srcpart.ds) as string), "0") as bigint)) + 10) = abs(negative(cast(concat(cast(day(srcpart_date_n0.ds) as string), "0") as bigint)) + 10) where srcpart_date_n0.`date` = '2008-04-08';

-- implicit type conversion between join columns
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join srcpart_date_n0 on cast(day(srcpart.ds) as smallint) = cast(day(srcpart_date_n0.ds) as decimal) where srcpart_date_n0.`date` = '2008-04-08';
select count(*) from srcpart join srcpart_date_n0 on cast(day(srcpart.ds) as smallint) = cast(day(srcpart_date_n0.ds) as decimal) where srcpart_date_n0.`date` = '2008-04-08';

-- multiple sources, single key
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) join srcpart_hour on (srcpart.hr = srcpart_hour.hr) 
where srcpart_date_n0.`date` = '2008-04-08' and srcpart_hour.hour = 11;
select count(*) from srcpart join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) join srcpart_hour on (srcpart.hr = srcpart_hour.hr) 
where srcpart_date_n0.`date` = '2008-04-08' and srcpart_hour.hour = 11;
set hive.spark.dynamic.partition.pruning=false;
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) join srcpart_hour on (srcpart.hr = srcpart_hour.hr) 
where srcpart_date_n0.`date` = '2008-04-08' and srcpart_hour.hour = 11;
select count(*) from srcpart join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) join srcpart_hour on (srcpart.hr = srcpart_hour.hr) 
where srcpart_date_n0.`date` = '2008-04-08' and srcpart_hour.hour = 11;
set hive.spark.dynamic.partition.pruning=true;
select count(*) from srcpart where hr = 11 and ds = '2008-04-08';

-- multiple columns single source
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join srcpart_date_hour on (srcpart.ds = srcpart_date_hour.ds and srcpart.hr = srcpart_date_hour.hr) where srcpart_date_hour.`date` = '2008-04-08' and srcpart_date_hour.hour = 11;
select count(*) from srcpart join srcpart_date_hour on (srcpart.ds = srcpart_date_hour.ds and srcpart.hr = srcpart_date_hour.hr) where srcpart_date_hour.`date` = '2008-04-08' and srcpart_date_hour.hour = 11;
set hive.spark.dynamic.partition.pruning=false;
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join srcpart_date_hour on (srcpart.ds = srcpart_date_hour.ds and srcpart.hr = srcpart_date_hour.hr) where srcpart_date_hour.`date` = '2008-04-08' and srcpart_date_hour.hour = 11;
select count(*) from srcpart join srcpart_date_hour on (srcpart.ds = srcpart_date_hour.ds and srcpart.hr = srcpart_date_hour.hr) where srcpart_date_hour.`date` = '2008-04-08' and srcpart_date_hour.hour = 11;
set hive.spark.dynamic.partition.pruning=true;
select count(*) from srcpart where ds = '2008-04-08' and hr = 11;

-- empty set
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) where srcpart_date_n0.`date` = 'I DONT EXIST';
select count(*) from srcpart join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) where srcpart_date_n0.`date` = 'I DONT EXIST';
set hive.spark.dynamic.partition.pruning=false;
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) where srcpart_date_n0.`date` = 'I DONT EXIST';
select count(*) from srcpart join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) where srcpart_date_n0.`date` = 'I DONT EXIST';
set hive.spark.dynamic.partition.pruning=true;
select count(*) from srcpart where ds = 'I DONT EXIST';

-- expressions
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join srcpart_double_hour on (srcpart.hr = cast(srcpart_double_hour.hr/2 as int)) where srcpart_double_hour.hour = 11;
select count(*) from srcpart join srcpart_double_hour on (srcpart.hr = cast(srcpart_double_hour.hr/2 as int)) where srcpart_double_hour.hour = 11;
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join srcpart_double_hour on (srcpart.hr*2 = srcpart_double_hour.hr) where srcpart_double_hour.hour = 11;
select count(*) from srcpart join srcpart_double_hour on (srcpart.hr*2 = srcpart_double_hour.hr) where srcpart_double_hour.hour = 11;
set hive.spark.dynamic.partition.pruning=false;
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join srcpart_double_hour on (srcpart.hr = cast(srcpart_double_hour.hr/2 as int)) where srcpart_double_hour.hour = 11;
select count(*) from srcpart join srcpart_double_hour on (srcpart.hr = cast(srcpart_double_hour.hr/2 as int)) where srcpart_double_hour.hour = 11;
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join srcpart_double_hour on (srcpart.hr*2 = srcpart_double_hour.hr) where srcpart_double_hour.hour = 11;
select count(*) from srcpart join srcpart_double_hour on (srcpart.hr*2 = srcpart_double_hour.hr) where srcpart_double_hour.hour = 11;
set hive.spark.dynamic.partition.pruning=true;
select count(*) from srcpart where hr = 11;
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join srcpart_double_hour on (cast(srcpart.hr*2 as string) = cast(srcpart_double_hour.hr as string)) where srcpart_double_hour.hour = 11;
select count(*) from srcpart join srcpart_double_hour on (cast(srcpart.hr*2 as string) = cast(srcpart_double_hour.hr as string)) where srcpart_double_hour.hour = 11;
set hive.spark.dynamic.partition.pruning=true;
select count(*) from srcpart where cast(hr as string) = 11;


-- parent is reduce tasks
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join (select ds as ds, ds as `date` from srcpart group by ds) s on (srcpart.ds = s.ds) where s.`date` = '2008-04-08';
select count(*) from srcpart join (select ds as ds, ds as `date` from srcpart group by ds) s on (srcpart.ds = s.ds) where s.`date` = '2008-04-08';
select count(*) from srcpart where ds = '2008-04-08';

-- non-equi join
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart, srcpart_date_hour where (srcpart_date_hour.`date` = '2008-04-08' and srcpart_date_hour.hour = 11) and (srcpart.ds = srcpart_date_hour.ds or srcpart.hr = srcpart_date_hour.hr);
select count(*) from srcpart, srcpart_date_hour where (srcpart_date_hour.`date` = '2008-04-08' and srcpart_date_hour.hour = 11) and (srcpart.ds = srcpart_date_hour.ds or srcpart.hr = srcpart_date_hour.hr);

-- old style join syntax
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart, srcpart_date_hour where srcpart_date_hour.`date` = '2008-04-08' and srcpart_date_hour.hour = 11 and srcpart.ds = srcpart_date_hour.ds and srcpart.hr = srcpart_date_hour.hr;
select count(*) from srcpart, srcpart_date_hour where srcpart_date_hour.`date` = '2008-04-08' and srcpart_date_hour.hour = 11 and srcpart.ds = srcpart_date_hour.ds and srcpart.hr = srcpart_date_hour.hr;

-- left join
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart left join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) where srcpart_date_n0.`date` = '2008-04-08';
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart_date_n0 left join srcpart on (srcpart.ds = srcpart_date_n0.ds) where srcpart_date_n0.`date` = '2008-04-08';

-- full outer
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart full outer join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) where srcpart_date_n0.`date` = '2008-04-08';

-- with static pruning
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) join srcpart_hour on (srcpart.hr = srcpart_hour.hr) 
where srcpart_date_n0.`date` = '2008-04-08' and srcpart_hour.hour = 11 and srcpart.hr = 11;
select count(*) from srcpart join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) join srcpart_hour on (srcpart.hr = srcpart_hour.hr) 
where srcpart_date_n0.`date` = '2008-04-08' and srcpart_hour.hour = 11 and srcpart.hr = 11;
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) join srcpart_hour on (srcpart.hr = srcpart_hour.hr) 
where srcpart_date_n0.`date` = '2008-04-08' and srcpart.hr = 13;
select count(*) from srcpart join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) join srcpart_hour on (srcpart.hr = srcpart_hour.hr) 
where srcpart_date_n0.`date` = '2008-04-08' and srcpart.hr = 13;

-- union + subquery
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart where srcpart.ds in (select max(srcpart.ds) from srcpart union all select min(srcpart.ds) from srcpart);
select count(*) from srcpart where srcpart.ds in (select max(srcpart.ds) from srcpart union all select min(srcpart.ds) from srcpart);
EXPLAIN VECTORIZATION DETAIL select distinct(ds) from srcpart where srcpart.ds in (select max(srcpart.ds) from srcpart union all select min(srcpart.ds) from srcpart);
select distinct(ds) from srcpart where srcpart.ds in (select max(srcpart.ds) from srcpart union all select min(srcpart.ds) from srcpart);
EXPLAIN VECTORIZATION DETAIL select ds from (select distinct(ds) as ds from srcpart union all select distinct(ds) as ds from srcpart) s where s.ds in (select max(srcpart.ds) from srcpart union all select min(srcpart.ds) from srcpart);
select ds from (select distinct(ds) as ds from srcpart union all select distinct(ds) as ds from srcpart) s where s.ds in (select max(srcpart.ds) from srcpart union all select min(srcpart.ds) from srcpart);

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask = true;
set hive.auto.convert.join.noconditionaltask.size = 10000000;

-- single column, single key
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) where srcpart_date_n0.`date` = '2008-04-08';
select count(*) from srcpart join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) where srcpart_date_n0.`date` = '2008-04-08';
select count(*) from srcpart where ds = '2008-04-08';

-- single column, single key, udf with typechange
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join srcpart_date_n0 on (day(srcpart.ds) = day(srcpart_date_n0.ds)) where srcpart_date_n0.`date` = '2008-04-08';
select count(*) from srcpart join srcpart_date_n0 on (day(srcpart.ds) = day(srcpart_date_n0.ds)) where srcpart_date_n0.`date` = '2008-04-08';

-- multiple sources, single key
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) join srcpart_hour on (srcpart.hr = srcpart_hour.hr) 
where srcpart_date_n0.`date` = '2008-04-08' and srcpart_hour.hour = 11;
select count(*) from srcpart join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) join srcpart_hour on (srcpart.hr = srcpart_hour.hr) 
where srcpart_date_n0.`date` = '2008-04-08' and srcpart_hour.hour = 11;
select count(*) from srcpart where hr = 11 and ds = '2008-04-08';

-- multiple columns single source
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join srcpart_date_hour on (srcpart.ds = srcpart_date_hour.ds and srcpart.hr = srcpart_date_hour.hr) where srcpart_date_hour.`date` = '2008-04-08' and srcpart_date_hour.hour = 11;
select count(*) from srcpart join srcpart_date_hour on (srcpart.ds = srcpart_date_hour.ds and srcpart.hr = srcpart_date_hour.hr) where srcpart_date_hour.`date` = '2008-04-08' and srcpart_date_hour.hour = 11;
select count(*) from srcpart where ds = '2008-04-08' and hr = 11;

-- empty set
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) where srcpart_date_n0.`date` = 'I DONT EXIST';
select count(*) from srcpart join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) where srcpart_date_n0.`date` = 'I DONT EXIST';

-- expressions
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join srcpart_double_hour on (srcpart.hr = cast(srcpart_double_hour.hr/2 as int)) where srcpart_double_hour.hour = 11;
select count(*) from srcpart join srcpart_double_hour on (srcpart.hr = cast(srcpart_double_hour.hr/2 as int)) where srcpart_double_hour.hour = 11;
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join srcpart_double_hour on (srcpart.hr*2 = srcpart_double_hour.hr) where srcpart_double_hour.hour = 11;
select count(*) from srcpart join srcpart_double_hour on (srcpart.hr*2 = srcpart_double_hour.hr) where srcpart_double_hour.hour = 11;
select count(*) from srcpart where hr = 11;

-- parent is reduce tasks
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join (select ds as ds, ds as `date` from srcpart group by ds) s on (srcpart.ds = s.ds) where s.`date` = '2008-04-08';
select count(*) from srcpart join (select ds as ds, ds as `date` from srcpart group by ds) s on (srcpart.ds = s.ds) where s.`date` = '2008-04-08';
select count(*) from srcpart where ds = '2008-04-08';

-- left join
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart left join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) where srcpart_date_n0.`date` = '2008-04-08';
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart_date_n0 left join srcpart on (srcpart.ds = srcpart_date_n0.ds) where srcpart_date_n0.`date` = '2008-04-08';

-- full outer
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart full outer join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) where srcpart_date_n0.`date` = '2008-04-08';

-- with static pruning
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) join srcpart_hour on (srcpart.hr = srcpart_hour.hr) 
where srcpart_date_n0.`date` = '2008-04-08' and srcpart_hour.hour = 11 and srcpart.hr = 11;
select count(*) from srcpart join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) join srcpart_hour on (srcpart.hr = srcpart_hour.hr) 
where srcpart_date_n0.`date` = '2008-04-08' and srcpart_hour.hour = 11 and srcpart.hr = 11;
EXPLAIN VECTORIZATION DETAIL select count(*) from srcpart join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) join srcpart_hour on (srcpart.hr = srcpart_hour.hr) 
where srcpart_date_n0.`date` = '2008-04-08' and srcpart.hr = 13;
select count(*) from srcpart join srcpart_date_n0 on (srcpart.ds = srcpart_date_n0.ds) join srcpart_hour on (srcpart.hr = srcpart_hour.hr) 
where srcpart_date_n0.`date` = '2008-04-08' and srcpart.hr = 13;

-- union + subquery
EXPLAIN VECTORIZATION DETAIL select distinct(ds) from srcpart where srcpart.ds in (select max(srcpart.ds) from srcpart union all select min(srcpart.ds) from srcpart);
select distinct(ds) from srcpart where srcpart.ds in (select max(srcpart.ds) from srcpart union all select min(srcpart.ds) from srcpart);

drop table srcpart_date_n0;
drop table srcpart_hour;
drop table srcpart_date_hour;
drop table srcpart_double_hour;
