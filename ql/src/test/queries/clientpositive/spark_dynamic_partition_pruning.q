set hive.support.sql11.reserved.keywords=false;
set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;
set hive.spark.dynamic.partition.pruning=true;
set hive.optimize.metadataonly=false;
set hive.optimize.index.filter=true;

-- SORT_QUERY_RESULTS

select distinct ds from srcpart;
select distinct hr from srcpart;

EXPLAIN create table srcpart_date as select ds as ds, ds as date from srcpart group by ds;
create table srcpart_date as select ds as ds, ds as date from srcpart group by ds;
create table srcpart_hour as select hr as hr, hr as hour from srcpart group by hr;
create table srcpart_date_hour as select ds as ds, ds as date, hr as hr, hr as hour from srcpart group by ds, hr;
create table srcpart_double_hour as select (hr*2) as hr, hr as hour from srcpart group by hr;

-- single column, single key
EXPLAIN select count(*) from srcpart join srcpart_date on (srcpart.ds = srcpart_date.ds) where srcpart_date.date = '2008-04-08';
select count(*) from srcpart join srcpart_date on (srcpart.ds = srcpart_date.ds) where srcpart_date.date = '2008-04-08';
set hive.spark.dynamic.partition.pruning=false;
EXPLAIN select count(*) from srcpart join srcpart_date on (srcpart.ds = srcpart_date.ds) where srcpart_date.date = '2008-04-08';
select count(*) from srcpart join srcpart_date on (srcpart.ds = srcpart_date.ds) where srcpart_date.date = '2008-04-08';
set hive.spark.dynamic.partition.pruning=true;
select count(*) from srcpart where ds = '2008-04-08';

-- multiple sources, single key
EXPLAIN select count(*) from srcpart join srcpart_date on (srcpart.ds = srcpart_date.ds) join srcpart_hour on (srcpart.hr = srcpart_hour.hr) 
where srcpart_date.date = '2008-04-08' and srcpart_hour.hour = 11;
select count(*) from srcpart join srcpart_date on (srcpart.ds = srcpart_date.ds) join srcpart_hour on (srcpart.hr = srcpart_hour.hr) 
where srcpart_date.date = '2008-04-08' and srcpart_hour.hour = 11;
set hive.spark.dynamic.partition.pruning=false;
EXPLAIN select count(*) from srcpart join srcpart_date on (srcpart.ds = srcpart_date.ds) join srcpart_hour on (srcpart.hr = srcpart_hour.hr) 
where srcpart_date.date = '2008-04-08' and srcpart_hour.hour = 11;
select count(*) from srcpart join srcpart_date on (srcpart.ds = srcpart_date.ds) join srcpart_hour on (srcpart.hr = srcpart_hour.hr) 
where srcpart_date.date = '2008-04-08' and srcpart_hour.hour = 11;
set hive.spark.dynamic.partition.pruning=true;
select count(*) from srcpart where hr = 11 and ds = '2008-04-08';

-- multiple columns single source
EXPLAIN select count(*) from srcpart join srcpart_date_hour on (srcpart.ds = srcpart_date_hour.ds and srcpart.hr = srcpart_date_hour.hr) where srcpart_date_hour.date = '2008-04-08' and srcpart_date_hour.hour = 11;
select count(*) from srcpart join srcpart_date_hour on (srcpart.ds = srcpart_date_hour.ds and srcpart.hr = srcpart_date_hour.hr) where srcpart_date_hour.date = '2008-04-08' and srcpart_date_hour.hour = 11;
set hive.spark.dynamic.partition.pruning=false;
EXPLAIN select count(*) from srcpart join srcpart_date_hour on (srcpart.ds = srcpart_date_hour.ds and srcpart.hr = srcpart_date_hour.hr) where srcpart_date_hour.date = '2008-04-08' and srcpart_date_hour.hour = 11;
select count(*) from srcpart join srcpart_date_hour on (srcpart.ds = srcpart_date_hour.ds and srcpart.hr = srcpart_date_hour.hr) where srcpart_date_hour.date = '2008-04-08' and srcpart_date_hour.hour = 11;
set hive.spark.dynamic.partition.pruning=true;
select count(*) from srcpart where ds = '2008-04-08' and hr = 11;

-- empty set
EXPLAIN select count(*) from srcpart join srcpart_date on (srcpart.ds = srcpart_date.ds) where srcpart_date.date = 'I DONT EXIST';
select count(*) from srcpart join srcpart_date on (srcpart.ds = srcpart_date.ds) where srcpart_date.date = 'I DONT EXIST';
set hive.spark.dynamic.partition.pruning=false;
EXPLAIN select count(*) from srcpart join srcpart_date on (srcpart.ds = srcpart_date.ds) where srcpart_date.date = 'I DONT EXIST';
select count(*) from srcpart join srcpart_date on (srcpart.ds = srcpart_date.ds) where srcpart_date.date = 'I DONT EXIST';
set hive.spark.dynamic.partition.pruning=true;
select count(*) from srcpart where ds = 'I DONT EXIST';

-- expressions
EXPLAIN select count(*) from srcpart join srcpart_double_hour on (srcpart.hr = cast(srcpart_double_hour.hr/2 as int)) where srcpart_double_hour.hour = 11;
select count(*) from srcpart join srcpart_double_hour on (srcpart.hr = cast(srcpart_double_hour.hr/2 as int)) where srcpart_double_hour.hour = 11;
EXPLAIN select count(*) from srcpart join srcpart_double_hour on (srcpart.hr*2 = srcpart_double_hour.hr) where srcpart_double_hour.hour = 11;
select count(*) from srcpart join srcpart_double_hour on (srcpart.hr*2 = srcpart_double_hour.hr) where srcpart_double_hour.hour = 11;
set hive.spark.dynamic.partition.pruning=false;
EXPLAIN select count(*) from srcpart join srcpart_double_hour on (srcpart.hr = cast(srcpart_double_hour.hr/2 as int)) where srcpart_double_hour.hour = 11;
select count(*) from srcpart join srcpart_double_hour on (srcpart.hr = cast(srcpart_double_hour.hr/2 as int)) where srcpart_double_hour.hour = 11;
EXPLAIN select count(*) from srcpart join srcpart_double_hour on (srcpart.hr*2 = srcpart_double_hour.hr) where srcpart_double_hour.hour = 11;
select count(*) from srcpart join srcpart_double_hour on (srcpart.hr*2 = srcpart_double_hour.hr) where srcpart_double_hour.hour = 11;
set hive.spark.dynamic.partition.pruning=true;
select count(*) from srcpart where hr = 11;
EXPLAIN select count(*) from srcpart join srcpart_double_hour on (cast(srcpart.hr*2 as string) = cast(srcpart_double_hour.hr as string)) where srcpart_double_hour.hour = 11;
select count(*) from srcpart join srcpart_double_hour on (cast(srcpart.hr*2 as string) = cast(srcpart_double_hour.hr as string)) where srcpart_double_hour.hour = 11;
set hive.spark.dynamic.partition.pruning=true;
select count(*) from srcpart where cast(hr as string) = 11;


-- parent is reduce tasks
EXPLAIN select count(*) from srcpart join (select ds as ds, ds as date from srcpart group by ds) s on (srcpart.ds = s.ds) where s.date = '2008-04-08';
select count(*) from srcpart join (select ds as ds, ds as date from srcpart group by ds) s on (srcpart.ds = s.ds) where s.date = '2008-04-08';
select count(*) from srcpart where ds = '2008-04-08';

-- non-equi join
EXPLAIN select count(*) from srcpart, srcpart_date_hour where (srcpart_date_hour.date = '2008-04-08' and srcpart_date_hour.hour = 11) and (srcpart.ds = srcpart_date_hour.ds or srcpart.hr = srcpart_date_hour.hr);
select count(*) from srcpart, srcpart_date_hour where (srcpart_date_hour.date = '2008-04-08' and srcpart_date_hour.hour = 11) and (srcpart.ds = srcpart_date_hour.ds or srcpart.hr = srcpart_date_hour.hr);

-- old style join syntax
EXPLAIN select count(*) from srcpart, srcpart_date_hour where srcpart_date_hour.date = '2008-04-08' and srcpart_date_hour.hour = 11 and srcpart.ds = srcpart_date_hour.ds and srcpart.hr = srcpart_date_hour.hr;
select count(*) from srcpart, srcpart_date_hour where srcpart_date_hour.date = '2008-04-08' and srcpart_date_hour.hour = 11 and srcpart.ds = srcpart_date_hour.ds and srcpart.hr = srcpart_date_hour.hr;

-- left join
EXPLAIN select count(*) from srcpart left join srcpart_date on (srcpart.ds = srcpart_date.ds) where srcpart_date.date = '2008-04-08';
EXPLAIN select count(*) from srcpart_date left join srcpart on (srcpart.ds = srcpart_date.ds) where srcpart_date.date = '2008-04-08';

-- full outer
EXPLAIN select count(*) from srcpart full outer join srcpart_date on (srcpart.ds = srcpart_date.ds) where srcpart_date.date = '2008-04-08';

-- with static pruning
EXPLAIN select count(*) from srcpart join srcpart_date on (srcpart.ds = srcpart_date.ds) join srcpart_hour on (srcpart.hr = srcpart_hour.hr) 
where srcpart_date.date = '2008-04-08' and srcpart_hour.hour = 11 and srcpart.hr = 11;
select count(*) from srcpart join srcpart_date on (srcpart.ds = srcpart_date.ds) join srcpart_hour on (srcpart.hr = srcpart_hour.hr) 
where srcpart_date.date = '2008-04-08' and srcpart_hour.hour = 11 and srcpart.hr = 11;
EXPLAIN select count(*) from srcpart join srcpart_date on (srcpart.ds = srcpart_date.ds) join srcpart_hour on (srcpart.hr = srcpart_hour.hr) 
where srcpart_date.date = '2008-04-08' and srcpart.hr = 13;
select count(*) from srcpart join srcpart_date on (srcpart.ds = srcpart_date.ds) join srcpart_hour on (srcpart.hr = srcpart_hour.hr) 
where srcpart_date.date = '2008-04-08' and srcpart.hr = 13;

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
EXPLAIN select count(*) from srcpart join srcpart_date on (srcpart.ds = srcpart_date.ds) where srcpart_date.date = '2008-04-08';
select count(*) from srcpart join srcpart_date on (srcpart.ds = srcpart_date.ds) where srcpart_date.date = '2008-04-08';
select count(*) from srcpart where ds = '2008-04-08';

-- multiple sources, single key
EXPLAIN select count(*) from srcpart join srcpart_date on (srcpart.ds = srcpart_date.ds) join srcpart_hour on (srcpart.hr = srcpart_hour.hr) 
where srcpart_date.date = '2008-04-08' and srcpart_hour.hour = 11;
select count(*) from srcpart join srcpart_date on (srcpart.ds = srcpart_date.ds) join srcpart_hour on (srcpart.hr = srcpart_hour.hr) 
where srcpart_date.date = '2008-04-08' and srcpart_hour.hour = 11;
select count(*) from srcpart where hr = 11 and ds = '2008-04-08';

-- multiple columns single source
EXPLAIN select count(*) from srcpart join srcpart_date_hour on (srcpart.ds = srcpart_date_hour.ds and srcpart.hr = srcpart_date_hour.hr) where srcpart_date_hour.date = '2008-04-08' and srcpart_date_hour.hour = 11;
select count(*) from srcpart join srcpart_date_hour on (srcpart.ds = srcpart_date_hour.ds and srcpart.hr = srcpart_date_hour.hr) where srcpart_date_hour.date = '2008-04-08' and srcpart_date_hour.hour = 11;
select count(*) from srcpart where ds = '2008-04-08' and hr = 11;

-- empty set
EXPLAIN select count(*) from srcpart join srcpart_date on (srcpart.ds = srcpart_date.ds) where srcpart_date.date = 'I DONT EXIST';
-- Disabled until TEZ-1486 is fixed
-- select count(*) from srcpart join srcpart_date on (srcpart.ds = srcpart_date.ds) where srcpart_date.date = 'I DONT EXIST';

-- expressions
EXPLAIN select count(*) from srcpart join srcpart_double_hour on (srcpart.hr = cast(srcpart_double_hour.hr/2 as int)) where srcpart_double_hour.hour = 11;
select count(*) from srcpart join srcpart_double_hour on (srcpart.hr = cast(srcpart_double_hour.hr/2 as int)) where srcpart_double_hour.hour = 11;
EXPLAIN select count(*) from srcpart join srcpart_double_hour on (srcpart.hr*2 = srcpart_double_hour.hr) where srcpart_double_hour.hour = 11;
select count(*) from srcpart join srcpart_double_hour on (srcpart.hr*2 = srcpart_double_hour.hr) where srcpart_double_hour.hour = 11;
select count(*) from srcpart where hr = 11;

-- parent is reduce tasks
EXPLAIN select count(*) from srcpart join (select ds as ds, ds as date from srcpart group by ds) s on (srcpart.ds = s.ds) where s.date = '2008-04-08';
select count(*) from srcpart join (select ds as ds, ds as date from srcpart group by ds) s on (srcpart.ds = s.ds) where s.date = '2008-04-08';
select count(*) from srcpart where ds = '2008-04-08';

-- left join
EXPLAIN select count(*) from srcpart left join srcpart_date on (srcpart.ds = srcpart_date.ds) where srcpart_date.date = '2008-04-08';
EXPLAIN select count(*) from srcpart_date left join srcpart on (srcpart.ds = srcpart_date.ds) where srcpart_date.date = '2008-04-08';

-- full outer
EXPLAIN select count(*) from srcpart full outer join srcpart_date on (srcpart.ds = srcpart_date.ds) where srcpart_date.date = '2008-04-08';

-- with static pruning
EXPLAIN select count(*) from srcpart join srcpart_date on (srcpart.ds = srcpart_date.ds) join srcpart_hour on (srcpart.hr = srcpart_hour.hr) 
where srcpart_date.date = '2008-04-08' and srcpart_hour.hour = 11 and srcpart.hr = 11;
select count(*) from srcpart join srcpart_date on (srcpart.ds = srcpart_date.ds) join srcpart_hour on (srcpart.hr = srcpart_hour.hr) 
where srcpart_date.date = '2008-04-08' and srcpart_hour.hour = 11 and srcpart.hr = 11;
EXPLAIN select count(*) from srcpart join srcpart_date on (srcpart.ds = srcpart_date.ds) join srcpart_hour on (srcpart.hr = srcpart_hour.hr) 
where srcpart_date.date = '2008-04-08' and srcpart.hr = 13;
-- Disabled until TEZ-1486 is fixed
-- select count(*) from srcpart join srcpart_date on (srcpart.ds = srcpart_date.ds) join srcpart_hour on (srcpart.hr = srcpart_hour.hr) 
-- where srcpart_date.date = '2008-04-08' and srcpart.hr = 13;

-- union + subquery
EXPLAIN select distinct(ds) from srcpart where srcpart.ds in (select max(srcpart.ds) from srcpart union all select min(srcpart.ds) from srcpart);
select distinct(ds) from srcpart where srcpart.ds in (select max(srcpart.ds) from srcpart union all select min(srcpart.ds) from srcpart);


drop table srcpart_orc;
drop table srcpart_date;
drop table srcpart_hour;
drop table srcpart_date_hour;
drop table srcpart_double_hour;
