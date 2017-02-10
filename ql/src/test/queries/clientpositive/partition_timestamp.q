set hive.mapred.mode=nonstrict;
drop table partition_timestamp_1;

create table partition_timestamp_1 (key string, value string) partitioned by (dt timestamp, region string);

insert overwrite table partition_timestamp_1 partition(dt='2000-01-01 01:00:00', region= '1')
  select * from src tablesample (10 rows);
insert overwrite table partition_timestamp_1 partition(dt='2000-01-01 02:00:00', region= '2')
  select * from src tablesample (5 rows);
insert overwrite table partition_timestamp_1 partition(dt='2001-01-01 01:00:00', region= '2020-20-20')
  select * from src tablesample (5 rows);
insert overwrite table partition_timestamp_1 partition(dt='2001-01-01 02:00:00', region= '1')
  select * from src tablesample (20 rows);
insert overwrite table partition_timestamp_1 partition(dt='2001-01-01 03:00:00', region= '10')
  select * from src tablesample (11 rows);

select distinct dt from partition_timestamp_1;
select * from partition_timestamp_1 where dt = '2000-01-01 01:00:00' and region = '2' order by key,value;

-- 10
select count(*) from partition_timestamp_1 where dt = timestamp '2000-01-01 01:00:00';
-- 10.  Also try with string value in predicate
select count(*) from partition_timestamp_1 where dt = '2000-01-01 01:00:00';
-- 5
select count(*) from partition_timestamp_1 where dt = timestamp '2000-01-01 02:00:00' and region = '2';
-- 11
select count(*) from partition_timestamp_1 where dt = timestamp '2001-01-01 03:00:00' and region = '10';
-- 30
select count(*) from partition_timestamp_1 where region = '1';
-- 0
select count(*) from partition_timestamp_1 where dt = timestamp '2000-01-01 01:00:00' and region = '3';
-- 0
select count(*) from partition_timestamp_1 where dt = timestamp '1999-01-01 01:00:00';

-- Try other comparison operations

-- 20
select count(*) from partition_timestamp_1 where dt > timestamp '2000-01-01 01:00:00' and region = '1';
-- 10
select count(*) from partition_timestamp_1 where dt < timestamp '2000-01-02 01:00:00' and region = '1';
-- 20
select count(*) from partition_timestamp_1 where dt >= timestamp '2000-01-02 01:00:00' and region = '1';
-- 10
select count(*) from partition_timestamp_1 where dt <= timestamp '2000-01-01 01:00:00' and region = '1';
-- 20
select count(*) from partition_timestamp_1 where dt <> timestamp '2000-01-01 01:00:00' and region = '1';
-- 10
select count(*) from partition_timestamp_1 where dt between timestamp '1999-12-30 12:00:00' and timestamp '2000-01-03 12:00:00' and region = '1';


-- Try a string key with timestamp-like strings

-- 5
select count(*) from partition_timestamp_1 where region = '2020-20-20';
-- 5
select count(*) from partition_timestamp_1 where region > '2010-01-01';

drop table partition_timestamp_1;
