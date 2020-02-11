set hive.optimize.filter.stats.reduction=true;
set hive.mapred.mode=nonstrict;
set hive.stats.fetch.column.stats=true;

create table if not exists loc_staging_n0 (
  state string,
  locid int,
  zip bigint,
  year int
) row format delimited fields terminated by '|' stored as textfile;

create table loc_orc_n0 like loc_staging_n0;
alter table loc_orc_n0 set fileformat orc;

load data local inpath '../../data/files/loc.txt' overwrite into table loc_staging_n0;

insert overwrite table loc_orc_n0 select * from loc_staging_n0;

analyze table loc_orc_n0 compute statistics for columns state,locid,zip,year;

-- always true
explain select * from loc_orc_n0 where locid < 30;
-- always false
explain select * from loc_orc_n0 where locid > 30;
-- always true
explain select * from loc_orc_n0 where locid <= 30;
-- always false
explain select * from loc_orc_n0 where locid >= 30;

-- nothing to do
explain select * from loc_orc_n0 where locid < 6;
-- always false
explain select * from loc_orc_n0 where locid > 6;
-- always true
explain select * from loc_orc_n0 where locid <= 6;
-- nothing to do
explain select * from loc_orc_n0 where locid >= 6;

-- always false
explain select * from loc_orc_n0 where locid < 1;
-- nothing to do
explain select * from loc_orc_n0 where locid > 1;
-- nothing to do
explain select * from loc_orc_n0 where locid <= 1;
-- always true
explain select * from loc_orc_n0 where locid >= 1;

-- 5 should stay
explain select * from loc_orc_n0 where locid IN (-4,5,30,40);
-- nothing to do
explain select * from loc_orc_n0 where locid IN (5,2,3);
-- 1 and 6 should be left
explain select * from loc_orc_n0 where locid IN (1,6,9);
-- always false
explain select * from loc_orc_n0 where locid IN (40,30);



create table t_n7 ( s string);
insert into t_n7 values (null),(null);
analyze table t_n7 compute statistics for columns s;

-- true
explain select * from t_n7 where s is null;
explain select * from loc_orc_n0 where locid is not null;
-- false
explain select * from t_n7 where s is not null;
explain select * from loc_orc_n0 where locid is null;

insert into t_n7 values ('val1');
analyze table t_n7 compute statistics for columns s;

-- untouched
explain select * from t_n7 where s is not null;
explain select * from t_n7 where s is null;
