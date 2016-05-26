set hive.optimize.filter.stats.reduction=true;
set hive.mapred.mode=nonstrict;
set hive.stats.fetch.column.stats=true;

create table if not exists loc_staging (
  state string,
  locid int,
  zip bigint,
  year int
) row format delimited fields terminated by '|' stored as textfile;

create table loc_orc like loc_staging;
alter table loc_orc set fileformat orc;

load data local inpath '../../data/files/loc.txt' overwrite into table loc_staging;

insert overwrite table loc_orc select * from loc_staging;

analyze table loc_orc compute statistics for columns state,locid,zip,year;

-- always true
explain select * from loc_orc where locid < 30;
-- always false
explain select * from loc_orc where locid > 30;
-- always true
explain select * from loc_orc where locid <= 30;
-- always false
explain select * from loc_orc where locid >= 30;

-- nothing to do
explain select * from loc_orc where locid < 6;
-- always false
explain select * from loc_orc where locid > 6;
-- always true
explain select * from loc_orc where locid <= 6;
-- nothing to do
explain select * from loc_orc where locid >= 6;

-- always false
explain select * from loc_orc where locid < 1;
-- nothing to do
explain select * from loc_orc where locid > 1;
-- nothing to do
explain select * from loc_orc where locid <= 1;
-- always true
explain select * from loc_orc where locid >= 1;

-- 5 should stay
explain select * from loc_orc where locid IN (-4,5,30,40);
-- nothing to do
explain select * from loc_orc where locid IN (5,2,3);
-- 1 and 6 should be left
explain select * from loc_orc where locid IN (1,6,9);
-- always false
explain select * from loc_orc where locid IN (40,30);
