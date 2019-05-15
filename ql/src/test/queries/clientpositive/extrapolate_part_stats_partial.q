set hive.mapred.mode=nonstrict;
set hive.stats.fetch.column.stats=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.metastore.aggregate.stats.cache.enabled=false;


create table if not exists ext_loc_n1 (
  state string,
  locid int,
  zip int,
  year string
) row format delimited fields terminated by '|' stored as textfile;

LOAD DATA LOCAL INPATH '../../data/files/extrapolate_stats_partial.txt' OVERWRITE INTO TABLE ext_loc_n1;

create table if not exists loc_orc_1d_n1 (
  state string,
  locid int,
  zip int
) partitioned by(year string) stored as orc;

insert overwrite table loc_orc_1d_n1 partition(year) select * from ext_loc_n1;

analyze table loc_orc_1d_n1 partition(year='2001') compute statistics for columns state,locid;

analyze table loc_orc_1d_n1 partition(year='2002') compute statistics for columns state,locid;

describe formatted loc_orc_1d_n1 PARTITION(year='2001') state;

describe formatted loc_orc_1d_n1 PARTITION(year='2002') state;

-- basicStatState: COMPLETE colStatState: PARTIAL
explain extended select state from loc_orc_1d_n1;

-- column statistics for __HIVE_DEFAULT_PARTITION__ is not supported yet. Hence colStatState reports PARTIAL
-- basicStatState: COMPLETE colStatState: PARTIAL
explain extended select state,locid from loc_orc_1d_n1;

analyze table loc_orc_1d_n1 partition(year='2000') compute statistics for columns state;

analyze table loc_orc_1d_n1 partition(year='2003') compute statistics for columns state;

explain extended select state from loc_orc_1d_n1;

explain extended select state,locid from loc_orc_1d_n1;

create table if not exists loc_orc_2d_n1 (
  state string,
  locid int
) partitioned by(zip int, year string) stored as orc;

insert overwrite table loc_orc_2d_n1 partition(zip, year) select * from ext_loc_n1;

analyze table loc_orc_2d_n1 partition(zip=94086, year='2001') compute statistics for columns state,locid;

analyze table loc_orc_2d_n1 partition(zip=94087, year='2002') compute statistics for columns state,locid;

explain extended select state from loc_orc_2d_n1;

explain extended select state,locid from loc_orc_2d_n1;
