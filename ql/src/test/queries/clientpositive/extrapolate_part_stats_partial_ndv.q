set hive.mapred.mode=nonstrict;
set hive.metastore.stats.ndv.densityfunction=true;
set hive.stats.fetch.column.stats=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.metastore.aggregate.stats.cache.enabled=false;


drop table if exists ext_loc;

create table ext_loc (
  state string,
  locid double,
  cnt decimal,
  zip int,
  year string
) row format delimited fields terminated by '|' stored as textfile;

LOAD DATA LOCAL INPATH '../../data/files/extrapolate_stats_partial_ndv.txt' OVERWRITE INTO TABLE ext_loc;

drop table if exists loc_orc_1d;

create table loc_orc_1d (
  state string,
  locid double,
  cnt decimal,
  zip int
) partitioned by(year string) stored as orc;

insert overwrite table loc_orc_1d partition(year) select * from ext_loc;

analyze table loc_orc_1d partition(year='2001') compute statistics for columns state,locid,cnt,zip;

analyze table loc_orc_1d partition(year='2002') compute statistics for columns state,locid,cnt,zip;

describe formatted loc_orc_1d PARTITION(year='2001') state;

describe formatted loc_orc_1d PARTITION(year='2002') state;

describe formatted loc_orc_1d PARTITION(year='2001') locid;

describe formatted loc_orc_1d PARTITION(year='2002') locid;

describe formatted loc_orc_1d PARTITION(year='2001') cnt;

describe formatted loc_orc_1d PARTITION(year='2002') cnt;

describe formatted loc_orc_1d PARTITION(year='2001') zip;

describe formatted loc_orc_1d PARTITION(year='2002') zip;

explain extended select state,locid,cnt,zip from loc_orc_1d;

analyze table loc_orc_1d partition(year='2000') compute statistics for columns state,locid,cnt,zip;

analyze table loc_orc_1d partition(year='2003') compute statistics for columns state,locid,cnt,zip;

describe formatted loc_orc_1d PARTITION(year='2000') state;

describe formatted loc_orc_1d PARTITION(year='2003') state;

describe formatted loc_orc_1d PARTITION(year='2000') locid;

describe formatted loc_orc_1d PARTITION(year='2003') locid;

describe formatted loc_orc_1d PARTITION(year='2000') cnt;

describe formatted loc_orc_1d PARTITION(year='2003') cnt;

describe formatted loc_orc_1d PARTITION(year='2000') zip;

describe formatted loc_orc_1d PARTITION(year='2003') zip;

explain extended select state,locid,cnt,zip from loc_orc_1d;

drop table if exists loc_orc_2d;

create table loc_orc_2d (
  state string,
  locid int,
  cnt decimal
) partitioned by(zip int, year string) stored as orc;

insert overwrite table loc_orc_2d partition(zip, year) select * from ext_loc;

analyze table loc_orc_2d partition(zip=94086, year='2001') compute statistics for columns state,locid,cnt;

analyze table loc_orc_2d partition(zip=94087, year='2002') compute statistics for columns state,locid,cnt;

describe formatted loc_orc_2d partition(zip=94086, year='2001') state;

describe formatted loc_orc_2d partition(zip=94087, year='2002') state;

describe formatted loc_orc_2d partition(zip=94086, year='2001') locid;

describe formatted loc_orc_2d partition(zip=94087, year='2002') locid;

describe formatted loc_orc_2d partition(zip=94086, year='2001') cnt;

describe formatted loc_orc_2d partition(zip=94087, year='2002') cnt;

explain extended select state,locid,cnt,zip from loc_orc_2d;
