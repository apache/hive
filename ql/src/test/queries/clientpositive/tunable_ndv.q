set hive.mapred.mode=nonstrict;
set hive.stats.fetch.column.stats=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.metastore.aggregate.stats.cache.enabled=false;

create table if not exists ext_loc (
  state string,
  locid int,
  zip int,
  year string
) row format delimited fields terminated by '|' stored as textfile;

LOAD DATA LOCAL INPATH '../../data/files/extrapolate_stats_full.txt' OVERWRITE INTO TABLE ext_loc;

create table if not exists loc_orc_1d (
  state string,
  locid int,
  zip int
) partitioned by(year string) stored as orc;

insert overwrite table loc_orc_1d partition(year) select * from ext_loc;

analyze table loc_orc_1d compute statistics for columns state,locid;

describe formatted loc_orc_1d partition(year=2000) locid;
describe formatted loc_orc_1d partition(year=2001) locid;

describe formatted loc_orc_1d locid;

set hive.metastore.stats.ndv.tuner=1.0;

describe formatted loc_orc_1d locid;

set hive.metastore.stats.ndv.tuner=0.5;

describe formatted loc_orc_1d locid;

create table if not exists loc_orc_2d (
  state string,
  locid int
) partitioned by(zip int, year string) stored as orc;

insert overwrite table loc_orc_2d partition(zip, year) select * from ext_loc;

analyze table loc_orc_2d partition(zip=94086, year='2000') compute statistics for columns state,locid;

analyze table loc_orc_2d partition(zip=94087, year='2000') compute statistics for columns state,locid;

analyze table loc_orc_2d partition(zip=94086, year='2001') compute statistics for columns state,locid;

analyze table loc_orc_2d partition(zip=94087, year='2001') compute statistics for columns state,locid;

set hive.metastore.stats.ndv.tuner=0.0;

describe formatted loc_orc_2d locid;

set hive.metastore.stats.ndv.tuner=1.0;

describe formatted loc_orc_2d locid;

set hive.metastore.stats.ndv.tuner=0.5;

describe formatted loc_orc_2d locid;
