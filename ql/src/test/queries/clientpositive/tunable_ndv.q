set hive.mapred.mode=nonstrict;
set hive.stats.fetch.column.stats=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.metastore.aggregate.stats.cache.enabled=false;

create table if not exists ext_loc_n2 (
  state string,
  locid int,
  zip int,
  year string
) row format delimited fields terminated by '|' stored as textfile;

LOAD DATA LOCAL INPATH '../../data/files/extrapolate_stats_full.txt' OVERWRITE INTO TABLE ext_loc_n2;

create table if not exists loc_orc_1d_n2 (
  state string,
  locid int,
  zip int
) partitioned by(year string) stored as orc;

insert overwrite table loc_orc_1d_n2 partition(year) select * from ext_loc_n2;

analyze table loc_orc_1d_n2 compute statistics for columns state,locid;

describe formatted loc_orc_1d_n2 partition(year=2000) locid;
describe formatted loc_orc_1d_n2 partition(year=2001) locid;

describe formatted loc_orc_1d_n2 locid;

set hive.metastore.stats.ndv.tuner=1.0;

describe formatted loc_orc_1d_n2 locid;

set hive.metastore.stats.ndv.tuner=0.5;

describe formatted loc_orc_1d_n2 locid;

create table if not exists loc_orc_2d_n2 (
  state string,
  locid int
) partitioned by(zip int, year string) stored as orc;

insert overwrite table loc_orc_2d_n2 partition(zip, year) select * from ext_loc_n2;

analyze table loc_orc_2d_n2 partition(zip=94086, year='2000') compute statistics for columns state,locid;

analyze table loc_orc_2d_n2 partition(zip=94087, year='2000') compute statistics for columns state,locid;

analyze table loc_orc_2d_n2 partition(zip=94086, year='2001') compute statistics for columns state,locid;

analyze table loc_orc_2d_n2 partition(zip=94087, year='2001') compute statistics for columns state,locid;

set hive.metastore.stats.ndv.tuner=0.0;

describe formatted loc_orc_2d_n2 locid;

set hive.metastore.stats.ndv.tuner=1.0;

describe formatted loc_orc_2d_n2 locid;

set hive.metastore.stats.ndv.tuner=0.5;

describe formatted loc_orc_2d_n2 locid;
