set hive.mapred.mode=nonstrict;
set hive.stats.fetch.column.stats=true;
set hive.stats.autogather=false;
set hive.exec.dynamic.partition=true;
set hive.metastore.aggregate.stats.cache.enabled=false;

create table if not exists loc_staging_n4 (
  state string,
  locid int,
  zip bigint,
  year string
) row format delimited fields terminated by '|' stored as textfile;

LOAD DATA LOCAL INPATH '../../data/files/loc.txt' OVERWRITE INTO TABLE loc_staging_n4;

create table if not exists loc_orc_n4 (
  state string,
  locid int,
  zip bigint
) partitioned by(year string) stored as orc;

-- basicStatState: NONE colStatState: NONE
explain select * from loc_orc_n4;

insert overwrite table loc_orc_n4 partition(year) select * from loc_staging_n4;

-- stats are disabled. basic stats will report the file size but not raw data size. so initial statistics will be PARTIAL

-- basicStatState: PARTIAL colStatState: NONE
explain select * from loc_orc_n4;

-- partition level analyze statistics for specific parition
analyze table loc_orc_n4 partition(year='2001') compute statistics;

-- basicStatState: PARTIAL colStatState: NONE
explain select * from loc_orc_n4 where year='__HIVE_DEFAULT_PARTITION__';

-- basicStatState: PARTIAL colStatState: NONE
explain select * from loc_orc_n4;

-- basicStatState: COMPLETE colStatState: NONE
explain select * from loc_orc_n4 where year='2001';

-- partition level analyze statistics for all partitions
analyze table loc_orc_n4 partition(year) compute statistics;

-- basicStatState: COMPLETE colStatState: NONE
explain select * from loc_orc_n4 where year='__HIVE_DEFAULT_PARTITION__';

-- basicStatState: COMPLETE colStatState: NONE
explain select * from loc_orc_n4;

-- basicStatState: COMPLETE colStatState: NONE
explain select * from loc_orc_n4 where year='2001' or year='__HIVE_DEFAULT_PARTITION__';

-- both partitions will be pruned
-- basicStatState: NONE colStatState: NONE
explain select * from loc_orc_n4 where year='2001' and year='__HIVE_DEFAULT_PARTITION__';

-- partition level partial column statistics
analyze table loc_orc_n4 partition(year='2001') compute statistics for columns state,locid;

-- basicStatState: COMPLETE colStatState: NONE
explain select zip from loc_orc_n4;

-- basicStatState: COMPLETE colStatState: PARTIAL
explain select state from loc_orc_n4;

-- basicStatState: COMPLETE colStatState: COMPLETE
explain select year from loc_orc_n4;

-- column statistics for __HIVE_DEFAULT_PARTITION__ is not supported yet. Hence colStatState reports PARTIAL
-- basicStatState: COMPLETE colStatState: PARTIAL
explain select state,locid from loc_orc_n4;

-- basicStatState: COMPLETE colStatState: COMPLETE
explain select state,locid from loc_orc_n4 where year='2001';

-- basicStatState: COMPLETE colStatState: NONE
explain select state,locid from loc_orc_n4 where year!='2001';

-- basicStatState: COMPLETE colStatState: PARTIAL
explain select * from loc_orc_n4;

-- This is to test filter expression evaluation on partition column
-- numRows: 2 dataSize: 8 basicStatState: COMPLETE colStatState: COMPLETE
explain select locid from loc_orc_n4 where locid>0 and year='2001';
explain select locid,year from loc_orc_n4 where locid>0 and year='2001';
explain select * from (select locid,year from loc_orc_n4) test where locid>0 and year='2001';
