set hive.stats.fetch.column.stats=true;

create table if not exists loc_staging_n3 (
  state string,
  locid int,
  zip bigint,
  year int
) row format delimited fields terminated by '|' stored as textfile;

create table loc_orc_n3 like loc_staging_n3;
alter table loc_orc_n3 set fileformat orc;

load data local inpath '../../data/files/loc.txt' overwrite into table loc_staging_n3;

insert overwrite table loc_orc_n3 select * from loc_staging_n3;

analyze table loc_orc_n3 compute statistics for columns state,locid,zip,year;

-- numRows: 8 rawDataSize: 688
explain select state from loc_orc_n3;

-- numRows: 16 rawDataSize: 1376
explain select * from (select state from loc_orc_n3 union all select state from loc_orc_n3) tmp;

-- numRows: 8 rawDataSize: 796
explain select * from loc_orc_n3;

-- numRows: 16 rawDataSize: 1592
explain select * from (select * from loc_orc_n3 union all select * from loc_orc_n3) tmp;

create database test;
use test;
create table if not exists loc_staging_n3 (
  state string,
  locid int,
  zip bigint,
  year int
) row format delimited fields terminated by '|' stored as textfile;

create table loc_orc_n3 like loc_staging_n3;
alter table loc_orc_n3 set fileformat orc;

load data local inpath '../../data/files/loc.txt' overwrite into table loc_staging_n3;

insert overwrite table loc_orc_n3 select * from loc_staging_n3;

analyze table loc_staging_n3 compute statistics;
analyze table loc_staging_n3 compute statistics for columns state,locid,zip,year;
analyze table loc_orc_n3 compute statistics for columns state,locid,zip,year;

-- numRows: 16 rawDataSize: 1376
explain select * from (select state from default.loc_orc_n3 union all select state from test.loc_orc_n3) temp;

-- numRows: 16 rawDataSize: 1376
explain select * from (select state from test.loc_staging_n3 union all select state from test.loc_orc_n3) temp;
