set hive.stats.fetch.column.stats=true;

create table if not exists loc_staging_n5 (
  state string,
  locid int,
  zip bigint,
  year int
) row format delimited fields terminated by '|' stored as textfile;

create table loc_orc_n5 like loc_staging_n5;
alter table loc_orc_n5 set fileformat orc;

load data local inpath '../../data/files/loc.txt' overwrite into table loc_staging_n5;

insert overwrite table loc_orc_n5 select * from loc_staging_n5;

analyze table loc_orc_n5 compute statistics for columns state, locid, zip, year;

-- numRows: 8 rawDataSize: 796
explain select * from loc_orc_n5;

-- numRows: 4 rawDataSize: 396
explain select * from loc_orc_n5 limit 4;

-- greater than the available number of rows
-- numRows: 8 rawDataSize: 796
explain select * from loc_orc_n5 limit 16;

-- numRows: 0 rawDataSize: 0
explain select * from loc_orc_n5 limit 0;
