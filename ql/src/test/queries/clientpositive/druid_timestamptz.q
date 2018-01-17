set hive.fetch.task.conversion=more;

drop table tstz1;

create table tstz1(`__time` timestamp with local time zone, n string, v integer)
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES ("druid.segment.granularity" = "HOUR");

insert into table tstz1
values(cast('2016-01-03 12:26:34 America/Los_Angeles' as timestamp with local time zone), 'Bill', 10);

select `__time` from tstz1;
select cast(`__time` as timestamp) from tstz1;
select cast(`__time` as timestamp) from tstz1 where `__time` >= cast('2016-01-03 12:26:34 America/Los_Angeles' as timestamp with local time zone);

set time zone UTC;

select `__time` from tstz1;
select cast(`__time` as timestamp) from tstz1;
select cast(`__time` as timestamp) from tstz1 where `__time` >= cast('2016-01-03 12:26:34 America/Los_Angeles' as timestamp with local time zone);
