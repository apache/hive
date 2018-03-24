set hive.fetch.task.conversion=more;


drop table tstz1;

create table tstz1(`__time` timestamp with local time zone, n string, v integer)
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES ("druid.segment.granularity" = "HOUR");

insert into table tstz1
values(cast('2016-01-03 12:26:34 America/Los_Angeles' as timestamp with local time zone), 'Bill', 10);

EXPLAIN select `__time` from tstz1;
select `__time` from tstz1;

EXPLAIN select cast(`__time` as timestamp) from tstz1;
select cast(`__time` as timestamp) from tstz1;

EXPLAIN select cast(`__time` as timestamp) from tstz1 where `__time` >= cast('2016-01-03 12:26:34 America/Los_Angeles' as timestamp with local time zone);
select cast(`__time` as timestamp) from tstz1 where `__time` >= cast('2016-01-03 12:26:34 America/Los_Angeles' as timestamp with local time zone);

EXPLAIN SELECT EXTRACT(HOUR FROM CAST(`__time` AS timestamp)) FROM tstz1;
SELECT EXTRACT(HOUR FROM CAST(`__time` AS timestamp)) FROM tstz1;

EXPLAIN SELECT FLOOR(CAST(`__time` AS timestamp) to HOUR) FROM tstz1;
SELECT FLOOR(CAST(`__time` AS timestamp) to HOUR) FROM tstz1;


set time zone UTC;
EXPLAIN select `__time` from tstz1;
select `__time` from tstz1;
EXPLAIN select cast(`__time` as timestamp) from tstz1;
select cast(`__time` as timestamp) from tstz1;
EXPLAIN select cast(`__time` as timestamp) from tstz1 where `__time` >= cast('2016-01-03 12:26:34 America/Los_Angeles' as timestamp with local time zone);
select cast(`__time` as timestamp) from tstz1 where `__time` >= cast('2016-01-03 12:26:34 America/Los_Angeles' as timestamp with local time zone);

-- THIS is failing explore why
--EXPLAIN select cast(`__time` as timestamp) from tstz1 where `__time` = cast('2016-01-03 12:26:34 America/Los_Angeles' as timestamp with local time zone);
--select cast(`__time` as timestamp) from tstz1 where `__time` = cast('2016-01-03 12:26:34 America/Los_Angeles' as timestamp with local time zone);

EXPLAIN select cast(`__time` as timestamp) from tstz1 where `__time` >= cast('2016-01-03 20:26:34' as timestamp);
select cast(`__time` as timestamp) from tstz1 where `__time` >= cast('2016-01-03 20:26:34' as timestamp);

EXPLAIN select cast(`__time` as timestamp) from tstz1 where `__time` >= cast('2016-01-03 12:26:34 America/Los_Angeles' as timestamp with local time zone) AND `__time` <= cast('2016-01-03 12:26:34 America/Los_Angeles' as timestamp with local time zone);
select cast(`__time` as timestamp) from tstz1 where `__time` >= cast('2016-01-03 12:26:34 America/Los_Angeles' as timestamp with local time zone) AND `__time` <= cast('2016-01-03 12:26:34 America/Los_Angeles' as timestamp with local time zone);

EXPLAIN SELECT EXTRACT(HOUR FROM CAST(`__time` AS timestamp)) FROM tstz1;
SELECT EXTRACT(HOUR FROM CAST(`__time` AS timestamp)) FROM tstz1;

EXPLAIN  SELECT FLOOR(CAST(`__time` AS timestamp) to HOUR) FROM tstz1;
SELECT FLOOR(CAST(`__time` AS timestamp) to HOUR) FROM tstz1;
