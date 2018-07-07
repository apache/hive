set hive.fetch.task.conversion=more;
SET hive.ctas.external.tables=true;
SET hive.external.table.purge.default = true;

drop table tstz1_n0;

create external table tstz1_n0(`__time` timestamp with local time zone, n string, v integer)
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES ("druid.segment.granularity" = "HOUR");

insert into table tstz1_n0
values(cast('2016-01-03 12:26:34 America/Los_Angeles' as timestamp with local time zone), 'Bill', 10);

EXPLAIN select `__time` from tstz1_n0;
select `__time` from tstz1_n0;

EXPLAIN select cast(`__time` as timestamp) from tstz1_n0;
select cast(`__time` as timestamp) from tstz1_n0;

EXPLAIN select cast(`__time` as timestamp) from tstz1_n0 where `__time` >= cast('2016-01-03 12:26:34 America/Los_Angeles' as timestamp with local time zone);
select cast(`__time` as timestamp) from tstz1_n0 where `__time` >= cast('2016-01-03 12:26:34 America/Los_Angeles' as timestamp with local time zone);

EXPLAIN SELECT EXTRACT(HOUR FROM CAST(`__time` AS timestamp)) FROM tstz1_n0;
SELECT EXTRACT(HOUR FROM CAST(`__time` AS timestamp)) FROM tstz1_n0;

EXPLAIN SELECT FLOOR(CAST(`__time` AS timestamp) to HOUR) FROM tstz1_n0;
SELECT FLOOR(CAST(`__time` AS timestamp) to HOUR) FROM tstz1_n0;


set time zone UTC;
EXPLAIN select `__time` from tstz1_n0;
select `__time` from tstz1_n0;
EXPLAIN select cast(`__time` as timestamp) from tstz1_n0;
select cast(`__time` as timestamp) from tstz1_n0;
EXPLAIN select cast(`__time` as timestamp) from tstz1_n0 where `__time` >= cast('2016-01-03 12:26:34 America/Los_Angeles' as timestamp with local time zone);
select cast(`__time` as timestamp) from tstz1_n0 where `__time` >= cast('2016-01-03 12:26:34 America/Los_Angeles' as timestamp with local time zone);

-- THIS is failing explore why
--EXPLAIN select cast(`__time` as timestamp) from tstz1_n0 where `__time` = cast('2016-01-03 12:26:34 America/Los_Angeles' as timestamp with local time zone);
--select cast(`__time` as timestamp) from tstz1_n0 where `__time` = cast('2016-01-03 12:26:34 America/Los_Angeles' as timestamp with local time zone);

EXPLAIN select cast(`__time` as timestamp) from tstz1_n0 where `__time` >= cast('2016-01-03 20:26:34' as timestamp);
select cast(`__time` as timestamp) from tstz1_n0 where `__time` >= cast('2016-01-03 20:26:34' as timestamp);

EXPLAIN select cast(`__time` as timestamp) from tstz1_n0 where `__time` >= cast('2016-01-03 12:26:34 America/Los_Angeles' as timestamp with local time zone) AND `__time` <= cast('2016-01-03 12:26:34 America/Los_Angeles' as timestamp with local time zone);
select cast(`__time` as timestamp) from tstz1_n0 where `__time` >= cast('2016-01-03 12:26:34 America/Los_Angeles' as timestamp with local time zone) AND `__time` <= cast('2016-01-03 12:26:34 America/Los_Angeles' as timestamp with local time zone);

EXPLAIN SELECT EXTRACT(HOUR FROM CAST(`__time` AS timestamp)) FROM tstz1_n0;
SELECT EXTRACT(HOUR FROM CAST(`__time` AS timestamp)) FROM tstz1_n0;

EXPLAIN  SELECT FLOOR(CAST(`__time` AS timestamp) to HOUR) FROM tstz1_n0;
SELECT FLOOR(CAST(`__time` AS timestamp) to HOUR) FROM tstz1_n0;
