CREATE TABLE date_table (date_string string);
INSERT INTO date_table VALUES ('2023-09-13'), ('2023-09-14'), ('2023-09-15');
-- Task conversion is disabled to ensure/test that session configurations take effect
-- when tasks are not run inside HS2 but in Tez, MapReduce, etc.
set hive.fetch.task.conversion=none;
set hive.datetime.formatter=SIMPLE;

set hive.local.time.zone=Asia/Bangkok;
SELECT date_format(date_string, 'u z') FROM date_table;

set hive.local.time.zone=Australia/Sydney;
SELECT date_format(date_string, 'u z') FROM date_table;

set hive.local.time.zone=Africa/Johannesburg;
SELECT date_format(date_string, 'u z') FROM date_table;

set hive.datetime.formatter=DATETIME;

set hive.local.time.zone=Asia/Bangkok;
SELECT date_format(date_string, 'u z') FROM date_table;

set hive.local.time.zone=Australia/Sydney;
SELECT date_format(date_string, 'u z') FROM date_table;

set hive.local.time.zone=Africa/Johannesburg;
SELECT date_format(date_string, 'u z') FROM date_table;
