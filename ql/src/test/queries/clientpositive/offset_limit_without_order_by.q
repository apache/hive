--! qt:dataset:srcpart

-- Srcpart has 4 partitions(files) and each partition contains 500 rows
-- We disable Tez Grouping in order to emulate HIVE-27480
set tez.grouping.min-size=1;
set tez.grouping.max-size=1;

EXPLAIN SELECT substr(value, 0, 3) FROM srcpart LIMIT 5, 10;
SELECT substr(value, 0, 3) FROM srcpart LIMIT 5, 10;

EXPLAIN SELECT substr(value, 0, 3) FROM srcpart LIMIT 1990, 10;
SELECT substr(value, 0, 3) FROM srcpart LIMIT 1990, 10;

EXPLAIN SELECT substr(value, 0, 3) FROM srcpart LIMIT 1995, 10;
SELECT substr(value, 0, 3) FROM srcpart LIMIT 1995, 10;

EXPLAIN SELECT substr(value, 0, 3) FROM srcpart LIMIT 2000, 10;
SELECT substr(value, 0, 3) FROM srcpart LIMIT 2000, 10;

EXPLAIN SELECT count(*) FROM (SELECT * FROM srcpart LIMIT 5, 10) t;
SELECT count(*) FROM (SELECT * FROM srcpart LIMIT 5, 10) t;

EXPLAIN SELECT count(*) FROM (SELECT * FROM srcpart LIMIT 1990, 10) t;
SELECT count(*) FROM (SELECT * FROM srcpart LIMIT 1990, 10) t;

EXPLAIN SELECT count(*) FROM (SELECT * FROM srcpart LIMIT 1995, 10) t;
SELECT count(*) FROM (SELECT * FROM srcpart LIMIT 1995, 10) t;

EXPLAIN SELECT count(*) FROM (SELECT * FROM srcpart LIMIT 2000, 10) t;
SELECT count(*) FROM (SELECT * FROM srcpart LIMIT 2000, 10) t;


set hive.fetch.task.conversion=none;

EXPLAIN SELECT substr(value, 0, 3) FROM srcpart LIMIT 5, 10;
SELECT substr(value, 0, 3) FROM srcpart LIMIT 5, 10;

EXPLAIN SELECT substr(value, 0, 3) FROM srcpart LIMIT 1990, 10;
SELECT substr(value, 0, 3) FROM srcpart LIMIT 1990, 10;

EXPLAIN SELECT substr(value, 0, 3) FROM srcpart LIMIT 1995, 10;
SELECT substr(value, 0, 3) FROM srcpart LIMIT 1995, 10;

EXPLAIN SELECT substr(value, 0, 3) FROM srcpart LIMIT 2000, 10;
SELECT substr(value, 0, 3) FROM srcpart LIMIT 2000, 10;

EXPLAIN SELECT count(*) FROM (SELECT * FROM srcpart LIMIT 5, 10) t;
SELECT count(*) FROM (SELECT * FROM srcpart LIMIT 5, 10) t;

EXPLAIN SELECT count(*) FROM (SELECT * FROM srcpart LIMIT 1990, 10) t;
SELECT count(*) FROM (SELECT * FROM srcpart LIMIT 1990, 10) t;

EXPLAIN SELECT count(*) FROM (SELECT * FROM srcpart LIMIT 1995, 10) t;
SELECT count(*) FROM (SELECT * FROM srcpart LIMIT 1995, 10) t;

EXPLAIN SELECT count(*) FROM (SELECT * FROM srcpart LIMIT 2000, 10) t;
SELECT count(*) FROM (SELECT * FROM srcpart LIMIT 2000, 10) t;
