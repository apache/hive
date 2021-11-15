--! qt:dataset:srcpart
--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
CREATE TEMPORARY TABLE tmp_src AS SELECT * FROM src WHERE key % 2 = 0;
CREATE TEMPORARY TABLE tmp_srcpart AS SELECT * FROM srcpart;

DESCRIBE tmp_src;
DESCRIBE tmp_srcpart;
SHOW TABLES LIKE "tmp_src%";

SELECT count(*) FROM tmp_src;
SELECT count(*) FROM tmp_srcpart;

EXPLAIN TRUNCATE TABLE tmp_src;
TRUNCATE TABLE tmp_src;

SELECT count(*) FROM tmp_src;

EXPLAIN TRUNCATE TABLE tmp_srcpart;
TRUNCATE TABLE tmp_srcpart;

SELECT count(*) FROM tmp_srcpart;
