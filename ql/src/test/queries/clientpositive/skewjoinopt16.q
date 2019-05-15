set hive.mapred.mode=nonstrict;
set hive.optimize.skewjoin.compiletime = true;

CREATE TABLE T1_n154(key STRING, val STRING)
SKEWED BY (key, val) ON ((2, 12)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n154;

CREATE TABLE T2_n90(key STRING, val STRING)
SKEWED BY (key) ON ((3)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2_n90;

-- One of the tables is skewed by 2 columns, and the other table is
-- skewed by one column. Ths join is performed on the both the columns
-- adding a order by at the end to make the results deterministic

EXPLAIN
SELECT a.*, b.* FROM T1_n154 a JOIN T2_n90 b ON a.key = b.key and a.val = b.val;

SELECT a.*, b.* FROM T1_n154 a JOIN T2_n90 b ON a.key = b.key and a.val = b.val
ORDER BY a.key, b.key, a.val, b.val;
