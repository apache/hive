set hive.mapred.mode=nonstrict;
set hive.optimize.skewjoin.compiletime = true;

CREATE TABLE T1_n100(key STRING, val STRING)
SKEWED BY (key, val) ON ((2, 12)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n100;

CREATE TABLE T2_n63(key STRING, val STRING)
SKEWED BY (key) ON ((3)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2_n63;

-- One of the tables is skewed by 2 columns, and the other table is
-- skewed by one column. Ths join is performed on the first skewed column
-- adding a order by at the end to make the results deterministic

EXPLAIN
SELECT a.*, b.* FROM T1_n100 a JOIN T2_n63 b ON a.key = b.key;

SELECT a.*, b.* FROM T1_n100 a JOIN T2_n63 b ON a.key = b.key
ORDER BY a.key, b.key, a.val, b.val;
