set hive.mapred.mode=nonstrict;
set hive.optimize.skewjoin.compiletime = true;

CREATE TABLE T1_n52(key STRING, val STRING)
SKEWED BY (key) ON ((2)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n52;

CREATE TABLE T2_n32(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2_n32;

-- only of the tables of the join (the left table of the join) is skewed
-- the skewed filter would still be applied to both the tables
-- adding a order by at the end to make the results deterministic

EXPLAIN
SELECT a.*, b.* FROM T1_n52 a JOIN T2_n32 b ON a.key = b.key;

SELECT a.*, b.* FROM T1_n52 a JOIN T2_n32 b ON a.key = b.key
ORDER BY a.key, b.key, a.val, b.val;

-- the order of the join should not matter, just confirming
EXPLAIN
SELECT a.*, b.* FROM T2_n32 a JOIN T1_n52 b ON a.key = b.key;

SELECT a.*, b.* FROM T2_n32 a JOIN T1_n52 b ON a.key = b.key
ORDER BY a.key, b.key, a.val, b.val;
