set hive.mapred.mode=nonstrict;
set hive.optimize.skewjoin.compiletime = true;

CREATE TABLE T1_n27(key STRING, val STRING)
SKEWED BY (key, val) ON ((2, 12)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n27;

CREATE TABLE T2_n18(key STRING, val STRING)
SKEWED BY (key) ON ((2)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2_n18;

-- One of the tables is skewed by 2 columns, and the other table is
-- skewed by one column. Ths join is performed on the first skewed column
-- The skewed value for the jon key is common to both the tables.
-- In this case, the skewed join value is not repeated in the filter.
-- adding a order by at the end to make the results deterministic

EXPLAIN
SELECT a.*, b.* FROM T1_n27 a JOIN T2_n18 b ON a.key = b.key;

SELECT a.*, b.* FROM T1_n27 a JOIN T2_n18 b ON a.key = b.key
ORDER BY a.key, b.key, a.val, b.val;

DROP TABLE T1_n27;
DROP TABLE T2_n18;


CREATE TABLE T1_n27(key STRING, val STRING)
SKEWED BY (key, val) ON ((2, 12)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n27;

CREATE TABLE T2_n18(key STRING, val STRING)
SKEWED BY (key) ON ((2)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2_n18;

-- One of the tables is skewed by 2 columns, and the other table is
-- skewed by one column. Ths join is performed on the both the columns
-- In this case, the skewed join value is repeated in the filter.

EXPLAIN
SELECT a.*, b.* FROM T1_n27 a JOIN T2_n18 b ON a.key = b.key and a.val = b.val;

SELECT a.*, b.* FROM T1_n27 a JOIN T2_n18 b ON a.key = b.key and a.val = b.val
ORDER BY a.key, b.key, a.val, b.val;
