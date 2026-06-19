set hive.mapred.mode=nonstrict;
set hive.optimize.skewjoin.compiletime = true;

CREATE TABLE tmpT1_n1(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE tmpT1_n1;

-- testing skew on other data types - int
CREATE TABLE T1_n160(key INT, val STRING) SKEWED BY (key) ON ((2));
INSERT OVERWRITE TABLE T1_n160 SELECT key, val FROM tmpT1_n1;

-- Tke skewed column is same in both the tables, however it is
-- INT in one of the tables, and STRING in the other table

CREATE TABLE T2_n94(key STRING, val STRING)
SKEWED BY (key) ON ((3)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2_n94;

-- Once HIVE-3445 is fixed, the compile time skew join optimization would be
-- applicable here. Till the above jira is fixed, it would be performed as a
-- regular join
-- adding a order by at the end to make the results deterministic

EXPLAIN
SELECT a.*, b.* FROM T1_n160 a JOIN T2_n94 b ON a.key = b.key;

SELECT a.*, b.* FROM T1_n160 a JOIN T2_n94 b ON a.key = b.key
ORDER BY a.key, b.key, a.val, b.val;
