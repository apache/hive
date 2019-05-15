set hive.mapred.mode=nonstrict;
set hive.optimize.skewjoin.compiletime = true;
set hive.auto.convert.join=true;

CREATE TABLE tmpT1_n0(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE tmpT1_n0;

-- testing skew on other data types - int
CREATE TABLE T1_n151(key INT, val STRING) SKEWED BY (key) ON ((2));
INSERT OVERWRITE TABLE T1_n151 SELECT key, val FROM tmpT1_n0;

CREATE TABLE tmpT2_n0(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE tmpT2_n0;

CREATE TABLE T2_n88(key INT, val STRING) SKEWED BY (key) ON ((3));

INSERT OVERWRITE TABLE T2_n88 SELECT key, val FROM tmpT2_n0;

-- copy from skewjoinopt15
-- test compile time skew join and auto map join
-- The skewed key is a integer column.
-- Otherwise this test is similar to skewjoinopt1.q
-- Both the joined tables are skewed, and the joined column
-- is an integer
-- adding a order by at the end to make the results deterministic

EXPLAIN
SELECT a.*, b.* FROM T1_n151 a JOIN T2_n88 b ON a.key = b.key;

SELECT a.*, b.* FROM T1_n151 a JOIN T2_n88 b ON a.key = b.key
ORDER BY a.key, b.key, a.val, b.val;

-- test outer joins also

EXPLAIN
SELECT a.*, b.* FROM T1_n151 a RIGHT OUTER JOIN T2_n88 b ON a.key = b.key;

SELECT a.*, b.* FROM T1_n151 a RIGHT OUTER JOIN T2_n88 b ON a.key = b.key
ORDER BY a.key, b.key, a.val, b.val;

-- an aggregation at the end should not change anything

EXPLAIN
SELECT count(1) FROM T1_n151 a JOIN T2_n88 b ON a.key = b.key;

SELECT count(1) FROM T1_n151 a JOIN T2_n88 b ON a.key = b.key;

EXPLAIN
SELECT count(1) FROM T1_n151 a RIGHT OUTER JOIN T2_n88 b ON a.key = b.key;

SELECT count(1) FROM T1_n151 a RIGHT OUTER JOIN T2_n88 b ON a.key = b.key;
