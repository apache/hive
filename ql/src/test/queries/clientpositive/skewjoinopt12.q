set hive.mapred.mode=nonstrict;
set hive.optimize.skewjoin.compiletime = true;

CREATE TABLE T1_n159(key STRING, val STRING)
SKEWED BY (key, val) ON ((2, 12), (8, 18)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n159;

CREATE TABLE T2_n93(key STRING, val STRING)
SKEWED BY (key, val) ON ((3, 13), (8, 18)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2_n93;

-- Both the join tables are skewed by 2 keys, and one of the skewed values
-- is common to both the tables. The join key matches the skewed key set.
-- adding a order by at the end to make the results deterministic

EXPLAIN
SELECT a.*, b.* FROM T1_n159 a JOIN T2_n93 b ON a.key = b.key and a.val = b.val;

SELECT a.*, b.* FROM T1_n159 a JOIN T2_n93 b ON a.key = b.key and a.val = b.val
ORDER BY a.key, b.key, a.val, b.val;
