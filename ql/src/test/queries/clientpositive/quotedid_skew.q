set hive.mapred.mode=nonstrict;

set hive.support.quoted.identifiers=column;

set hive.optimize.skewjoin.compiletime = true;

CREATE TABLE T1_n46(`!@#$%^&*()_q` string, `y&y` string)
SKEWED BY (`!@#$%^&*()_q`) ON ((2)) STORED AS TEXTFILE
;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n46;

CREATE TABLE T2_n28(`!@#$%^&*()_q` string, `y&y` string)
SKEWED BY (`!@#$%^&*()_q`) ON ((2)) STORED AS TEXTFILE
;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T2_n28;

-- a simple join query with skew on both the tables on the join key
-- adding a order by at the end to make the results deterministic

EXPLAIN
SELECT a.*, b.* FROM T1_n46 a JOIN T2_n28 b ON a. `!@#$%^&*()_q`  = b. `!@#$%^&*()_q` 
;

