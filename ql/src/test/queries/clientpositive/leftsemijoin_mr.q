CREATE TABLE T1_n43(key INT);
LOAD DATA LOCAL INPATH '../../data/files/leftsemijoin_mr_t1.txt' INTO TABLE T1_n43;
CREATE TABLE T2_n27(key INT);
LOAD DATA LOCAL INPATH '../../data/files/leftsemijoin_mr_t2.txt' INTO TABLE T2_n27;

-- Run this query using TestMinimrCliDriver

SELECT * FROM T1_n43;
SELECT * FROM T2_n27;

set hive.auto.convert.join=false;
set mapred.reduce.tasks=2;

set hive.join.emit.interval=100;

SELECT T1_n43.key FROM T1_n43 LEFT SEMI JOIN (SELECT key FROM T2_n27 SORT BY key) tmp ON (T1_n43.key=tmp.key);

set hive.join.emit.interval=1;

SELECT T1_n43.key FROM T1_n43 LEFT SEMI JOIN (SELECT key FROM T2_n27 SORT BY key) tmp ON (T1_n43.key=tmp.key);
