--! qt:dataset:src1
--! qt:dataset:src
set hive.stats.deserialization.factor=1.0;
set hive.mapred.mode=nonstrict;
set hive.optimize.skewjoin = true;
set hive.skewjoin.key = 4;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=50;

-- This is to test runtime skew join together with map join

CREATE TABLE T1_n94(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n94;

EXPLAIN
SELECT COUNT(*) FROM
  (SELECT src1.key,src1.value FROM src src1 JOIN src src2 ON src1.key=src2.key) a
JOIN
  (SELECT src.key,src.value FROM src JOIN T1_n94 ON src.key=T1_n94.key) b
ON a.key=b.key;

SELECT COUNT(*) FROM
  (SELECT src1.key,src1.value FROM src src1 JOIN src src2 ON src1.key=src2.key) a
JOIN
  (SELECT src.key,src.value FROM src JOIN T1_n94 ON src.key=T1_n94.key) b
ON a.key=b.key;
