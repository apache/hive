set hive.compute.query.using.stats=false;
set hive.mapred.mode=nonstrict;
set mapred.max.split.size = 32000000;

CREATE TABLE T1(name STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' INTO TABLE T1;

CREATE TABLE T2(name STRING) STORED AS SEQUENCEFILE;

INSERT OVERWRITE TABLE T2 SELECT * FROM (
SELECT tmp1.name as name FROM (
  SELECT name, 'MMM' AS n FROM T1) tmp1 
  JOIN (SELECT 'MMM' AS n FROM T1) tmp2
  JOIN (SELECT 'MMM' AS n FROM T1) tmp3
  ON tmp1.n = tmp2.n AND tmp1.n = tmp3.n) ttt LIMIT 5000000;

CREATE TABLE T3(name STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' INTO TABLE T3;
LOAD DATA LOCAL INPATH '../../data/files/kv2.txt' INTO TABLE T3;

set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecutePrinter,org.apache.hadoop.hive.ql.hooks.ShowMapredStatsHook;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

-- 2 split by max.split.size
SELECT COUNT(1) FROM T2;

-- 1 split for two file
SELECT COUNT(1) FROM T3;

set hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

-- 1 split
SELECT COUNT(1) FROM T2;

-- 2 split for two file
SELECT COUNT(1) FROM T3;

