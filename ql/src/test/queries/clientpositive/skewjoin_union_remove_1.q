set hive.mapred.mode=nonstrict;
set hive.optimize.skewjoin.compiletime = true;

set hive.stats.autogather=false;
set hive.optimize.union.remove=true;

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;

-- This is to test the union->selectstar->filesink and skewjoin optimization
-- Union of 2 map-reduce subqueries is performed for the skew join
-- There is no need to write the temporary results of the sub-queries, and then read them 
-- again to process the union. The union can be removed completely.
-- Since this test creates sub-directories for the output, it might be easier to run the test
-- only on hadoop 23

CREATE TABLE T1_n57(key STRING, val STRING)
SKEWED BY (key) ON ((2)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n57;

CREATE TABLE T2_n35(key STRING, val STRING)
SKEWED BY (key) ON ((3)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2_n35;

-- a simple join query with skew on both the tables on the join key

EXPLAIN
SELECT * FROM T1_n57 a JOIN T2_n35 b ON a.key = b.key;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

SELECT * FROM T1_n57 a JOIN T2_n35 b ON a.key = b.key
ORDER BY a.key, b.key, a.val, b.val;

-- test outer joins also

EXPLAIN
SELECT a.*, b.* FROM T1_n57 a RIGHT OUTER JOIN T2_n35 b ON a.key = b.key;

SELECT a.*, b.* FROM T1_n57 a RIGHT OUTER JOIN T2_n35 b ON a.key = b.key
ORDER BY a.key, b.key, a.val, b.val;

create table DEST1_n58(key1 STRING, val1 STRING, key2 STRING, val2 STRING);

EXPLAIN
INSERT OVERWRITE TABLE DEST1_n58
SELECT * FROM T1_n57 a JOIN T2_n35 b ON a.key = b.key;

INSERT OVERWRITE TABLE DEST1_n58
SELECT * FROM T1_n57 a JOIN T2_n35 b ON a.key = b.key;

SELECT * FROM DEST1_n58
ORDER BY key1, key2, val1, val2;

EXPLAIN
INSERT OVERWRITE TABLE DEST1_n58
SELECT * FROM T1_n57 a RIGHT OUTER JOIN T2_n35 b ON a.key = b.key;

INSERT OVERWRITE TABLE DEST1_n58
SELECT * FROM T1_n57 a RIGHT OUTER JOIN T2_n35 b ON a.key = b.key;

SELECT * FROM DEST1_n58
ORDER BY key1, key2, val1, val2;
