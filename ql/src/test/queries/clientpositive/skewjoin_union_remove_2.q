set hive.mapred.mode=nonstrict;
set hive.optimize.skewjoin.compiletime = true;

set hive.stats.autogather=false;
set hive.optimize.union.remove=true;

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;

CREATE TABLE T1_n8(key STRING, val STRING)
SKEWED BY (key) ON ((2), (8)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n8;

CREATE TABLE T2_n4(key STRING, val STRING)
SKEWED BY (key) ON ((3), (8)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2_n4;

CREATE TABLE T3_n2(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T3.txt' INTO TABLE T3_n2;

-- This is to test the union->selectstar->filesink and skewjoin optimization
-- Union of 3 map-reduce subqueries is performed for the skew join
-- There is no need to write the temporary results of the sub-queries, and then read them 
-- again to process the union. The union can be removed completely.
-- Since this test creates sub-directories for the output table, it might be easier
-- to run the test only on hadoop 23

EXPLAIN
SELECT a.*, b.*, c.* FROM T1_n8 a JOIN T2_n4 b ON a.key = b.key JOIN T3_n2 c on a.key = c.key;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

SELECT a.*, b.*, c.* FROM T1_n8 a JOIN T2_n4 b ON a.key = b.key JOIN T3_n2 c on a.key = c.key
ORDER BY a.key, b.key, c.key, a.val, b.val, c.val;
