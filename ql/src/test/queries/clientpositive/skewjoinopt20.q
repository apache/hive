set hive.mapred.mode=nonstrict;
set hive.optimize.skewjoin.compiletime = true;

CREATE TABLE T1_n103(key STRING, val STRING)
CLUSTERED BY (key) SORTED BY (key) INTO 4 BUCKETS
SKEWED BY (key) ON ((2)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/bucket_files/000000_0' INTO TABLE T1_n103;

CREATE TABLE T2_n65(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2_n65;

-- add a test where the skewed key is also the bucketized/sorted key
-- it should not matter, and the compile time skewed join
-- optimization is performed
-- adding a order by at the end to make the results deterministic

EXPLAIN
SELECT a.*, b.* FROM T1_n103 a JOIN T2_n65 b ON a.key = b.key;

SELECT a.*, b.* FROM T1_n103 a JOIN T2_n65 b ON a.key = b.key
ORDER BY a.key, b.key, a.val, b.val;
