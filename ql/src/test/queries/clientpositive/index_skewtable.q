set hive.mapred.mode=nonstrict;
-- Test creating an index on skewed table

-- Create a skew table
CREATE TABLE kv(key STRING, value STRING) SKEWED BY (key) ON ((3), (8)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE kv;

-- Create and build an index
CREATE INDEX kv_index ON TABLE kv(value) AS 'COMPACT' WITH DEFERRED REBUILD;
DESCRIBE FORMATTED default__kv_kv_index__;
ALTER INDEX kv_index ON kv REBUILD;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.index.filter=true;
SET hive.optimize.index.filter.compact.minsize=0;

-- Run a query that uses the index
EXPLAIN SELECT * FROM kv WHERE value > '15' ORDER BY value;
SELECT * FROM kv WHERE value > '15' ORDER BY value;

DROP INDEX kv_index ON kv;
DROP TABLE kv;
