set hive.mapred.mode=nonstrict;
-- Test to ensure that an empty index result is propagated correctly

CREATE DATABASE it;
-- Create temp, and populate it with some values in src.
CREATE TABLE it.temp(key STRING, val STRING) STORED AS TEXTFILE;

set hive.stats.dbclass=fs;
-- Build an index on it.temp.
CREATE INDEX temp_index ON TABLE it.temp(key) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX temp_index ON it.temp REBUILD;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.index.filter=true;
SET hive.optimize.index.filter.compact.minsize=0;

-- query should not return any values
SELECT * FROM it.it__temp_temp_index__ WHERE key = 86;
EXPLAIN SELECT * FROM it.temp WHERE key  = 86;
SELECT * FROM it.temp WHERE key  = 86;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.index.filter=false;
DROP table it.temp;

DROP DATABASE it;
