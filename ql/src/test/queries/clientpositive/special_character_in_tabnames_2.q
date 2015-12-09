set hive.cbo.enable=true;

-- try the query without indexing, with manual indexing, and with automatic indexing
-- SORT_QUERY_RESULTS

DROP TABLE IF EXISTS `s/c`;

CREATE TABLE `s/c` (key STRING COMMENT 'default', value STRING COMMENT 'default') STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "../../data/files/kv1.txt" INTO TABLE `s/c`;

ANALYZE TABLE `s/c` COMPUTE STATISTICS;

ANALYZE TABLE `s/c` COMPUTE STATISTICS FOR COLUMNS key,value;

-- without indexing
SELECT key, value FROM `s/c` WHERE key > 80 AND key < 100;

set hive.stats.dbclass=fs;
CREATE INDEX src_index ON TABLE `s/c`(key) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX src_index ON `s/c` REBUILD;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-- manual indexing
INSERT OVERWRITE DIRECTORY "${system:test.tmp.dir}/index_where" SELECT `_bucketname` ,  `_offsets` FROM `default__s/c_src_index__` WHERE key > 80 AND key < 100;
SET hive.index.compact.file=${system:test.tmp.dir}/index_where;
SET hive.optimize.index.filter=false;
SET hive.input.format=org.apache.hadoop.hive.ql.index.compact.HiveCompactIndexInputFormat;

EXPLAIN SELECT key, value FROM `s/c` WHERE key > 80 AND key < 100;
SELECT key, value FROM `s/c` WHERE key > 80 AND key < 100;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.index.filter=true;
SET hive.optimize.index.filter.compact.minsize=0;

-- automatic indexing
EXPLAIN SELECT key, value FROM `s/c` WHERE key > 80 AND key < 100;
SELECT key, value FROM `s/c` WHERE key > 80 AND key < 100;

DROP INDEX src_index on `s/c`;
