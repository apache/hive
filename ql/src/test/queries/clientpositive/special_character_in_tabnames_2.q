set hive.cbo.enable=true;

-- try the query without indexing, with manual indexing, and with automatic indexing
-- SORT_QUERY_RESULTS

DROP TABLE IF EXISTS `s/c`;

CREATE TABLE `s/c` (key STRING COMMENT 'default', value STRING COMMENT 'default') STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "../../data/files/kv1.txt" INTO TABLE `s/c`;

ANALYZE TABLE `s/c` COMPUTE STATISTICS;

ANALYZE TABLE `s/c` COMPUTE STATISTICS FOR COLUMNS key,value;

SELECT key, value FROM `s/c` WHERE key > 80 AND key < 100;

EXPLAIN SELECT key, value FROM `s/c` WHERE key > 80 AND key < 100;
SELECT key, value FROM `s/c` WHERE key > 80 AND key < 100;
