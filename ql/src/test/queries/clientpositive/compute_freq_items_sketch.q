--! qt:dataset:src
--! qt:dataset:alltypesorc

-- Note that ds_kll_sketch(..., k) calls are not vectorized, independently of the value of
-- 'hive.vectorized.execution.enabled' because the current framework handles single param function calls only;
-- here we are adding the ds_kll_sketch(..., 200) version to be able to check if the two sketches are the same,
-- since the default k value is set to 200 in HiveConfig;
--
--set hive.vectorized.execution.enabled=false;
--select ds_freq_frequent_items(ds_freq_sketch(cstring1), 2) from alltypesorc;

set hive.metastore.partition.name.whitelist.pattern=metastore.stats.fetch.bitvector;
set metastore.stats.fetch.bitvector=true;
set hive.stats.freq.items.enable=true;

-- non-partitioned table (to check table statistics)
CREATE TABLE test (a string, b float);

INSERT INTO test VALUES ("a", 1);
INSERT INTO test VALUES ("b", 2);
INSERT INTO test VALUES ("c", 2);
INSERT INTO test VALUES ("d", 3);
INSERT INTO test VALUES ("e", null);

ANALYZE TABLE test COMPUTE STATISTICS FOR COLUMNS;

ANALYZE TABLE test COMPUTE STATISTICS;

DESCRIBE EXTENDED test;

DESCRIBE FORMATTED test a;
