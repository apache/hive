--! qt:dataset:src
--! qt:dataset:alltypesorc

-- Note that ds_freq_sketch(..., maxSize) calls are not vectorized, independently of the value of
-- 'hive.vectorized.execution.enabled' because the current framework handles single param function calls only;
-- here we are adding the ds_kll_sketch(..., 200) version to be able to check if the two sketches are the same,
-- since the default k value is set to 200 in HiveConfig;
--
--set hive.vectorized.execution.enabled=false;
--select ds_freq_frequent_items(ds_freq_sketch(cstring1), 2) from alltypesorc;

--set hive.metastore.partition.name.whitelist.pattern=null;
set metastore.stats.fetch.bitvector=true;
set hive.stats.freq.items.enable=true;

-- non-partitioned table (to check table statistics)
CREATE TABLE test (a string, b float);

INSERT INTO test VALUES ("a", 1);
INSERT INTO test VALUES ("b", 2);
INSERT INTO test VALUES ("b", 2);
INSERT INTO test VALUES ("b", 2);
INSERT INTO test VALUES ("c", 2);
INSERT INTO test VALUES ("c", 2);
INSERT INTO test VALUES ("c", 2);
INSERT INTO test VALUES ("c", 2);
INSERT INTO test VALUES ("c", 2);
INSERT INTO test VALUES ("c", 2);
INSERT INTO test VALUES ("c", 2);
INSERT INTO test VALUES ("d", 3);
INSERT INTO test VALUES ("e", null);
INSERT INTO test VALUES ("abcdefghijklmnopqrstuvwxyz_12345", 1);

ANALYZE TABLE test COMPUTE STATISTICS FOR COLUMNS;

ANALYZE TABLE test COMPUTE STATISTICS;

DESCRIBE EXTENDED test;

DESCRIBE FORMATTED test a;

-- partitioned table (to check table statistics)
CREATE TABLE test2 (b float, c string) partitioned by (a string);
INSERT INTO test2 VALUES (1,"A", "a" );
INSERT INTO test2 VALUES (2,"B", "a" );
INSERT INTO test2 VALUES (2,"C", "a" );
INSERT INTO test2 VALUES (2,"B", "b" );
INSERT INTO test2 VALUES (2,"B", "b" );
INSERT INTO test2 VALUES (2,"B", "b" );
INSERT INTO test2 VALUES (2,"L", "c" );
INSERT INTO test2 VALUES (2,"M", "c" );
INSERT INTO test2 VALUES (2,"N", "c" );
INSERT INTO test2 VALUES (2,"O", "c" );
INSERT INTO test2 VALUES (2,"P", "c" );
INSERT INTO test2 VALUES (2,"Q", "c" );
INSERT INTO test2 VALUES (2,"R", "c" );
INSERT INTO test2 VALUES (2,"S", "c" );
INSERT INTO test2 VALUES (2,"T", "c" );
INSERT INTO test2 VALUES (2,"U", "c" );
INSERT INTO test2 VALUES (2,"V", "c" );
INSERT INTO test2 VALUES (2,"W", "c" );
INSERT INTO test2 VALUES (2,"X", "c" );
INSERT INTO test2 VALUES (2,"Y", "c" );
INSERT INTO test2 VALUES (2,"Z", "c" );
INSERT INTO test2 VALUES (1, "abcdefghijklmnopqrstuvwxyz_12345", "a");
INSERT INTO test2 VALUES (2, "abcdefghijklmnopqrstuvwxyz_12345", "a");

ANALYZE TABLE test2 COMPUTE STATISTICS FOR COLUMNS;

ANALYZE TABLE test2 COMPUTE STATISTICS;

DESCRIBE EXTENDED test2;

DESCRIBE FORMATTED test2 partition(a='a') c;

DESCRIBE FORMATTED test2 partition(a='c') c;

DESCRIBE FORMATTED test2 c;