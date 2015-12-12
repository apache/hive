set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS
-- try the query without indexing, with manual indexing, and with automatic indexing

-- without indexing
EXPLAIN SELECT a.key, a.value FROM src a JOIN srcpart b ON (a.key = b.key) WHERE a.key > 80 AND a.key < 100 AND b.key > 70 AND b.key < 90;
SELECT a.key, a.value FROM src a JOIN srcpart b ON (a.key = b.key) WHERE a.key > 80 AND a.key < 100 AND b.key > 70 AND b.key < 90;

set hive.stats.dbclass=fs;

CREATE INDEX src_index_compact ON TABLE src(key) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX src_index_compact ON src REBUILD;

CREATE INDEX srcpart_index_compact ON TABLE srcpart(key) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX srcpart_index_compact ON srcpart REBUILD;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.index.filter=true;
SET hive.optimize.index.filter.compact.minsize=0;

-- automatic indexing
EXPLAIN SELECT a.key, a.value FROM src a JOIN srcpart b ON (a.key = b.key) WHERE a.key > 80 AND a.key < 100 AND b.key > 70 AND b.key < 90;
SELECT a.key, a.value FROM src a JOIN srcpart b ON (a.key = b.key) WHERE a.key > 80 AND a.key < 100 AND b.key > 70 AND b.key < 90;

DROP INDEX src_index_compact on src;
DROP INDEX srcpart_index_compact on src;
