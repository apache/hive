-- Simple static filter attempt
SET hive.stats.column.autogather=true;
SET hive.vectorized.execution.enabled=true;
SET hive.explain.user=false;

SET hive.optimize.scan.probedecode=true;

DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS orders_all;
--
dfs -mkdir ${system:test.tmp.dir}/orc_split;
dfs -cp /Users/panosgarefalakis/IdeaProjects/hive/data/files/orc_split_compressed ${system:test.tmp.dir}/orc_split/;
--
CREATE TABLE orders (key2 int, stringval string, doubleval double, decival decimal(38,0), tsval timestamp) stored as orc;
--
CREATE EXTERNAL TABLE orders_all (key2 int, stringval string, doubleval double, decival decimal(38,0), tsval timestamp)
stored as orc
LOCATION '${system:test.tmp.dir}/orc_split';

FROM orders_all ss
INSERT OVERWRITE TABLE orders
SELECT *;
--
-- -- Distinct keys -> Cardinality (25k rows)
-- -- 5 -> 1
-- -- 13 -> 1
-- -- 29 -> 1
-- -- 2 -> 1
-- -- 70 -> 1
-- -- 100 -> 24995
--
-- set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecTezSummaryPrinter;

EXPLAIN VECTORIZATION DETAIL
SELECT * from orders where orders.key2 <> 100;
-- 
EXPLAIN VECTORIZATION DETAIL SELECT key2, doubleval, stringval, tsval
FROM orders
WHERE key2 + 1 <> bround(101.1);
--
EXPLAIN VECTORIZATION SELECT SUM(key2 * doubleval)
FROM orders
WHERE key2 <> 100;
