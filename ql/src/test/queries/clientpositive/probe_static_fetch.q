-- Simple static filter with fetch task
set hive.stats.column.autogather=true;
SET hive.vectorized.execution.enabled=true;
set hive.explain.user=false;

SET hive.optimize.scan.probedecode=true;

--
drop table if exists orders_all;
CREATE TABLE orders_all (key2 int, stringval string, doubleval double, decival decimal(38,0), tsval timestamp) stored as orc;

LOAD DATA local inpath '../../data/files/orc_split_elim.orc' INTO table orders_all;

-- -- Distinct keys -> Cardinality (25k rows)
-- -- 5 -> 1
-- -- 13 -> 1
-- -- 29 -> 1
-- -- 2 -> 1
-- -- 70 -> 1
-- -- 100 -> 24995

EXPLAIN VECTORIZATION DETAIL
SELECT * from orders_all where orders_all.key2 <> 100;

SELECT * from orders_all where orders_all.key2 <> 100;
