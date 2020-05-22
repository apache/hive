set hive.stats.column.autogather=true;
SET hive.vectorized.execution.enabled=true;
set hive.explain.user=false;

CREATE TABLE orders (key2 int, dt timestamp) stored as ORC;
--
INSERT INTO orders values(104, '2002-02-30 00:00:00');
INSERT INTO orders values(108, '2003-03-30 00:00:00');
INSERT INTO orders values( 99, '2004-04-30 00:00:00');
INSERT INTO orders values(109, '2005-05-30 00:00:00');
INSERT INTO orders values(110, '2006-06-30 00:00:00');
--
SET hive.optimize.scan.probedecode=true;
--
-- set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecTezSummaryPrinter;

-- EXPLAIN VECTORIZATION DETAIL
-- SELECT * from orders where orders.key2 <> 100;

SELECT * from orders where orders.key2 < 100;
