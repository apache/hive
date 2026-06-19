set hive.stats.column.autogather=false;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=1000000000;
SET hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.mapjoin.native.fast.hashtable.enabled=true;
set hive.fetch.task.conversion=none;
SET mapred.min.split.size=1000;
SET mapred.max.split.size=5000;

CREATE TABLE item_dim (key1 int, name string) stored as ORC;
CREATE TABLE orders_fact (nokey int, key2 int, dt timestamp) stored as ORC;

INSERT INTO item_dim values(101, "Item 101");
INSERT INTO item_dim values(102, "Item 102");

INSERT INTO orders_fact values(12345, 101, '2001-01-30 00:00:00');
INSERT INTO orders_fact values(23456, 104, '2002-02-30 00:00:00');
INSERT INTO orders_fact values(34567, 108, '2003-03-30 00:00:00');
INSERT INTO orders_fact values(45678, 102, '2004-04-30 00:00:00');
INSERT INTO orders_fact values(56789, 109, '2005-05-30 00:00:00');
INSERT INTO orders_fact values(67891, 110, '2006-06-30 00:00:00');

SET hive.optimize.scan.probedecode=true;

EXPLAIN VECTORIZATION DETAIL select key1, key2, name, dt from orders_fact join item_dim on (orders_fact.key2 = item_dim.key1);

-- two keys match, the remaining rows can be skipped
select key1, key2, name, dt from orders_fact join item_dim on (orders_fact.key2 = item_dim.key1);
