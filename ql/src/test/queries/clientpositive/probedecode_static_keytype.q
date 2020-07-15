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

SET hive.optimize.scan.probedecode=true;


CREATE TABLE item_dim_dt (key1 DATE, name string) stored as ORC;
CREATE TABLE orders_fact_dt (nokey int, key2 DATE, dt timestamp) stored as ORC;

INSERT INTO item_dim_dt values('2001-01-30', "Item 101");
INSERT INTO item_dim_dt values('2002-01-30', "Item 102");

INSERT INTO orders_fact_dt values(12345, '2001-01-30', '2011-01-30 00:00:00');
INSERT INTO orders_fact_dt values(23456, '2004-01-30', '2014-02-30 00:00:00');
INSERT INTO orders_fact_dt values(34567, '2008-01-30', '2018-03-30 00:00:00');
INSERT INTO orders_fact_dt values(45678, '2002-01-30', '2012-04-30 00:00:00');
INSERT INTO orders_fact_dt values(56789, '2009-01-30', '2019-05-30 00:00:00');
INSERT INTO orders_fact_dt values(67891, '2010-01-30', '2020-06-30 00:00:00');

-- Reduce Sink Vectorization -> Expected className: VectorReduceSinkLongOperator
EXPLAIN VECTORIZATION DETAIL select key1, key2, name, dt from orders_fact_dt join item_dim_dt on (orders_fact_dt.key2 = item_dim_dt.key1);
-- two keys match, the remaining rows can be skipped
select key1, key2, name, dt from orders_fact_dt join item_dim_dt on (orders_fact_dt.key2 = item_dim_dt.key1);


CREATE TABLE item_dim_ts (key1 timestamp, name string) stored as ORC;
CREATE TABLE orders_fact_ts (nokey int, key2 timestamp, dt timestamp) stored as ORC;

INSERT INTO item_dim_ts values('2001-01-30 00:00:00', "Item 101");
INSERT INTO item_dim_ts values('2002-01-30 00:00:00', "Item 102");

INSERT INTO orders_fact_ts values(12345, '2001-01-30 00:00:00', '2011-01-30 00:00:00');
INSERT INTO orders_fact_ts values(23456, '2004-01-30 00:00:00', '2014-02-30 00:00:00');
INSERT INTO orders_fact_ts values(34567, '2008-01-30 00:00:00', '2018-03-30 00:00:00');
INSERT INTO orders_fact_ts values(45678, '2002-01-30 00:00:00', '2012-04-30 00:00:00');
INSERT INTO orders_fact_ts values(56789, '2009-01-30 00:00:00', '2019-05-30 00:00:00');
INSERT INTO orders_fact_ts values(67891, '2010-01-30 00:00:00', '2020-06-30 00:00:00');

-- Reduce Sink Vectorization -> Expected className: VectorReduceSinkMultiKeyOperator
EXPLAIN VECTORIZATION DETAIL select key1, key2, name, dt from orders_fact_ts join item_dim_ts on (orders_fact_ts.key2 = item_dim_ts.key1);

-- two keys match, the remaining rows can be skipped
select key1, key2, name, dt from orders_fact_ts join item_dim_ts on (orders_fact_ts.key2 = item_dim_ts.key1);
