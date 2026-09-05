-- Parquet mirror of probedecode_mapjoin_simple.q.
--
-- Exercises the Parquet ProbeDecode path: the vectorizer wires the big-side
-- TableScan with a ProbeDecodeContext, VectorizedParquetRecordReader decodes
-- the join-key column first, probes the small-side hash table, and passes the
-- resulting ParquetProbeFilter down to the remaining columns' readBatch calls
-- so filtered rows skip decode / conversion.
--
-- The correctness bar for this test is that the join result must be identical
-- with and without hive.optimize.scan.probedecode enabled; the fast-path is
-- purely a performance optimisation (rows that would be filtered anyway are
-- read as nulls into the batch, then dropped via batch.selected[]).
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

CREATE TABLE item_dim_pq (key1 int, name string) stored as parquet;
CREATE TABLE orders_fact_pq (nokey int, key2 int, dt timestamp) stored as parquet;

INSERT INTO item_dim_pq values(101, "Item 101");
INSERT INTO item_dim_pq values(102, "Item 102");

INSERT INTO orders_fact_pq values(12345, 101, '2001-01-30 00:00:00');
INSERT INTO orders_fact_pq values(23456, 104, '2002-02-30 00:00:00');
INSERT INTO orders_fact_pq values(34567, 108, '2003-03-30 00:00:00');
INSERT INTO orders_fact_pq values(45678, 102, '2004-04-30 00:00:00');
INSERT INTO orders_fact_pq values(56789, 109, '2005-05-30 00:00:00');
INSERT INTO orders_fact_pq values(67891, 110, '2006-06-30 00:00:00');

-- Baseline: probedecode disabled. Result set is the reference for equivalence.
SET hive.optimize.scan.probedecode=false;

select key1, key2, name, dt from orders_fact_pq join item_dim_pq on (orders_fact_pq.key2 = item_dim_pq.key1)
order by key2;

-- Now enable probedecode. The plan should carry the ProbeDecodeContext on the
-- big-side (orders_fact_pq) TableScan; the join result must be identical.
SET hive.optimize.scan.probedecode=true;

EXPLAIN VECTORIZATION DETAIL
select key1, key2, name, dt from orders_fact_pq join item_dim_pq on (orders_fact_pq.key2 = item_dim_pq.key1);

-- Two keys match (101, 102); the other four rows are dropped by the probe filter.
select key1, key2, name, dt from orders_fact_pq join item_dim_pq on (orders_fact_pq.key2 = item_dim_pq.key1)
order by key2;
