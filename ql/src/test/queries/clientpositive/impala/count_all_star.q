--! qt:dataset:impala_dataset

EXPLAIN CBO
SELECT COUNT(ALL *)
 FROM impala_tpch_lineitem;

EXPLAIN CBO
SELECT COUNT(*)
 FROM impala_tpch_lineitem;
