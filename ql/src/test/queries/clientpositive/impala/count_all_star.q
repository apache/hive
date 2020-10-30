--! qt:dataset:impala_dataset

EXPLAIN CBO PHYSICAL
SELECT COUNT(ALL *)
 FROM impala_tpch_lineitem;

EXPLAIN CBO PHYSICAL
SELECT COUNT(*)
 FROM impala_tpch_lineitem;
