--! qt:dataset:impala_dataset

EXPLAIN CBO PHYSICAL
SELECT COUNT(*)
 FROM impala_tpch_lineitem limit 0;
