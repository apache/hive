--! qt:dataset:impala_dataset

EXPLAIN CBO PHYSICAL
SELECT stddev(p_size) FROM impala_tpch_part;

EXPLAIN CBO PHYSICAL
SELECT stddev_pop(p_size) FROM impala_tpch_part;

EXPLAIN CBO PHYSICAL
SELECT stddev_samp(p_size) FROM impala_tpch_part;

EXPLAIN CBO PHYSICAL
SELECT variance(p_size) FROM impala_tpch_part;

EXPLAIN CBO PHYSICAL
SELECT var_pop(p_size) FROM impala_tpch_part;

EXPLAIN CBO PHYSICAL
SELECT var_samp(p_size) FROM impala_tpch_part;
