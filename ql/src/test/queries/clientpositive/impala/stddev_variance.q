--! qt:dataset:impala_dataset

EXPLAIN CBO
SELECT stddev(p_size) FROM impala_tpch_part;

EXPLAIN CBO
SELECT stddev_pop(p_size) FROM impala_tpch_part;

EXPLAIN CBO
SELECT stddev_samp(p_size) FROM impala_tpch_part;

EXPLAIN CBO
SELECT variance(p_size) FROM impala_tpch_part;

EXPLAIN CBO
SELECT var_pop(p_size) FROM impala_tpch_part;

EXPLAIN CBO
SELECT var_samp(p_size) FROM impala_tpch_part;
