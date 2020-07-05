--! qt:dataset:impala_dataset

explain
analyze table `impala_tpch_lineitem`
compute statistics noscan;
