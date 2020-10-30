--! qt:dataset:impala_dataset

explain cbo physical
select l_quantity from `impala_tpch_lineitem` limit 10;

explain
select l_quantity from `impala_tpch_lineitem` limit 10;
