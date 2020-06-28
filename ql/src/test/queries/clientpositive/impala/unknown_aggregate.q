--! qt:dataset:impala_dataset

explain cbo
select ndv(l_returnflag) from `impala_tpch_lineitem`;

explain
select ndv(l_returnflag) from `impala_tpch_lineitem`;
