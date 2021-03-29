--! qt:dataset:impala_dataset

explain
analyze table `impala_tpch_lineitem`
compute statistics;

explain
analyze table `impala_tpch_lineitem`
compute statistics for columns;

explain
analyze table `impala_tpch_lineitem`
compute statistics for columns l_returnflag;

explain
analyze table `impala_tpch_lineitem`
compute statistics for columns
  `l_orderkey`, `l_orderkey`, `l_suppkey`,
  `l_linenumber`, `l_quantity`, `l_extendedprice`,
  `l_discount`, `l_tax`, `l_returnflag`,
  `l_linestatus`, `l_shipdate`, `l_commitdate`,
  `l_receiptdate`, `l_shipinstruct`, `l_shipmode`,
  `l_comment`;

explain
analyze table `impala_tpcds_store_sales`
compute statistics;

explain
analyze table `impala_tpcds_store_sales`
compute statistics for columns;

explain
analyze table `impala_tpcds_store_sales`
compute statistics for columns ss_addr_sk;

explain
analyze table `impala_tpch_lineitem`
compute incremental statistics;

explain
analyze table `impala_tpcds_store_sales`
compute incremental statistics;

explain
analyze table `impala_tpcds_store_sales` partition(`ss_sold_date_sk`='1')
compute incremental statistics;
