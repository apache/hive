--! qt:dataset:impala_dataset

-- verifies execution.engine and hive.execution.engine are consistent before any SET statement
set execution.engine;
set hive.execution.engine;

set execution.engine=tez;
-- verifies both properties are updated if execution.engine is changed
set execution.engine;
set hive.execution.engine;

set hive.execution.engine=impala;
-- verifies both properties are updated if hive.execution.engine is changed
set hive.execution.engine;
set execution.engine;

set num_nodes=1;
set num_nodes;
set impala.num_nodes;
explain select count(*) from impala_tpch_lineitem;
-- verifies that the change to impala.num_nodes takes effect after a query is executed
set impala.num_nodes;

set impala.num_nodes=3;
set impala.num_nodes;
-- verifies that the change to num_nodes takes effect immediately
set num_nodes;

set impala.num_nodes=1;
set num_nodes=3;
-- verifies that impala.num_nodes and num_nodes are different at this point
set impala.num_nodes;
set num_nodes;
-- executes a query
explain select count(*) from impala_tpch_lineitem;
-- verifies that a non-prefixed option takes precedence over the prefixed one after a query is executed
set impala.num_nodes;
set num_nodes;

set impala.dummy_option=aaa;
set impala.dummy_option;
set dummy_option;
explain select count(*) from impala_tpch_lineitem;
-- verifies that unsupported Impala query options are removed after a query is executed
set impala.dummy_option;
set dummy_option;
