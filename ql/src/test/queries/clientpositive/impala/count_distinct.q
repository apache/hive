--! qt:dataset:impala_dataset

explain
select count(distinct l_quantity), count(distinct l_suppkey)
from impala_tpch_lineitem;

explain
select l_orderkey, count(distinct l_quantity), count(distinct l_suppkey)
from impala_tpch_lineitem
group by l_orderkey;

explain
select l_orderkey, l_partkey, count(distinct l_quantity), count(distinct l_suppkey)
from impala_tpch_lineitem
group by l_orderkey, l_partkey;

explain
select l_partkey, l_orderkey, count(distinct l_quantity), count(distinct l_suppkey)
from impala_tpch_lineitem
group by l_orderkey, l_partkey;

explain
select l_partkey, l_suppkey, l_linenumber, count(distinct l_quantity)
from impala_tpch_lineitem
group by l_suppkey, l_partkey, l_linenumber;

explain
select l_partkey, count(distinct l_quantity), l_orderkey, count(distinct l_suppkey)
from impala_tpch_lineitem
group by l_orderkey, l_partkey;

explain
select count(l_quantity), count(distinct l_suppkey), count(distinct l_quantity), count(l_orderkey)
from impala_tpch_lineitem;

explain
select count(l_quantity), count(distinct l_suppkey), count(distinct l_quantity), count(distinct l_orderkey)
from impala_tpch_lineitem;

explain
select l_orderkey, count(distinct l_quantity), count(l_suppkey)
from impala_tpch_lineitem
group by l_orderkey;

explain
select l_orderkey, count(distinct l_quantity), count(l_suppkey), count(distinct l_suppkey), count(distinct l_quantity)
from impala_tpch_lineitem
group by l_orderkey;

explain
select count(l_quantity), sum(distinct l_suppkey), avg(distinct l_quantity), count(distinct l_orderkey)
from impala_tpch_lineitem;

explain
select l_orderkey, avg(distinct l_quantity), count(l_suppkey), sum(distinct l_suppkey), count(distinct l_quantity)
from impala_tpch_lineitem
group by l_orderkey;
