--! qt:dataset:tpch_0_001.lineitem
--! qt:dataset:tpch_0_001.partsupp
use tpch_0_001;
-- The test is meant to verify the plan, results, and Tez execution stats/counters
-- of the same query in three cases:
--   case 1: no semi-join reducers
--   case 2: one single column semi-join reducer
--   case 3: one multi column semi-join reducer
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.tez.bigtable.minsize.semijoin.reduction=6000;
-- Use TezSummaryPrinter hook to verify the impact of bloom filters by
-- comparing the RECORDS_OUT of the respective operators.
set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecTezSummaryPrinter;

-- Case 1: No semi-join reducers in the query
set hive.tez.dynamic.semijoin.reduction=false;

explain
select li.l_orderkey, li.l_partkey, li.l_suppkey, ps.ps_availqty
from lineitem li
         inner join partsupp ps on li.l_partkey = ps.ps_partkey and li.l_suppkey = ps.ps_suppkey
where ps.ps_availqty < 15
order by li.l_orderkey, li.l_partkey, li.l_suppkey, ps.ps_availqty;

select li.l_orderkey, li.l_partkey, li.l_suppkey, ps.ps_availqty
from lineitem li
         inner join partsupp ps on li.l_partkey = ps.ps_partkey and li.l_suppkey = ps.ps_suppkey
where ps.ps_availqty < 15
order by li.l_orderkey, li.l_partkey, li.l_suppkey, ps.ps_availqty;

-- Case 2: One single column semi-join reducer
set hive.tez.dynamic.semijoin.reduction=true;
set hive.tez.dynamic.semijoin.reduction.multicolumn=false;

explain
select li.l_orderkey, li.l_partkey, li.l_suppkey, ps.ps_availqty
from lineitem li
         inner join partsupp ps on li.l_partkey = ps.ps_partkey and li.l_suppkey = ps.ps_suppkey
where ps.ps_availqty < 15
order by li.l_orderkey, li.l_partkey, li.l_suppkey, ps.ps_availqty;

select li.l_orderkey, li.l_partkey, li.l_suppkey, ps.ps_availqty
from lineitem li
         inner join partsupp ps on li.l_partkey = ps.ps_partkey and li.l_suppkey = ps.ps_suppkey
where ps.ps_availqty < 15
order by li.l_orderkey, li.l_partkey, li.l_suppkey, ps.ps_availqty;

-- Case 3: One multi-column semi-join reducer
set hive.tez.dynamic.semijoin.reduction=true;
set hive.tez.dynamic.semijoin.reduction.multicolumn=true;

explain
select li.l_orderkey, li.l_partkey, li.l_suppkey, ps.ps_availqty
from lineitem li
         inner join partsupp ps on li.l_partkey = ps.ps_partkey and li.l_suppkey = ps.ps_suppkey
where ps.ps_availqty < 15
order by li.l_orderkey, li.l_partkey, li.l_suppkey, ps.ps_availqty;

select li.l_orderkey, li.l_partkey, li.l_suppkey, ps.ps_availqty
from lineitem li
         inner join partsupp ps on li.l_partkey = ps.ps_partkey and li.l_suppkey = ps.ps_suppkey
where ps.ps_availqty < 15
order by li.l_orderkey, li.l_partkey, li.l_suppkey, ps.ps_availqty;