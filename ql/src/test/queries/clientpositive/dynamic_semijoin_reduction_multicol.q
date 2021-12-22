--! qt:dataset:tpch_0_001.lineitem
--! qt:dataset:tpch_0_001.partsupp
use tpch_0_001;
-- The test is meant to verify the plan, results, and Tez execution stats/counters
-- of the same query in the following cases:
--   case 1: no semi-join reducers
--   case 2: one single column semi-join reducer
--   case 3: one multi column semi-join reducer
--   case 4: one multi column semi-join reducer created via hints
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

-- Set the size of the big table to a value much greater than the real sizes of
-- the tables so that semijoin reducers do not kick in without explicit hints
set hive.tez.bigtable.minsize.semijoin.reduction=100000000;
-- Set the expected size of the bloom filter in the columns to different values
-- so that we can verify that the plan picks the bigger one for the composite
-- bloom filter.
explain
select
    /*+ semi(ps, ps_partkey, li, 2), semi(ps, ps_suppkey, li, 3)*/
    li.l_orderkey,
    li.l_partkey,
    li.l_suppkey,
    ps.ps_availqty
from lineitem li
         inner join partsupp ps on li.l_partkey = ps.ps_partkey and li.l_suppkey = ps.ps_suppkey
where ps.ps_availqty < 15
order by li.l_orderkey, li.l_partkey, li.l_suppkey, ps.ps_availqty;

select
    /*+ semi(ps, ps_partkey, li, 2), semi(ps, ps_suppkey, li, 3)*/
    li.l_orderkey,
    li.l_partkey,
    li.l_suppkey,
    ps.ps_availqty
from lineitem li
         inner join partsupp ps on li.l_partkey = ps.ps_partkey and li.l_suppkey = ps.ps_suppkey
where ps.ps_availqty < 15
order by li.l_orderkey, li.l_partkey, li.l_suppkey, ps.ps_availqty;