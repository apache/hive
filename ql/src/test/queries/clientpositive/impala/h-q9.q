--! qt:dataset:impala_dataset

explain cbo select
  nation,
  o_year,
  sum(amount) as sum_profit
from(
  select 
    n_name as nation,
    year(o_orderdate) as o_year,
    l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
  from
    impala_tpch_part,
    impala_tpch_supplier, 
    impala_tpch_lineitem,
    impala_tpch_partsupp,
    impala_tpch_orders,
    impala_tpch_nation
  where
    s_suppkey = l_suppkey
    and ps_suppkey = l_suppkey
    and ps_partkey = l_partkey
    and p_partkey = l_partkey
    and o_orderkey = l_orderkey
    and s_nationkey = n_nationkey
    and p_name like '%green%'
  ) as profit
group by
  nation,
  o_year
order by
  nation,
  o_year desc;

explain select
  nation,
  o_year,
  sum(amount) as sum_profit
from(
  select 
    n_name as nation,
    year(o_orderdate) as o_year,
    l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
  from
    impala_tpch_part,
    impala_tpch_supplier, 
    impala_tpch_lineitem,
    impala_tpch_partsupp,
    impala_tpch_orders,
    impala_tpch_nation
  where
    s_suppkey = l_suppkey
    and ps_suppkey = l_suppkey
    and ps_partkey = l_partkey
    and p_partkey = l_partkey
    and o_orderkey = l_orderkey
    and s_nationkey = n_nationkey
    and p_name like '%green%'
  ) as profit
group by
  nation,
  o_year
order by
  nation,
  o_year desc;
