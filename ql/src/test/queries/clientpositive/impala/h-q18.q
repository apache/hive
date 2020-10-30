--! qt:dataset:impala_dataset

explain cbo physical select
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice,
  sum(l_quantity)
from
  impala_tpch_customer,
  impala_tpch_orders,
  impala_tpch_lineitem
where
  o_orderkey in (
    select
      l_orderkey
    from
      impala_tpch_lineitem
    group by
      l_orderkey
    having
      sum(l_quantity) > 300
    )
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice
order by
  o_totalprice desc,
  o_orderdate,
  o_orderkey
limit 100;

explain select
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice,
  sum(l_quantity)
from
  impala_tpch_customer,
  impala_tpch_orders,
  impala_tpch_lineitem
where
  o_orderkey in (
    select
      l_orderkey
    from
      impala_tpch_lineitem
    group by
      l_orderkey
    having
      sum(l_quantity) > 300
    )
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice
order by
  o_totalprice desc,
  o_orderdate,
  o_orderkey
limit 100;
