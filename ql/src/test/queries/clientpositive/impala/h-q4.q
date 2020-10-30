--! qt:dataset:impala_dataset

explain cbo physical select
  o_orderpriority,
  count(*) as order_count
from
  impala_tpch_orders
where
  o_orderdate >= '1993-07-01'
  and o_orderdate < '1993-10-01'
  and exists (
    select
      *
    from
      impala_tpch_lineitem
    where
      l_orderkey = o_orderkey
      and l_commitdate < l_receiptdate
    ) 
group by
  o_orderpriority
order by
  o_orderpriority;

explain select
  o_orderpriority,
  count(*) as order_count
from
  impala_tpch_orders
where
  o_orderdate >= '1993-07-01'
  and o_orderdate < '1993-10-01'
  and exists (
    select
      *
    from
      impala_tpch_lineitem
    where
      l_orderkey = o_orderkey
      and l_commitdate < l_receiptdate
    ) 
group by
  o_orderpriority
order by
  o_orderpriority;
