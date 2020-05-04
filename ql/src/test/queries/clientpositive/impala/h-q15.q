--! qt:dataset:impala_dataset

explain cbo with revenue_view as (
  select 
    l_suppkey as supplier_no,
    sum(l_extendedprice * (1 - l_discount)) as total_revenue
  from
    impala_tpch_lineitem
  where
    l_shipdate >= '1996-01-01'
    and l_shipdate < '1996-04-01'
  group by
    l_suppkey)
select
  s_suppkey,
  s_name,
  s_address,
  s_phone,
  total_revenue
from
  impala_tpch_supplier,
  revenue_view
where
  s_suppkey = supplier_no
  and total_revenue = (
    select
      max(total_revenue)
    from
      revenue_view
    )
order by
  s_suppkey;

explain with revenue_view as (
  select 
    l_suppkey as supplier_no,
    sum(l_extendedprice * (1 - l_discount)) as total_revenue
  from
    impala_tpch_lineitem
  where
    l_shipdate >= '1996-01-01'
    and l_shipdate < '1996-04-01'
  group by
    l_suppkey)
select
  s_suppkey,
  s_name,
  s_address,
  s_phone,
  total_revenue
from
  impala_tpch_supplier,
  revenue_view
where
  s_suppkey = supplier_no
  and total_revenue = (
    select
      max(total_revenue)
    from
      revenue_view
    )
order by
  s_suppkey;
