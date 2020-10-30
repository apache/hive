--! qt:dataset:impala_dataset

explain cbo physical select
  *
from (
  select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
  from
    impala_tpch_partsupp,
    impala_tpch_supplier,
    impala_tpch_nation
  where
    ps_suppkey = s_suppkey
    and s_nationkey = n_nationkey
    and n_name = 'GERMANY'
  group by
    ps_partkey
) as inner_query
where 
  value > (
    select
      sum(ps_supplycost * ps_availqty) * 0.0001
    from
      impala_tpch_partsupp,
      impala_tpch_supplier,
      impala_tpch_nation
    where
      ps_suppkey = s_suppkey
      and s_nationkey = n_nationkey
      and n_name = 'GERMANY'
  )
order by
  value desc;

explain select
  *
from (
  select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
  from
    impala_tpch_partsupp,
    impala_tpch_supplier,
    impala_tpch_nation
  where
    ps_suppkey = s_suppkey
    and s_nationkey = n_nationkey
    and n_name = 'GERMANY'
  group by
    ps_partkey
) as inner_query
where 
  value > (
    select
      sum(ps_supplycost * ps_availqty) * 0.0001
    from
      impala_tpch_partsupp,
      impala_tpch_supplier,
      impala_tpch_nation
    where
      ps_suppkey = s_suppkey
      and s_nationkey = n_nationkey
      and n_name = 'GERMANY'
  )
order by
  value desc;
