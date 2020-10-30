--! qt:dataset:impala_dataset

explain cbo physical select
  s_name,
  s_address
from
  impala_tpch_supplier, impala_tpch_nation
where
  s_suppkey in (
    select
      ps_suppkey
    from
      impala_tpch_partsupp
    where 
      ps_partkey in (
        select
          p_partkey
        from
          impala_tpch_part
        where
          p_name like 'forest%'
        ) 
      and ps_availqty > (
        select
          0.5 * sum(l_quantity) 
        from
          impala_tpch_lineitem
        where
          l_partkey = ps_partkey
          and l_suppkey = ps_suppkey
          and l_shipdate >= '1994-01-01'
          and l_shipdate < '1995-01-01'
        ) 
    ) 
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
order by
  s_name;

explain select
  s_name,
  s_address
from
  impala_tpch_supplier, impala_tpch_nation
where
  s_suppkey in (
    select
      ps_suppkey
    from
      impala_tpch_partsupp
    where 
      ps_partkey in (
        select
          p_partkey
        from
          impala_tpch_part
        where
          p_name like 'forest%'
        ) 
      and ps_availqty > (
        select
          0.5 * sum(l_quantity) 
        from
          impala_tpch_lineitem
        where
          l_partkey = ps_partkey
          and l_suppkey = ps_suppkey
          and l_shipdate >= '1994-01-01'
          and l_shipdate < '1995-01-01'
        ) 
    ) 
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
order by
  s_name;
