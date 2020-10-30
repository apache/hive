--! qt:dataset:impala_dataset

explain cbo physical select
  sum(l_extendedprice) / 7.0 as avg_yearly
from
  impala_tpch_lineitem,
  impala_tpch_part
where
  p_partkey = l_partkey
  and p_brand = 'Brand#23'
  and p_container = 'MED BOX'
  and l_quantity < (
    select
      0.2 * avg(l_quantity)
    from
      impala_tpch_lineitem
    where
      l_partkey = p_partkey
  );

explain select
  sum(l_extendedprice) / 7.0 as avg_yearly
from
  impala_tpch_lineitem,
  impala_tpch_part
where
  p_partkey = l_partkey
  and p_brand = 'Brand#23'
  and p_container = 'MED BOX'
  and l_quantity < (
    select
      0.2 * avg(l_quantity)
    from
      impala_tpch_lineitem
    where
      l_partkey = p_partkey
  );
