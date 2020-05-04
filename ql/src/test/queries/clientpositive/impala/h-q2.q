--! qt:dataset:impala_dataset

explain cbo select
  s_acctbal,
  s_name,
  n_name,
  p_partkey,
  p_mfgr,
  s_address,
  s_phone,
  s_comment
from
  impala_tpch_part,
  impala_tpch_supplier,
  impala_tpch_partsupp,
  impala_tpch_nation,
  impala_tpch_region
where 
  p_partkey = ps_partkey
  and s_suppkey = ps_suppkey
  and p_size = 15
  and p_type like '%BRASS'
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'EUROPE'
  and ps_supplycost = (
    select
      min(ps_supplycost)
    from
      impala_tpch_partsupp,
      impala_tpch_supplier,
      impala_tpch_nation,
      impala_tpch_region
    where
      p_partkey = ps_partkey
      and s_suppkey = ps_suppkey
      and s_nationkey = n_nationkey
      and n_regionkey = r_regionkey
      and r_name = 'EUROPE'
    )
order by
  s_acctbal desc,
  n_name,
  s_name,
  p_partkey
limit 100;

explain select
  s_acctbal,
  s_name,
  n_name,
  p_partkey,
  p_mfgr,
  s_address,
  s_phone,
  s_comment
from
  impala_tpch_part,
  impala_tpch_supplier,
  impala_tpch_partsupp,
  impala_tpch_nation,
  impala_tpch_region
where 
  p_partkey = ps_partkey
  and s_suppkey = ps_suppkey
  and p_size = 15
  and p_type like '%BRASS'
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'EUROPE'
  and ps_supplycost = (
    select
      min(ps_supplycost)
    from
      impala_tpch_partsupp,
      impala_tpch_supplier,
      impala_tpch_nation,
      impala_tpch_region
    where
      p_partkey = ps_partkey
      and s_suppkey = ps_suppkey
      and s_nationkey = n_nationkey
      and n_regionkey = r_regionkey
      and r_name = 'EUROPE'
    )
order by
  s_acctbal desc,
  n_name,
  s_name,
  p_partkey
limit 100;

