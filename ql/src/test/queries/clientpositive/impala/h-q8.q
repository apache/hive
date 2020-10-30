--! qt:dataset:impala_dataset

explain cbo physical select
  o_year,
  sum(case
    when nation = 'BRAZIL'
    then volume
    else 0
  end) / sum(volume) as mkt_share
from (
  select
    year(o_orderdate) as o_year,
    l_extendedprice * (1 - l_discount) as volume,
    n2.n_name as nation
  from
    impala_tpch_part,
    impala_tpch_supplier,
    impala_tpch_lineitem,
    impala_tpch_orders,
    impala_tpch_customer,
    impala_tpch_nation n1,
    impala_tpch_nation n2,
    impala_tpch_region
  where 
    p_partkey = l_partkey
    and s_suppkey = l_suppkey
    and l_orderkey = o_orderkey
    and o_custkey = c_custkey
    and c_nationkey = n1.n_nationkey
    and n1.n_regionkey = r_regionkey
    and r_name = 'AMERICA' 
    and s_nationkey = n2.n_nationkey
    and o_orderdate between '1995-01-01' and '1996-12-31'
    and p_type = 'ECONOMY ANODIZED STEEL'
  ) as all_nations
group by
  o_year
order by
  o_year;

explain select
  o_year,
  sum(case
    when nation = 'BRAZIL'
    then volume
    else 0
  end) / sum(volume) as mkt_share
from (
  select
    year(o_orderdate) as o_year,
    l_extendedprice * (1 - l_discount) as volume,
    n2.n_name as nation
  from
    impala_tpch_part,
    impala_tpch_supplier,
    impala_tpch_lineitem,
    impala_tpch_orders,
    impala_tpch_customer,
    impala_tpch_nation n1,
    impala_tpch_nation n2,
    impala_tpch_region
  where 
    p_partkey = l_partkey
    and s_suppkey = l_suppkey
    and l_orderkey = o_orderkey
    and o_custkey = c_custkey
    and c_nationkey = n1.n_nationkey
    and n1.n_regionkey = r_regionkey
    and r_name = 'AMERICA' 
    and s_nationkey = n2.n_nationkey
    and o_orderdate between '1995-01-01' and '1996-12-31'
    and p_type = 'ECONOMY ANODIZED STEEL'
  ) as all_nations
group by
  o_year
order by
  o_year;
