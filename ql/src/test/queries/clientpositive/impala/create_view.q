--! qt:dataset:impala_dataset
drop view impala_complex_view;
create view impala_complex_view AS
select
  l_returnflag,
  l_linestatus,
  sum(l_quantity) as sum_qty,
  sum(l_extendedprice) as sum_base_price,
  sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
  avg(l_quantity) as avg_qty,
  avg(l_extendedprice) as avg_price,
  avg(l_discount) as avg_disc,
  count(*) as count_order
from
  impala_tpch_lineitem
where
  l_shipdate <= '1998-09-02'
group by
  l_returnflag,
  l_linestatus;

explain
select
  l_returnflag,
  l_linestatus,
  sum_qty,
  sum_base_price,
  sum_disc_price,
  sum_charge,
  avg_qty,
  avg_price,
  avg_disc,
  count_order
from
  impala_complex_view
order by
  l_returnflag,
  l_linestatus;

drop view impala_complex_view;
