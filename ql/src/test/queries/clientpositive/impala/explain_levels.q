--! qt:dataset:impala_dataset

set explain_level=MINIMAL;
set explain_level;
-- change to impala.explain_level will not take effect until a query has been executed
set impala.explain_level;
explain select
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
  l_linestatus
order by
  l_returnflag,
  l_linestatus;
-- verifies that the change to impala.explain_level takes effect after a query is executed
set impala.explain_level;

set explain_level=STANDARD;
set explain_level;
-- change to impala.explain_level will not take effect until a query has been executed
set impala.explain_level;
explain select
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
  l_linestatus
order by
  l_returnflag,
  l_linestatus;
-- verifies that the change to impala.explain_level takes effect after a query is executed
set impala.explain_level;

set explain_level=EXTENDED;
set explain_level;
-- change to impala.explain_level will not take effect until a query has been executed
set impala.explain_level;
explain select
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
  l_linestatus
order by
  l_returnflag,
  l_linestatus;
-- verifies that the change to impala.explain_level takes effect after a query is executed
set impala.explain_level;

set explain_level=VERBOSE;
set explain_level;
-- change to impala.explain_level will not take effect until a query has been executed
set impala.explain_level;
explain select
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
  l_linestatus
order by
  l_returnflag,
  l_linestatus;
-- verifies that the change to impala.explain_level takes effect after a query is executed
set impala.explain_level;

set impala.explain_level=MINIMAL;
set impala.explain_level;
-- verifies that the change to explain_level takes effect immediately
set explain_level;
explain select
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
  l_linestatus
order by
  l_returnflag,
  l_linestatus;

set impala.explain_level=STANDARD;
set impala.explain_level;
-- verifies that the change to explain_level takes effect immediately
set explain_level;
explain select
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
  l_linestatus
order by
  l_returnflag,
  l_linestatus;

set impala.explain_level=EXTENDED;
set impala.explain_level;
-- verifies that the change to explain_level takes effect immediately
set explain_level;
explain select
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
  l_linestatus
order by
  l_returnflag,
  l_linestatus;

set impala.explain_level=VERBOSE;
set impala.explain_level;
-- verifies that the change to explain_level takes effect immediately
set explain_level;
explain select
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
  l_linestatus
order by
  l_returnflag,
  l_linestatus;
