--! qt:dataset:tpch_0_001.customer
--! qt:dataset:tpch_0_001.lineitem
--! qt:dataset:tpch_0_001.nation
--! qt:dataset:tpch_0_001.orders
--! qt:dataset:tpch_0_001.part
--! qt:dataset:tpch_0_001.partsupp
--! qt:dataset:tpch_0_001.region
--! qt:dataset:tpch_0_001.supplier


use tpch_0_001;

set hive.transpose.aggr.join=true;
set hive.transpose.aggr.join.unique=true;
set hive.mapred.mode=nonstrict;

create view q18_tmp_cached as
select
	l_orderkey,
	sum(l_quantity) as t_sum_quantity
from
	lineitem
where
	l_orderkey is not null
group by
	l_orderkey;



explain cbo select
c_name,
c_custkey,
o_orderkey,
o_orderdate,
o_totalprice,
sum(l_quantity)
from
	customer,
	orders,
	q18_tmp_cached t,
	lineitem l
where
c_custkey = o_custkey
and o_orderkey = t.l_orderkey
and o_orderkey is not null
and t.t_sum_quantity > 300
and o_orderkey = l.l_orderkey
and l.l_orderkey is not null
group by
c_name,
c_custkey,
o_orderkey,
o_orderdate,
o_totalprice
order by
o_totalprice desc,
o_orderdate
limit 100;



select 'add constraints';

alter table orders add constraint pk_o primary key (o_orderkey) disable novalidate rely;
alter table customer add constraint pk_c primary key (c_custkey) disable novalidate rely;
alter table lineitem add constraint pk_l primary key (l_orderkey,l_linenumber) disable novalidate rely;

alter table lineitem change column L_ORDERKEY L_ORDERKEY int constraint li_ok_nn not null disable novalidate rely;

alter table lineitem add constraint li_o foreign key  (l_orderkey) references orders (o_orderkey) disable novalidate rely;
alter table orders   add constraint or_c foreign key  (o_custkey) references customer (c_custkey) disable novalidate rely;

explain cbo select
c_name,
c_custkey,
o_orderkey,
o_orderdate,
o_totalprice,
sum(l_quantity)
from
	customer,
	orders,
	q18_tmp_cached t,
	lineitem l
where
c_custkey = o_custkey
and o_orderkey = t.l_orderkey
and o_orderkey is not null
and t.t_sum_quantity > 300
and o_orderkey = l.l_orderkey
and l.l_orderkey is not null
group by
c_name,
c_custkey,
o_orderkey,
o_orderdate,
o_totalprice
order by
o_totalprice desc,
o_orderdate
limit 100;


select 'with extended costmodel';
set hive.cbo.costmodel.extended=true;

explain cbo select
c_name,
c_custkey,
o_orderkey,
o_orderdate,
o_totalprice,
sum(l_quantity)
from
	customer,
	orders,
	q18_tmp_cached t,
	lineitem l
where
c_custkey = o_custkey
and o_orderkey = t.l_orderkey
and o_orderkey is not null
and t.t_sum_quantity > 300
and o_orderkey = l.l_orderkey
and l.l_orderkey is not null
group by
c_name,
c_custkey,
o_orderkey,
o_orderdate,
o_totalprice
order by
o_totalprice desc,
o_orderdate
limit 100;

