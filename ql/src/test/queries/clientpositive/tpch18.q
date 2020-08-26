--! qt:dataset:tpch_0_001.customer
--! qt:dataset:tpch_0_001.lineitem
--! qt:dataset:tpch_0_001.nation
--! qt:dataset:tpch_0_001.orders
--! qt:dataset:tpch_0_001.part
--! qt:dataset:tpch_0_001.partsupp
--! qt:dataset:tpch_0_001.region
--! qt:dataset:tpch_0_001.supplier




use tpch_0_001;

-- alter table catalog_sales add constraint cs_w foreign key  (cs_warehouse_sk) references warehouse (w_warehouse_sk) disable novalidate rely;

alter table orders add constraint pk_o primary key (o_orderkey) disable novalidate rely;
alter table customer add constraint pk_c primary key (c_custkey) disable novalidate rely;

-- alter table orders add constraint u_o unique (o_orderkey) disable novalidate rely;
-- alter table customer add constraint u_c unique (c_custkey) disable novalidate rely;

alter table lineitem add constraint li_o foreign key  (l_orderkey) references orders (o_orderkey) disable novalidate rely;
alter table orders   add constraint or_c foreign key  (o_custkey) references customer (c_custkey) disable novalidate rely;


set hive.transpose.aggr.join=true;
set hive.mapred.mode=nonstrict;
-- start query 1 in stream 0 using template query18.tpl and seed 1978355063

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


explain select
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

