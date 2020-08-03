
set hive.mapred.mode=nonstrict;
-- start query 1 in stream 0 using template query18.tpl and seed 1978355063

create database x;
use x;

drop table if exists lineitem;
create external table lineitem 
(L_ORDERKEY BIGINT,
 L_PARTKEY BIGINT,
 L_SUPPKEY BIGINT,
 L_LINENUMBER INT,
 L_QUANTITY DOUBLE,
 L_EXTENDEDPRICE DOUBLE,
 L_DISCOUNT DOUBLE,
 L_TAX DOUBLE,
 L_RETURNFLAG STRING,
 L_LINESTATUS STRING,
 L_SHIPDATE STRING,
 L_COMMITDATE STRING,
 L_RECEIPTDATE STRING,
 L_SHIPINSTRUCT STRING,
 L_SHIPMODE STRING,
 L_COMMENT STRING)
;
drop table if exists part;
create external table part (P_PARTKEY BIGINT,
 P_NAME STRING,
 P_MFGR STRING,
 P_BRAND STRING,
 P_TYPE STRING,
 P_SIZE INT,
 P_CONTAINER STRING,
 P_RETAILPRICE DOUBLE,
 P_COMMENT STRING) 
;
drop table if exists supplier;
create external table supplier (S_SUPPKEY BIGINT,
 S_NAME STRING,
 S_ADDRESS STRING,
 S_NATIONKEY BIGINT,
 S_PHONE STRING,
 S_ACCTBAL DOUBLE,
 S_COMMENT STRING) 
;
drop table if exists partsupp;
create external table partsupp (PS_PARTKEY BIGINT,
 PS_SUPPKEY BIGINT,
 PS_AVAILQTY INT,
 PS_SUPPLYCOST DOUBLE,
 PS_COMMENT STRING)
;
drop table if exists nation;
create external table nation (N_NATIONKEY BIGINT,
 N_NAME STRING,
 N_REGIONKEY BIGINT,
 N_COMMENT STRING)
;

drop table if exists region;
create external table region (R_REGIONKEY BIGINT,
 R_NAME STRING,
 R_COMMENT STRING)
;

drop table if exists customer;
create external table customer (C_CUSTKEY BIGINT,
 C_NAME STRING,
 C_ADDRESS STRING,
 C_NATIONKEY BIGINT,
 C_PHONE STRING,
 C_ACCTBAL DOUBLE,
 C_MKTSEGMENT STRING,
 C_COMMENT STRING)
;

drop table if exists orders;
create external table orders (O_ORDERKEY BIGINT,
 O_CUSTKEY BIGINT,
 O_ORDERSTATUS STRING,
 O_TOTALPRICE DOUBLE,
 O_ORDERDATE STRING,
 O_ORDERPRIORITY STRING,
 O_CLERK STRING,
 O_SHIPPRIORITY INT,
 O_COMMENT STRING)
;


analyze table lineitem compute statistics for columns;
analyze table customer compute statistics for columns;
analyze table orders compute statistics for columns;


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
limit 100

