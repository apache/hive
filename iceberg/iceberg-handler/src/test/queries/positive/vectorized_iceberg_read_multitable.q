set hive.vectorized.execution.enabled=true;
set hive.query.results.cache.enabled=false;

create external table lineitem(l_discount decimal(15,2), l_orderkey int) stored as orc;
insert into lineitem values (100.2, 10);

create external table customer_ice(c_custkey int) STORED BY ICEBERG stored as orc
TBLPROPERTIES ('iceberg.decimal64.vectorization'='true');
insert into customer_ice values (10);

create external table orders(o_orderkey int, o_custkey int) stored as orc;
insert into orders values (10, 10);

alter table customer_ice set tblproperties ( 'iceberg.orc.files.only' = 'false');

select sum(1 - l_discount) as revenue
FROM customer_ice, orders, lineitem
WHERE c_custkey = o_custkey and l_orderkey = o_orderkey limit 20;

create external table lineitem_ice(l_discount decimal(15,2), l_orderkey int) STORED BY ICEBERG stored as orc
TBLPROPERTIES ('iceberg.decimal64.vectorization'='true');
insert into lineitem_ice values (100.2, 10);

select sum(1 - l_discount) as revenue
FROM customer_ice, orders, lineitem_ice
WHERE c_custkey = o_custkey and l_orderkey = o_orderkey limit 20;

alter table customer_ice set tblproperties ( 'iceberg.orc.files.only' = 'true');

select sum(1 - l_discount) as revenue
FROM customer_ice, orders, lineitem
WHERE c_custkey = o_custkey and l_orderkey = o_orderkey limit 20;

select sum(1 - l_discount) as revenue
FROM customer_ice, orders, lineitem_ice
WHERE c_custkey = o_custkey and l_orderkey = o_orderkey limit 20;