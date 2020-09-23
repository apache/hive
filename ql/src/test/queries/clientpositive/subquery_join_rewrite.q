create table web_sales (ws_order_number int, ws_warehouse_sk int) stored as orc;

insert into web_sales values
(null, null),
(1, 1),
(1, 2),
(null, null),
(null, null),
(2, 1),
(2, 2),
(null, null);

-- EXISTS, co-relation, LIMIT
explain cbo
select * from web_sales ws1
where exists (select 1 from web_sales ws2 where ws1.ws_order_number = ws2.ws_order_number limit 1);

select * from web_sales ws1
where exists (select 1 from web_sales ws2 where ws1.ws_order_number = ws2.ws_order_number limit 1);

explain cbo
select * from web_sales ws1
where exists (select 1 from web_sales ws2 where ws1.ws_order_number = ws2.ws_order_number limit 0);

select * from web_sales ws1
where exists (select 1 from web_sales ws2 where ws1.ws_order_number = ws2.ws_order_number limit 0);

-- NOT EXISTS, co-relation, LIMIT
explain cbo
select * from web_sales ws1
where not exists (select 1 from web_sales ws2 where ws1.ws_order_number = ws2.ws_order_number limit 1);

select * from web_sales ws1
where not exists (select 1 from web_sales ws2 where ws1.ws_order_number = ws2.ws_order_number limit 1);

-- EXISTS, co-relation, ORDER BY
explain cbo
select * from web_sales ws1
where exists (select 1 from web_sales ws2 where ws1.ws_order_number = ws2.ws_order_number order by ws2.ws_order_number);

select * from web_sales ws1
where exists (select 1 from web_sales ws2 where ws1.ws_order_number = ws2.ws_order_number order by ws2.ws_order_number);


-- NOT EXISTS, co-relation, ORDER BY
explain cbo
select * from web_sales ws1
where not exists (select 1 from web_sales ws2 where ws1.ws_order_number = ws2.ws_order_number order by ws2.ws_order_number);

select * from web_sales ws1
where not exists (select 1 from web_sales ws2 where ws1.ws_order_number = ws2.ws_order_number order by ws2.ws_order_number);


-- EXISTS, LIMIT
explain cbo
select * from web_sales ws1
where exists (select 1 from web_sales ws2 where ws2.ws_order_number = 2 limit 1);

select * from web_sales ws1
where exists (select 1 from web_sales ws2 where ws2.ws_order_number = 2 limit 1);

explain cbo
select * from web_sales ws1
where exists (select 1 from web_sales ws2 where ws2.ws_order_number = 2 limit 0);

select * from web_sales ws1
where exists (select 1 from web_sales ws2 where ws2.ws_order_number = 2 limit 0);


-- IN, LIMIT
explain cbo
select * from web_sales ws1
where ws1.ws_order_number in (select ws2.ws_order_number from web_sales ws2 order by ws2.ws_order_number nulls last limit 1);

select * from web_sales ws1
where ws1.ws_order_number in (select ws2.ws_order_number from web_sales ws2 order by ws2.ws_order_number nulls last limit 1);


explain cbo
select * from web_sales ws1
where ws1.ws_order_number in (select ws2.ws_order_number from web_sales ws2 order by ws2.ws_order_number nulls first limit 1);

select * from web_sales ws1
where ws1.ws_order_number in (select ws2.ws_order_number from web_sales ws2 order by ws2.ws_order_number nulls first limit 1);


-- NOT IN, LIMIT
explain cbo
select * from web_sales ws1
where ws1.ws_order_number not in (select ws2.ws_order_number from web_sales ws2 order by ws2.ws_order_number nulls last limit 1);

select * from web_sales ws1
where ws1.ws_order_number not in (select ws2.ws_order_number from web_sales ws2 order by ws2.ws_order_number nulls last limit 1);


explain cbo
select * from web_sales ws1
where ws1.ws_order_number not in (select ws2.ws_order_number from web_sales ws2 order by ws2.ws_order_number nulls first limit 1);

select * from web_sales ws1
where ws1.ws_order_number not in (select ws2.ws_order_number from web_sales ws2 order by ws2.ws_order_number nulls first limit 1);

explain cbo
select * from web_sales ws1
where ws1.ws_order_number not in (select ws2.ws_order_number from web_sales ws2 order by ws2.ws_order_number nulls last limit 1 offset 2);

select * from web_sales ws1
where ws1.ws_order_number not in (select ws2.ws_order_number from web_sales ws2 order by ws2.ws_order_number nulls last limit 1 offset 2);
