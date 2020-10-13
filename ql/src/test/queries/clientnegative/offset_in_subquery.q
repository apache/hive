create table web_sales (ws_order_number int, ws_warehouse_sk int) stored as orc;

select * from web_sales ws1
where exists (select 1 from web_sales ws2 where ws1.ws_order_number = ws2.ws_order_number limit 1 offset 20);
