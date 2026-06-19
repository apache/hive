set hive.merge.nway.joins=false;
set hive.vectorized.execution.enabled=false;
set hive.auto.convert.join=true;
set hive.auto.convert.anti.join=true;

drop table if exists tt1;
drop table if exists tt2;
drop table if exists tt3;

create table tt1 (ws_order_number bigint, ws_ext_ship_cost decimal(7, 2));
create table tt2 (ws_order_number bigint);
create table tt3 (wr_order_number bigint);

insert into tt1 values (42, 3093.96), (1041, 299.28), (1378, 85.56), (1378, 719.44), (1395, 145.68);
insert into tt2 values (1378), (1395);
insert into tt3 values (42), (1041);

-- The result should be the same regardless of vectorization.

explain
select sum(ws_ext_ship_cost) from tt1 ws1, tt2 ws2
where ws1.ws_order_number = ws2.ws_order_number
and not exists(select * from tt3 wr1 where ws1.ws_order_number = wr1.wr_order_number);
explain cbo
select sum(ws_ext_ship_cost) from tt1 ws1, tt2 ws2
where ws1.ws_order_number = ws2.ws_order_number
and not exists(select * from tt3 wr1 where ws1.ws_order_number = wr1.wr_order_number);
select sum(ws_ext_ship_cost) from tt1 ws1, tt2 ws2
where ws1.ws_order_number = ws2.ws_order_number
and not exists(select * from tt3 wr1 where ws1.ws_order_number = wr1.wr_order_number);

explain
select * from tt1 where not exists(select * from tt2 where tt1.ws_order_number = tt2.ws_order_number);
explain cbo
select * from tt1 where not exists(select * from tt2 where tt1.ws_order_number = tt2.ws_order_number);
select * from tt1 where not exists(select * from tt2 where tt1.ws_order_number = tt2.ws_order_number);


set hive.vectorized.execution.enabled=true;

explain
select sum(ws_ext_ship_cost) from tt1 ws1, tt2 ws2
where ws1.ws_order_number = ws2.ws_order_number
and not exists(select * from tt3 wr1 where ws1.ws_order_number = wr1.wr_order_number);
explain cbo
select sum(ws_ext_ship_cost) from tt1 ws1, tt2 ws2
where ws1.ws_order_number = ws2.ws_order_number
and not exists(select * from tt3 wr1 where ws1.ws_order_number = wr1.wr_order_number);
select sum(ws_ext_ship_cost) from tt1 ws1, tt2 ws2
where ws1.ws_order_number = ws2.ws_order_number
and not exists(select * from tt3 wr1 where ws1.ws_order_number = wr1.wr_order_number);

explain
select * from tt1 where not exists(select * from tt2 where tt1.ws_order_number = tt2.ws_order_number);
explain cbo
select * from tt1 where not exists(select * from tt2 where tt1.ws_order_number = tt2.ws_order_number);
select * from tt1 where not exists(select * from tt2 where tt1.ws_order_number = tt2.ws_order_number);


-- Test n-way join which contains AntiJoin

set hive.vectorized.execution.enabled=false;
set hive.merge.nway.joins=true;

explain
select sum(ws_ext_ship_cost) from tt1 ws1, tt2 ws2
where ws1.ws_order_number = ws2.ws_order_number
and not exists(select * from tt3 wr1 where ws1.ws_order_number = wr1.wr_order_number);
explain cbo
select sum(ws_ext_ship_cost) from tt1 ws1, tt2 ws2
where ws1.ws_order_number = ws2.ws_order_number
and not exists(select * from tt3 wr1 where ws1.ws_order_number = wr1.wr_order_number);
select sum(ws_ext_ship_cost) from tt1 ws1, tt2 ws2
where ws1.ws_order_number = ws2.ws_order_number
and not exists(select * from tt3 wr1 where ws1.ws_order_number = wr1.wr_order_number);


-- Test MergeJoin -> MapJoin pattern

set hive.merge.nway.joins=false;
set hive.vectorized.execution.enabled=false;
set hive.auto.convert.join=true;
set hive.auto.convert.anti.join=true;

alter table tt1 update statistics set ('numRows'='10000000');
alter table tt2 update statistics set ('numRows'='10000000');
alter table tt3 update statistics set ('numRows'='2');

explain
select sum(ws_ext_ship_cost) from tt1 ws1, tt2 ws2
where ws1.ws_order_number = ws2.ws_order_number
and not exists(select * from tt3 wr1 where ws1.ws_order_number = wr1.wr_order_number);
explain cbo
select sum(ws_ext_ship_cost) from tt1 ws1, tt2 ws2
where ws1.ws_order_number = ws2.ws_order_number
and not exists(select * from tt3 wr1 where ws1.ws_order_number = wr1.wr_order_number);
select sum(ws_ext_ship_cost) from tt1 ws1, tt2 ws2
where ws1.ws_order_number = ws2.ws_order_number
and not exists(select * from tt3 wr1 where ws1.ws_order_number = wr1.wr_order_number);

