drop table if exists x_date_dim;
drop table if exists x_item;
drop table if exists x_store_sales;

create table x_date_dim (d_date_sk bigint, d_year int)
stored by iceberg stored as orc;

create table x_item (i_item_sk bigint, i_color string, i_product_name string, i_current_price double)
stored by iceberg stored as orc;

create table x_store_sales (ss_item_sk bigint, dummy1 string, dummy2 string, dummy3 string, dummy4 string, dummy5 string)
partitioned by (ss_sold_date_sk bigint) stored by iceberg stored as orc;

insert into table x_date_dim (d_date_sk, d_year)
values (1, 1999), (2, 2000), (3, 2001);

insert into table x_item (i_item_sk, i_color, i_product_name, i_current_price)
values
(1, 'white', 'snow', '8.9'),
(2, 'steel', 'steel', '8.9'),
(3, 'dim', 'cloud', '8.9');

insert into table x_store_sales (ss_item_sk, dummy1, dummy2, dummy3, dummy4, dummy5, ss_sold_date_sk)
values
(1, 1234567890, 1234567890, 1234567890, 1234567890, 1234567890, 1),
(2, 1234567890, 1234567890, 1234567890, 1234567890, 1234567890, 2),
(2, 1234567890, 1234567890, 1234567890, 1234567890, 1234567890, 3);

set hive.cbo.enable=false;
set hive.auto.convert.join=true;
set hive.optimize.shared.work=true;
set hive.optimize.shared.work.extended=true;
set hive.optimize.shared.work.parallel.edge.support=true;

--------------------------
-- The following graph is the expected plan when the SharedWorkOptimizer begins.
-- TS[0]-FIL[64]-MAPJOIN[77]-MAPJOIN[78]-FIL[63]-SEL[14]-GBY[15]-RS[16]-GBY[17]-SEL[18]-MAPJOIN[81]-FIL[62]-SEL[44]-FS[45]
-- TS[1]-FIL[65]-RS[6]-MAPJOIN[77]
-- TS[2]-FIL[66]-RS[11]-MAPJOIN[78]
--              -SEL[71]-GBY[72]-EVENT[73]
-- TS[19]-FIL[68]-MAPJOIN[79]-MAPJOIN[80]-FIL[67]-SEL[33]-GBY[34]-RS[35]-GBY[36]-SEL[37]-RS[41]-MAPJOIN[81]
-- TS[20]-FIL[69]-RS[25]-MAPJOIN[79]
-- TS[21]-FIL[70]-RS[30]-MAPJOIN[80]
--               -SEL[74]-GBY[75]-EVENT[76]
-- 
-- Notes:
-- 1. x_store_sales(TS[0], TS[19]) should join with x_item(TS[1], TS[20]) before join with x_date_dim(TS[2], TS[21]).
-- hive.cbo.enable is set to false in order to arrange Joins in the expected order.
-- 
-- 2. In terms of getStatistics().getDataSize(), x_store_sales should be the biggest table and x_date_dim should be the smallest table.
-- Dummy columns in x_store_sales is used to increase rawDataSize of corresponding TSOperators.
--------------------------

set hive.optimize.shared.work.dppunion=true;

explain
with base as (
  select
    i_product_name product_name, i_item_sk item_sk, d_year year, count(*) cnt,
    sum(dummy1) s1, sum(dummy2) s2, sum(dummy3) s3, sum(dummy4) s4, sum(dummy5) s5
  from x_store_sales, x_item, x_date_dim
  where
    ss_item_sk = i_item_sk and ss_sold_date_sk = d_date_sk and
    i_color in ('maroon','burnished','dim','steel','navajo','chocolate') and
    i_current_price between 5 and 10
  group by i_product_name, i_item_sk, d_year
)
select cs1.product_name, cs1.year, cs1.cnt, cs2.year, cs2.cnt, cs1.s1 + cs1.s2 + cs2.s3 + cs2.s4 + cs2.s5
from base cs1, base cs2
where
  cs1.item_sk = cs2.item_sk and
  cs1.year = 2000 and
  cs2.year = 2001;

with base as (
  select
    i_product_name product_name, i_item_sk item_sk, d_year year, count(*) cnt,
    sum(dummy1) s1, sum(dummy2) s2, sum(dummy3) s3, sum(dummy4) s4, sum(dummy5) s5
  from x_store_sales, x_item, x_date_dim
  where
    ss_item_sk = i_item_sk and ss_sold_date_sk = d_date_sk and
    i_color in ('maroon','burnished','dim','steel','navajo','chocolate') and
    i_current_price between 5 and 10
  group by i_product_name, i_item_sk, d_year
)
select cs1.product_name, cs1.year, cs1.cnt, cs2.year, cs2.cnt, cs1.s1 + cs1.s2 + cs2.s3 + cs2.s4 + cs2.s5
from base cs1, base cs2
where
  cs1.item_sk = cs2.item_sk and
  cs1.year = 2000 and
  cs2.year = 2001;

set hive.optimize.shared.work.dppunion=false;

explain
with base as (
  select
    i_product_name product_name, i_item_sk item_sk, d_year year, count(*) cnt,
    sum(dummy1) s1, sum(dummy2) s2, sum(dummy3) s3, sum(dummy4) s4, sum(dummy5) s5
  from x_store_sales, x_item, x_date_dim
  where
    ss_item_sk = i_item_sk and ss_sold_date_sk = d_date_sk and
    i_color in ('maroon','burnished','dim','steel','navajo','chocolate') and
    i_current_price between 5 and 10
  group by i_product_name, i_item_sk, d_year
)
select cs1.product_name, cs1.year, cs1.cnt, cs2.year, cs2.cnt, cs1.s1 + cs1.s2 + cs2.s3 + cs2.s4 + cs2.s5
from base cs1, base cs2
where
  cs1.item_sk = cs2.item_sk and
  cs1.year = 2000 and
  cs2.year = 2001;

with base as (
  select
    i_product_name product_name, i_item_sk item_sk, d_year year, count(*) cnt,
    sum(dummy1) s1, sum(dummy2) s2, sum(dummy3) s3, sum(dummy4) s4, sum(dummy5) s5
  from x_store_sales, x_item, x_date_dim
  where
    ss_item_sk = i_item_sk and ss_sold_date_sk = d_date_sk and
    i_color in ('maroon','burnished','dim','steel','navajo','chocolate') and
    i_current_price between 5 and 10
  group by i_product_name, i_item_sk, d_year
)
select cs1.product_name, cs1.year, cs1.cnt, cs2.year, cs2.cnt, cs1.s1 + cs1.s2 + cs2.s3 + cs2.s4 + cs2.s5
from base cs1, base cs2
where
  cs1.item_sk = cs2.item_sk and
  cs1.year = 2000 and
  cs2.year = 2001;
