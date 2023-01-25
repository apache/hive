drop table if exists x_date_dim;
drop table if exists x_item;
drop table if exists x_store_sales;

create table x_date_dim (d_date_sk bigint, d_year int);

create table x_item (i_item_sk bigint, i_product_name string);

create table x_store_sales (ss_item_sk bigint, dummy string) partitioned by (ss_sold_date_sk bigint);
-- In terms of sizes, x_store_sales should be the biggest table and x_date_dim should be the smallest table so 
-- the content is chosen accordingly.

insert into table x_date_dim
values (1, 1999), (2, 2000), (3, 2001);

insert into table x_item values
(1, 'white snow'),
(2, 'solid steel'),
(3, 'dim cloud');

insert into table x_store_sales (ss_item_sk, dummy, ss_sold_date_sk)
values
(1, 'Dummy content just to make this table size the bigger among others', 1),
(2, 'Dummy content just to make this table size the bigger among others', 2),
(2, 'Dummy content just to make this table size the bigger among others', 3);

-- x_store_sales(TS[0], TS[19]) should join with x_item(TS[1], TS[20]) before join with x_date_dim(TS[2], TS[21]).
-- hive.cbo.enable is set to false in order to arrange joins in the desired order.
set hive.cbo.enable=false;
set hive.auto.convert.join=true;
set hive.optimize.shared.work=true;
set hive.optimize.shared.work.extended=true;
set hive.optimize.shared.work.parallel.edge.support=true;

explain
with base as (
  select
    i_product_name product_name, i_item_sk item_sk, d_year year
  from x_store_sales, x_item, x_date_dim
  where
    ss_item_sk = i_item_sk and ss_sold_date_sk = d_date_sk
  group by i_product_name, i_item_sk, d_year
)
select cs1.product_name, cs1.year, cs2.year
from base cs1, base cs2
where
  cs1.item_sk = cs2.item_sk and
  cs1.year = 2000 and
  cs2.year = 2001;

with base as (
  select
    i_product_name product_name, i_item_sk item_sk, d_year year
  from x_store_sales, x_item, x_date_dim
  where
    ss_item_sk = i_item_sk and ss_sold_date_sk = d_date_sk
  group by i_product_name, i_item_sk, d_year
)
select cs1.product_name, cs1.year, cs2.year
from base cs1, base cs2
where
  cs1.item_sk = cs2.item_sk and
  cs1.year = 2000 and
  cs2.year = 2001;
