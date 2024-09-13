set hive.cbo.enable=true;
set hive.auto.convert.join=true;
set hive.optimize.shared.work=true;
set hive.tez.dynamic.semijoin.reduction=false;
set hive.tez.dynamic.partition.pruning=true;

create table x_date_dim (d_date_sk bigint, d_year int);
create table x_inventory (inv_quantity_on_hand int) partitioned by (inv_date_sk bigint);
create table x_web_returns (wr_item_sk bigint, wr_refunded_customer_sk bigint) partitioned by (wr_returned_date_sk bigint);

insert into table x_date_dim values (1, 1999), (2, 2000), (3, 2001);
insert into table x_inventory (inv_quantity_on_hand, inv_date_sk) values (1, 1999), (2, 2000), (3, 2001);
insert into table x_web_returns (wr_item_sk, wr_refunded_customer_sk, wr_returned_date_sk) values (1, 1, 1999), (2, 2, 2000), (3, 3, 2001);

alter table x_date_dim update statistics set('numRows'='35', 'rawDataSize'='81449');
alter table x_inventory partition (inv_date_sk = 2000) update statistics set('numRows'='12345', 'rawDataSize'='1234567');
alter table x_web_returns partition (wr_returned_date_sk = 2000) update statistics set('numRows'='123456', 'rawDataSize'='12345678');

alter table x_date_dim update statistics for column d_date_sk set('numDVs'='35','numNulls'='0');
alter table x_inventory partition (inv_date_sk = 2000) update statistics for column inv_quantity_on_hand set('numDVs'='350','numNulls'='0');
alter table x_web_returns partition (wr_returned_date_sk = 2000) update statistics for column wr_item_sk set('numDVs'='3500','numNulls'='0');

with
a1 as (select distinct inv_date_sk, inv_quantity_on_hand from x_inventory, x_date_dim where inv_date_sk = d_date_sk and d_year = 2000),
a2 as (select * from x_web_returns, a1 where inv_date_sk = wr_returned_date_sk),
a3 as (select wr_item_sk, max(wr_refunded_customer_sk) col1 from a2 group by wr_item_sk),
a4 as (select wr_item_sk, min(wr_refunded_customer_sk) col1 from a2 group by wr_item_sk)
select * from a3 join a4 on a3.wr_item_sk = a4.wr_item_sk and a3.col1 < 2 * a4.col1;

