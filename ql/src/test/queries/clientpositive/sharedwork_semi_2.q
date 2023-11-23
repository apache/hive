set hive.auto.convert.join=true;
set hive.optimize.shared.work.dppunion.merge.eventops=true;
set hive.optimize.shared.work.dppunion=true;
set hive.optimize.shared.work.extended=true;
set hive.optimize.shared.work.parallel.edge.support=true;
set hive.optimize.shared.work=true;
set hive.support.concurrency=true;
set hive.tez.bigtable.minsize.semijoin.reduction=1;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

drop table if exists x1_store_sales;
drop table if exists x1_date_dim;

create table x1_store_sales (ss_sold_date_sk int, ss_item_sk int) stored as orc tblproperties('transactional'='true', 'transactional_properties'='default');
create table x1_date_dim (d_date_sk int, d_month_seq int, d_year int, d_moy int) stored as orc tblproperties('transactional'='true', 'transactional_properties'='default');

insert into x1_date_dim values (1,1,2000,1), (2,2,2001,2), (3,2,2001,3), (4,2,2001,4), (5,2,2001,5), (6,2,2001,6), (7,2,2001,7), (8,2,2001,8);
insert into x1_store_sales values (1,1),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10),(11,11);

alter table x1_store_sales update statistics set('numRows'='123456', 'rawDataSize'='1234567');
alter table x1_date_dim update statistics set('numRows'='28', 'rawDataSize'='81449');

explain
select ss_item_sk
from (
    select ss_item_sk, ss_sold_date_sk from x1_store_sales
    union all
    select ss_item_sk, ss_sold_date_sk from x1_store_sales
    union all
    select ss_item_sk, ss_sold_date_sk from x1_store_sales
  ) as tmp,
  x1_date_dim
where ss_sold_date_sk = d_date_sk and d_moy=1;

select ss_item_sk
from (
    select ss_item_sk, ss_sold_date_sk from x1_store_sales
    union all
    select ss_item_sk, ss_sold_date_sk from x1_store_sales
    union all
    select ss_item_sk, ss_sold_date_sk from x1_store_sales
  ) as tmp,
  x1_date_dim
where ss_sold_date_sk = d_date_sk and d_moy=1;


