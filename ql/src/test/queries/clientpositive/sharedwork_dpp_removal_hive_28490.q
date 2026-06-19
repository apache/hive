create table x2_date_dim (d_date_sk bigint, d_week_seq string, d_date string);
create table x2_item (i_item_sk bigint, i_item_id string);
create table x2_store_returns
(sr_returned_date_sk bigint, sr_item_sk bigint, sr_return_quantity int, sr_some_field string, sr_other_field string);
create table x2_catalog_returns
(cr_returned_date_sk bigint, cr_item_sk bigint, cr_return_quantity int, cr_some_field string, cr_other_field string);

alter table x2_date_dim update statistics set('numRows'='35', 'rawDataSize'='81449');
alter table x2_item update statistics set('numRows'='12345', 'rawDataSize'='123456');
alter table x2_store_returns update statistics set('numRows'='123456', 'rawDataSize'='1234567');
alter table x2_catalog_returns update statistics set('numRows'='123456', 'rawDataSize'='1234567');

set hive.auto.convert.join=true;
set hive.tez.dynamic.semijoin.reduction=true;
set hive.tez.bigtable.minsize.semijoin.reduction=30; -- This should be less than numRows of x2_date_dim
set hive.tez.dynamic.semijoin.reduction.threshold=0.0; -- In order not to remove any SemiJoin branch
set hive.tez.dynamic.semijoin.reduction.for.mapjoin=true; -- In order not to remove any SemiJoin branch

-- To check whether the original query plan contains the following pattern: 
-- date_dim ─┐
-- date_dim ─┴ MapJoin ─(DPP)─ date_dim ─ (... catalog_returns)
-- date_dim ─┐
-- date_dim ─┴ MapJoin ─(DPP)─ date_dim ─ (... store_returns)

set hive.optimize.shared.work=false;
explain
with sr_items as (
  select i_item_id item_id, sum(sr_return_quantity) sr_item_qty
  from x2_store_returns, x2_item, x2_date_dim
  where
    sr_item_sk = i_item_sk and
    d_date in (
      select d_date from x2_date_dim
      where d_week_seq in (
        select d_week_seq from x2_date_dim where d_date in ('1998-01-02','1998-10-15','1998-11-10'))) and
    sr_returned_date_sk = d_date_sk group by i_item_id
),
cr_items as (
  select i_item_id item_id, sum(cr_return_quantity) cr_item_qty
  from x2_catalog_returns, x2_item, x2_date_dim
  where
    cr_item_sk = i_item_sk and
    d_date in (
      select d_date from x2_date_dim
      where d_week_seq in (
        select d_week_seq from x2_date_dim where d_date in ('1998-01-02','1998-10-15','1998-11-10'))) and
    cr_returned_date_sk = d_date_sk group by i_item_id
)
select sr_items.item_id, sr_item_qty, cr_item_qty
from sr_items, cr_items
where sr_items.item_id=cr_items.item_id;

set hive.optimize.shared.work=true;
explain
with sr_items as (
  select i_item_id item_id, sum(sr_return_quantity) sr_item_qty
  from x2_store_returns, x2_item, x2_date_dim
  where
    sr_item_sk = i_item_sk and
    d_date in (
      select d_date from x2_date_dim
      where d_week_seq in (
        select d_week_seq from x2_date_dim where d_date in ('1998-01-02','1998-10-15','1998-11-10'))) and
    sr_returned_date_sk = d_date_sk group by i_item_id
),
cr_items as (
  select i_item_id item_id, sum(cr_return_quantity) cr_item_qty
  from x2_catalog_returns, x2_item, x2_date_dim
  where
    cr_item_sk = i_item_sk and
    d_date in (
      select d_date from x2_date_dim
      where d_week_seq in (
        select d_week_seq from x2_date_dim where d_date in ('1998-01-02','1998-10-15','1998-11-10'))) and
    cr_returned_date_sk = d_date_sk group by i_item_id
)
select sr_items.item_id, sr_item_qty, cr_item_qty
from sr_items, cr_items
where sr_items.item_id=cr_items.item_id;
