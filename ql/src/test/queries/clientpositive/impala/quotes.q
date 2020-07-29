--! qt:dataset:impala_dataset

explain
select count(distinct cs_order_number) as 'order count'
   ,sum(cs_ext_ship_cost) as "total shipping cost"
   ,sum(cs_net_profit) as `total net profit`
from
    impala_tpcds_catalog_sales cs1
   ,impala_tpcds_date_dim
   ,impala_tpcds_customer_address
   ,impala_tpcds_call_center
where
    d_date between '2000-3-01' and
             (cast('2000-3-01' as timestamp) + interval  60 days)
    and cs1.cs_ship_date_sk = d_date_sk
    and cs1.cs_ship_addr_sk = ca_address_sk
    and ca_state = 'MD'
    and cs1.cs_call_center_sk = cc_call_center_sk
    and cc_county in ('Mobile County','Barrow County','Dauphin County','Williamson County',
                   'Pennington County' )
    and exists (select *
             from impala_tpcds_catalog_sales cs2
             where cs1.cs_order_number = cs2.cs_order_number
               and cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
               and not exists(select *
                from impala_tpcds_catalog_returns cr1
                where cs1.cs_order_number = cr1.cr_order_number)
order by count(distinct cs_order_number) limit 100;

explain
select "What\'s happening?" as single_within_double,
       'I\'m not sure.' as single_within_single,
       "Homer wrote \"The Iliad\"." as double_within_double,
       'Homer also wrote "The Odyssey".' as double_within_single;

explain
select * from impala_tpch_customer "c" limit 10;

explain
select * from impala_tpch_customer AS 'c' limit 10;

explain
select * from (select * from impala_tpch_customer) "c" limit 10;

explain
select * from (select * from impala_tpch_customer) AS "c" limit 10;

explain
select 'c' from impala_tpch_customer AS 'c' limit 10;

explain
select *, 'c' from impala_tpch_customer AS 'c' limit 10;

explain
select *
from impala_tpcds_catalog_sales "cs1"
  join impala_tpcds_date_dim "dd1"
where
  cs_ship_date_sk = d_date_sk
limit 10;

explain
select 'value''alias';

explain
select "value"'alias';

explain
select 'value'"alias";

explain
select 'value'alias;

explain
select "value"alias;
