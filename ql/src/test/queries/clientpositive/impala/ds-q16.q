--! qt:dataset:impala_dataset

explain cbo select  
   count(distinct cs_order_number) as `order count`
  ,sum(cs_ext_ship_cost) as `total shipping cost`
  ,sum(cs_net_profit) as `total net profit`
from
   impala_tpcds_catalog_sales cs1
  ,impala_tpcds_date_dim
  ,impala_tpcds_customer_address
  ,impala_tpcds_call_center
where
    d_date between '1999-4-01' and 
           (cast('1999-4-01' as date) + 60 days)
and cs1.cs_ship_date_sk = d_date_sk
and cs1.cs_ship_addr_sk = ca_address_sk
and ca_state = 'IL'
and cs1.cs_call_center_sk = cc_call_center_sk
and cc_county in ('Richland County','Bronx County','Maverick County','Mesa County',
                  'Raleigh County'
)
and exists (select *
            from impala_tpcds_catalog_sales cs2
            where cs1.cs_order_number = cs2.cs_order_number
              and cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
and not exists(select *
               from impala_tpcds_catalog_returns cr1
               where cs1.cs_order_number = cr1.cr_order_number)
order by count(distinct cs_order_number)
limit 100;

explain select  
   count(distinct cs_order_number) as `order count`
  ,sum(cs_ext_ship_cost) as `total shipping cost`
  ,sum(cs_net_profit) as `total net profit`
from
   impala_tpcds_catalog_sales cs1
  ,impala_tpcds_date_dim
  ,impala_tpcds_customer_address
  ,impala_tpcds_call_center
where
    d_date between '1999-4-01' and 
           (cast('1999-4-01' as date) + 60 days)
and cs1.cs_ship_date_sk = d_date_sk
and cs1.cs_ship_addr_sk = ca_address_sk
and ca_state = 'IL'
and cs1.cs_call_center_sk = cc_call_center_sk
and cc_county in ('Richland County','Bronx County','Maverick County','Mesa County',
                  'Raleigh County'
)
and exists (select *
            from impala_tpcds_catalog_sales cs2
            where cs1.cs_order_number = cs2.cs_order_number
              and cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
and not exists(select *
               from impala_tpcds_catalog_returns cr1
               where cs1.cs_order_number = cr1.cr_order_number)
order by count(distinct cs_order_number)
limit 100;

