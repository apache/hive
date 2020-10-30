--! qt:dataset:impala_dataset

explain cbo physical select  
   count(distinct ws_order_number) as `order count`
  ,sum(ws_ext_ship_cost) as `total shipping cost`
  ,sum(ws_net_profit) as `total net profit`
from
   impala_tpcds_web_sales ws1
  ,impala_tpcds_date_dim
  ,impala_tpcds_customer_address
  ,impala_tpcds_web_site
where
    d_date between '1999-4-01' and 
           (cast('1999-4-01' as date) + 60 days)
and ws1.ws_ship_date_sk = d_date_sk
and ws1.ws_ship_addr_sk = ca_address_sk
and ca_state = 'NE'
and ws1.ws_web_site_sk = web_site_sk
and web_company_name = 'pri'
and exists (select *
            from impala_tpcds_web_sales ws2
            where ws1.ws_order_number = ws2.ws_order_number
              and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
and not exists(select *
               from impala_tpcds_web_returns wr1
               where ws1.ws_order_number = wr1.wr_order_number)
order by count(distinct ws_order_number)
limit 100;

explain select  
   count(distinct ws_order_number) as `order count`
  ,sum(ws_ext_ship_cost) as `total shipping cost`
  ,sum(ws_net_profit) as `total net profit`
from
   impala_tpcds_web_sales ws1
  ,impala_tpcds_date_dim
  ,impala_tpcds_customer_address
  ,impala_tpcds_web_site
where
    d_date between '1999-4-01' and 
           (cast('1999-4-01' as date) + 60 days)
and ws1.ws_ship_date_sk = d_date_sk
and ws1.ws_ship_addr_sk = ca_address_sk
and ca_state = 'NE'
and ws1.ws_web_site_sk = web_site_sk
and web_company_name = 'pri'
and exists (select *
            from impala_tpcds_web_sales ws2
            where ws1.ws_order_number = ws2.ws_order_number
              and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
and not exists(select *
               from impala_tpcds_web_returns wr1
               where ws1.ws_order_number = wr1.wr_order_number)
order by count(distinct ws_order_number)
limit 100;
