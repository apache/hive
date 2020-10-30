--! qt:dataset:impala_dataset

explain cbo physical with ss as (
 select i_item_id,sum(ss_ext_sales_price) total_sales
 from
      impala_tpcds_store_sales,
      impala_tpcds_date_dim,
         impala_tpcds_customer_address,
         impala_tpcds_item
 where i_item_id in (select
     i_item_id
from impala_tpcds_item
where i_color in ('chiffon','smoke','lace'))
 and     ss_item_sk              = i_item_sk
 and     ss_sold_date_sk         = d_date_sk
 and     d_year                  = 2001
 and     d_moy                   = 5
 and     ss_addr_sk              = ca_address_sk
 and     ca_gmt_offset           = -6 
 group by i_item_id),
 cs as (
 select i_item_id,sum(cs_ext_sales_price) total_sales
 from
      impala_tpcds_catalog_sales,
      impala_tpcds_date_dim,
         impala_tpcds_customer_address,
         impala_tpcds_item
 where
         i_item_id               in (select
  i_item_id
from impala_tpcds_item
where i_color in ('chiffon','smoke','lace'))
 and     cs_item_sk              = i_item_sk
 and     cs_sold_date_sk         = d_date_sk
 and     d_year                  = 2001
 and     d_moy                   = 5
 and     cs_bill_addr_sk         = ca_address_sk
 and     ca_gmt_offset           = -6 
 group by i_item_id),
 ws as (
 select i_item_id,sum(ws_ext_sales_price) total_sales
 from
      impala_tpcds_web_sales,
      impala_tpcds_date_dim,
         impala_tpcds_customer_address,
         impala_tpcds_item
 where
         i_item_id               in (select
  i_item_id
from impala_tpcds_item
where i_color in ('chiffon','smoke','lace'))
 and     ws_item_sk              = i_item_sk
 and     ws_sold_date_sk         = d_date_sk
 and     d_year                  = 2001
 and     d_moy                   = 5
 and     ws_bill_addr_sk         = ca_address_sk
 and     ca_gmt_offset           = -6
 group by i_item_id)
  select  i_item_id ,sum(total_sales) total_sales
 from  (select * from ss 
        union all
        select * from cs 
        union all
        select * from ws) tmp1
 group by i_item_id
 order by total_sales,
          i_item_id
 limit 100;

explain with ss as (
 select i_item_id,sum(ss_ext_sales_price) total_sales
 from
      impala_tpcds_store_sales,
      impala_tpcds_date_dim,
         impala_tpcds_customer_address,
         impala_tpcds_item
 where i_item_id in (select
     i_item_id
from impala_tpcds_item
where i_color in ('chiffon','smoke','lace'))
 and     ss_item_sk              = i_item_sk
 and     ss_sold_date_sk         = d_date_sk
 and     d_year                  = 2001
 and     d_moy                   = 5
 and     ss_addr_sk              = ca_address_sk
 and     ca_gmt_offset           = -6 
 group by i_item_id),
 cs as (
 select i_item_id,sum(cs_ext_sales_price) total_sales
 from
      impala_tpcds_catalog_sales,
      impala_tpcds_date_dim,
         impala_tpcds_customer_address,
         impala_tpcds_item
 where
         i_item_id               in (select
  i_item_id
from impala_tpcds_item
where i_color in ('chiffon','smoke','lace'))
 and     cs_item_sk              = i_item_sk
 and     cs_sold_date_sk         = d_date_sk
 and     d_year                  = 2001
 and     d_moy                   = 5
 and     cs_bill_addr_sk         = ca_address_sk
 and     ca_gmt_offset           = -6 
 group by i_item_id),
 ws as (
 select i_item_id,sum(ws_ext_sales_price) total_sales
 from
      impala_tpcds_web_sales,
      impala_tpcds_date_dim,
         impala_tpcds_customer_address,
         impala_tpcds_item
 where
         i_item_id               in (select
  i_item_id
from impala_tpcds_item
where i_color in ('chiffon','smoke','lace'))
 and     ws_item_sk              = i_item_sk
 and     ws_sold_date_sk         = d_date_sk
 and     d_year                  = 2001
 and     d_moy                   = 5
 and     ws_bill_addr_sk         = ca_address_sk
 and     ca_gmt_offset           = -6
 group by i_item_id)
  select  i_item_id ,sum(total_sales) total_sales
 from  (select * from ss 
        union all
        select * from cs 
        union all
        select * from ws) tmp1
 group by i_item_id
 order by total_sales,
          i_item_id
 limit 100;
