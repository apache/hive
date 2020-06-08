--! qt:dataset:impala_dataset

explain cbo select  i_item_id
       ,i_item_desc
       ,i_current_price
 from impala_tpcds_item, impala_tpcds_inventory, impala_tpcds_date_dim, impala_tpcds_catalog_sales
 where i_current_price between 39 and 39 + 30
 and inv_item_sk = i_item_sk
 and d_date_sk=inv_date_sk
 and d_date between cast('2001-01-16' as date) and (cast('2001-01-16' as date) +  60 days)
 and i_manufact_id in (765,886,889,728)
 and inv_quantity_on_hand between 100 and 500
 and cs_item_sk = i_item_sk
 group by i_item_id,i_item_desc,i_current_price
 order by i_item_id
 limit 100;

explain select  i_item_id
       ,i_item_desc
       ,i_current_price
 from impala_tpcds_item, impala_tpcds_inventory, impala_tpcds_date_dim, impala_tpcds_catalog_sales
 where i_current_price between 39 and 39 + 30
 and inv_item_sk = i_item_sk
 and d_date_sk=inv_date_sk
 and d_date between cast('2001-01-16' as date) and (cast('2001-01-16' as date) +  60 days)
 and i_manufact_id in (765,886,889,728)
 and inv_quantity_on_hand between 100 and 500
 and cs_item_sk = i_item_sk
 group by i_item_id,i_item_desc,i_current_price
 order by i_item_id
 limit 100;
