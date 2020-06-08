--! qt:dataset:impala_dataset

explain cbo select  sum(cs_ext_discount_amt)  as `excess discount amount` 
from 
   impala_tpcds_catalog_sales 
   ,impala_tpcds_item 
   ,impala_tpcds_date_dim
where
i_manufact_id = 66
and i_item_sk = cs_item_sk 
and d_date between '2002-03-29' and 
        (cast('2002-03-29' as date) + 90 days)
and d_date_sk = cs_sold_date_sk 
and cs_ext_discount_amt  
     > ( 
         select 
            1.3 * avg(cs_ext_discount_amt) 
         from 
            impala_tpcds_catalog_sales 
           ,impala_tpcds_date_dim
         where 
              cs_item_sk = i_item_sk 
          and d_date between '2002-03-29' and
                             (cast('2002-03-29' as date) + 90 days)
          and d_date_sk = cs_sold_date_sk 
      ) 
limit 100;

explain select  sum(cs_ext_discount_amt)  as `excess discount amount` 
from 
   impala_tpcds_catalog_sales 
   ,impala_tpcds_item 
   ,impala_tpcds_date_dim
where
i_manufact_id = 66
and i_item_sk = cs_item_sk 
and d_date between '2002-03-29' and 
        (cast('2002-03-29' as date) + 90 days)
and d_date_sk = cs_sold_date_sk 
and cs_ext_discount_amt  
     > ( 
         select 
            1.3 * avg(cs_ext_discount_amt) 
         from 
            impala_tpcds_catalog_sales 
           ,impala_tpcds_date_dim
         where 
              cs_item_sk = i_item_sk 
          and d_date between '2002-03-29' and
                             (cast('2002-03-29' as date) + 90 days)
          and d_date_sk = cs_sold_date_sk 
      ) 
limit 100;
