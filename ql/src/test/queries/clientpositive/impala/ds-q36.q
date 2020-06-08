--! qt:dataset:impala_dataset

explain cbo select  
    sum(ss_net_profit)/sum(ss_ext_sales_price) as gross_margin
   ,i_category
   ,i_class
   ,grouping(i_category)+grouping(i_class) as lochierarchy
   ,rank() over (
      partition by grouping(i_category)+grouping(i_class),
      case when grouping(i_class) = 0 then i_category end 
      order by sum(ss_net_profit)/sum(ss_ext_sales_price) asc) as rank_within_parent
 from
    impala_tpcds_store_sales
   ,impala_tpcds_date_dim       d1
   ,impala_tpcds_item
   ,impala_tpcds_store
 where
    d1.d_year = 1999 
 and d1.d_date_sk = ss_sold_date_sk
 and i_item_sk  = ss_item_sk 
 and s_store_sk  = ss_store_sk
 and s_state in ('IN','AL','MI','MN',
                 'TN','LA','FL','NM')
 group by rollup(i_category,i_class)
 order by
   lochierarchy desc
  ,case when lochierarchy = 0 then i_category end
  ,rank_within_parent
  limit 100;

explain select  
    sum(ss_net_profit)/sum(ss_ext_sales_price) as gross_margin
   ,i_category
   ,i_class
   ,grouping(i_category)+grouping(i_class) as lochierarchy
   ,rank() over (
      partition by grouping(i_category)+grouping(i_class),
      case when grouping(i_class) = 0 then i_category end 
      order by sum(ss_net_profit)/sum(ss_ext_sales_price) asc) as rank_within_parent
 from
    impala_tpcds_store_sales
   ,impala_tpcds_date_dim       d1
   ,impala_tpcds_item
   ,impala_tpcds_store
 where
    d1.d_year = 1999 
 and d1.d_date_sk = ss_sold_date_sk
 and i_item_sk  = ss_item_sk 
 and s_store_sk  = ss_store_sk
 and s_state in ('IN','AL','MI','MN',
                 'TN','LA','FL','NM')
 group by rollup(i_category,i_class)
 order by
   lochierarchy desc
  ,case when lochierarchy = 0 then i_category end
  ,rank_within_parent
  limit 100;
