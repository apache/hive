--! qt:dataset:impala_dataset

explain cbo physical select  i_item_id
      ,i_item_desc 
      ,i_category 
      ,i_class 
      ,i_current_price
      ,sum(ws_ext_sales_price) as itemrevenue 
      ,sum(ws_ext_sales_price)*100/sum(sum(ws_ext_sales_price)) over
          (partition by i_class) as revenueratio
from     
     impala_tpcds_web_sales
         ,impala_tpcds_item 
         ,impala_tpcds_date_dim
where 
     ws_item_sk = i_item_sk 
       and i_category in ('Electronics', 'Books', 'Women')
       and ws_sold_date_sk = d_date_sk
     and d_date between cast('1998-01-06' as date) 
                    and (cast('1998-01-06' as date) + 30 days)
group by 
     i_item_id
        ,i_item_desc 
        ,i_category
        ,i_class
        ,i_current_price
order by 
     i_category
        ,i_class
        ,i_item_id
        ,i_item_desc
        ,revenueratio
limit 100;

explain select  i_item_id
      ,i_item_desc 
      ,i_category 
      ,i_class 
      ,i_current_price
      ,sum(ws_ext_sales_price) as itemrevenue 
      ,sum(ws_ext_sales_price)*100/sum(sum(ws_ext_sales_price)) over
          (partition by i_class) as revenueratio
from     
     impala_tpcds_web_sales
         ,impala_tpcds_item 
         ,impala_tpcds_date_dim
where 
     ws_item_sk = i_item_sk 
       and i_category in ('Electronics', 'Books', 'Women')
       and ws_sold_date_sk = d_date_sk
     and d_date between cast('1998-01-06' as date) 
                    and (cast('1998-01-06' as date) + 30 days)
group by 
     i_item_id
        ,i_item_desc 
        ,i_category
        ,i_class
        ,i_current_price
order by 
     i_category
        ,i_class
        ,i_item_id
        ,i_item_desc
        ,revenueratio
limit 100;

