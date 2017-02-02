set hive.mapred.mode=nonstrict;
explain 
select i_item_desc ,i_category ,i_class ,i_current_price ,i_item_id ,sum(ss_ext_sales_price) as itemrevenue ,sum(ss_ext_sales_price)*100/sum(sum(ss_ext_sales_price)) over (partition by i_class) as revenueratio from store_sales ,item ,date_dim 
where store_sales.ss_item_sk = item.i_item_sk and i_category in ('Jewelry', 'Sports', 'Books') and store_sales.ss_sold_date_sk = date_dim.d_date_sk and 
d_date between cast('2001-01-12' as date)
                                and (cast('2001-01-12' as date) + 30 days)
group by i_item_id ,i_item_desc ,i_category ,i_class ,i_current_price order by i_category ,i_class ,i_item_id ,i_item_desc ,revenueratio;
