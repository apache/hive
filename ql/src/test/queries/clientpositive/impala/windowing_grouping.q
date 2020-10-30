--! qt:dataset:impala_dataset
drop table if exists tab_sales_n0;
create table tab_sales_n0
(
    ss_sold_date_sk           int,
    ss_sold_time_sk           int,
    ss_item_sk                int,
    ss_customer_sk            int,
    ss_cdemo_sk               int,
    ss_hdemo_sk               int,
    ss_addr_sk                int,
    ss_store_sk               int,
    ss_promo_sk               int,
    ss_ticket_number          int,
    ss_quantity               int,
    ss_wholesale_cost         decimal(7,2),
    ss_list_price             decimal(7,2),
    ss_sales_price            decimal(7,2),
    ss_ext_discount_amt       decimal(7,2),
    ss_ext_sales_price        decimal(7,2),
    ss_ext_wholesale_cost     decimal(7,2),
    ss_ext_list_price         decimal(7,2),
    ss_ext_tax                decimal(7,2),
    ss_coupon_amt             decimal(7,2),
    ss_net_paid               decimal(7,2),
    ss_net_paid_inc_tax       decimal(7,2),
    ss_net_profit             decimal(7,2)
)
STORED AS PARQUET;

drop table if exists tab_item_n0;
create table tab_item_n0
(
    i_item_sk                 int,
    i_item_id                 string,
    i_rec_start_date          string,
    i_rec_end_date            string,
    i_item_desc               string,
    i_current_price           decimal(7,2),
    i_wholesale_cost          decimal(7,2),
    i_brand_id                int,
    i_brand                   string,
    i_class_id                int,
    i_class                   string,
    i_category_id             int,
    i_category                string,
    i_manufact_id             int,
    i_manufact                string,
    i_size                    string,
    i_formulation             string,
    i_color                   string,
    i_units                   string,
    i_container               string,
    i_manager_id              int,
    i_product_name            string
)
STORED AS PARQUET;

explain cbo physical
select
    sum(ss_net_profit)/sum(ss_ext_sales_price) as gross_margin
   ,i_category
   ,i_class
   ,grouping(i_class) as lochierarchy
   ,rank() over (
 	partition by grouping(i_class),
 	case when grouping(i_class) = 0 then i_category end
 	order by sum(ss_net_profit)/sum(ss_ext_sales_price) asc) as rank_within_parent
 from
    tab_sales_n0
   ,tab_item_n0
 where i_item_sk  = ss_item_sk
 group by rollup(i_category,i_class);

explain cbo physical
select
    sum(ss_net_profit)/sum(ss_ext_sales_price) as gross_margin
   ,i_category
   ,i_class
   ,char_length(i_category)+char_length(i_class) as lochierarchy
   ,rank() over (
 	partition by char_length(i_category)+char_length(i_class),
 	case when char_length(i_class) = 0 then i_category end
 	order by sum(ss_net_profit)/sum(ss_ext_sales_price) asc) as rank_within_parent
 from
    tab_sales_n0
   ,tab_item_n0
 where i_item_sk  = ss_item_sk
 group by rollup(i_category,i_class);

explain cbo physical
select
    sum(ss_net_profit)/sum(ss_ext_sales_price) as gross_margin
   ,i_category
   ,i_class
   ,grouping(i_category)+grouping(i_class) as lochierarchy
   ,rank() over (
 	partition by grouping(i_category)+grouping(i_class),
 	case when grouping(i_class) = 0 then i_category end
 	order by sum(ss_net_profit)/sum(ss_ext_sales_price) asc) as rank_within_parent
 from
    tab_sales_n0
   ,tab_item_n0
 where i_item_sk  = ss_item_sk
 group by rollup(i_category,i_class);

-- IMPALA PLANS

explain
select
    sum(ss_net_profit)/sum(ss_ext_sales_price) as gross_margin
   ,i_category
   ,i_class
   ,grouping(i_class) as lochierarchy
   ,rank() over (
 	partition by grouping(i_class),
 	case when grouping(i_class) = 0 then i_category end
 	order by sum(ss_net_profit)/sum(ss_ext_sales_price) asc) as rank_within_parent
 from
    tab_sales_n0
   ,tab_item_n0
 where i_item_sk  = ss_item_sk
 group by rollup(i_category,i_class);

explain
select
    sum(ss_net_profit)/sum(ss_ext_sales_price) as gross_margin
   ,i_category
   ,i_class
   ,char_length(i_category)+char_length(i_class) as lochierarchy
   ,rank() over (
 	partition by char_length(i_category)+char_length(i_class),
 	case when char_length(i_class) = 0 then i_category end
 	order by sum(ss_net_profit)/sum(ss_ext_sales_price) asc) as rank_within_parent
 from
    tab_sales_n0
   ,tab_item_n0
 where i_item_sk  = ss_item_sk
 group by rollup(i_category,i_class);

explain
select
    sum(ss_net_profit)/sum(ss_ext_sales_price) as gross_margin
   ,i_category
   ,i_class
   ,grouping(i_category)+grouping(i_class) as lochierarchy
   ,rank() over (
 	partition by grouping(i_category)+grouping(i_class),
 	case when grouping(i_class) = 0 then i_category end
 	order by sum(ss_net_profit)/sum(ss_ext_sales_price) asc) as rank_within_parent
 from
    tab_sales_n0
   ,tab_item_n0
 where i_item_sk  = ss_item_sk
 group by rollup(i_category,i_class);


drop table if exists tab_sales_n0;
drop table if exists tab_item_n0;
