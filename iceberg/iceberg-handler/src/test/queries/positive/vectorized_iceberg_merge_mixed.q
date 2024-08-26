-- SORT_QUERY_RESULTS

-- Mask neededVirtualColumns due to non-strict order
--! qt:replace:/(\s+neededVirtualColumns:\s)(.*)/$1#Masked#/
-- Mask width
--! qt:replace:/(width=55)\d+/$1###/
-- Mask total data size
--! qt:replace:/(Data size: 11)\d+/$1####/

set hive.vectorized.execution.enabled=true;
set hive.llap.io.enabled=false;
set hive.auto.convert.join=true;

drop table if exists store_sales;
drop table if exists ssv;

create table store_sales (
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
partitioned by spec (ss_customer_sk, bucket(3, ss_item_sk))
stored by ICEBERG stored as PARQUET
 TBLPROPERTIES('format-version'='2', 'iceberg.delete.skiprowdata'='true');
 
insert into store_sales (ss_customer_sk, ss_item_sk, ss_sold_date_sk) values (1,1501,"2451181"), (2,1502,"2451181"), (3,1503,"2451181"), (4,1504,"2451181"), (5,1505,"2451181"); 
delete from store_sales where ss_customer_sk > 2;

select count(*) from store_sales;

create table ssv (
    ss_sold_date_sk           int,
    ss_sold_time_sk           int,
    ss_item_sk2               int,
    ss_customer_sk2           int,
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
partitioned by spec (ss_customer_sk2, bucket(3, ss_item_sk2))
stored by ICEBERG stored as ORC
 TBLPROPERTIES('format-version'='2'); 
 
insert into ssv (ss_customer_sk2, ss_item_sk2, ss_ext_discount_amt) values (1,1501,-0.1), (2,1502,-0.1), (3,1503,-0.1), (4,1504,-0.1), (5,1505,-0.1); 

select count(*) from ssv;

explain vectorization detail 
MERGE INTO store_sales t 
    USING ssv s 
ON (t.ss_item_sk = s.ss_item_sk2
    AND t.ss_customer_sk=s.ss_customer_sk2
    AND t.ss_sold_date_sk = "2451181"
    AND ((Floor((s.ss_item_sk2) / 1000) * 1000) BETWEEN 1000 AND 2000)
    AND s.ss_ext_discount_amt < 0.0) WHEN matched
    AND t.ss_ext_discount_amt IS NULL 
THEN UPDATE
    SET ss_ext_discount_amt = 0.0 
WHEN NOT matched THEN
    INSERT (ss_sold_time_sk,
        ss_item_sk,
        ss_customer_sk,
        ss_cdemo_sk,
        ss_hdemo_sk,
        ss_addr_sk,
        ss_store_sk,
        ss_promo_sk,
        ss_ticket_number,
        ss_quantity,
        ss_wholesale_cost,
        ss_list_price,
        ss_sales_price,
        ss_ext_discount_amt,
        ss_ext_sales_price,
        ss_ext_wholesale_cost,
        ss_ext_list_price,
        ss_ext_tax,
        ss_coupon_amt,
        ss_net_paid,
        ss_net_paid_inc_tax,
        ss_net_profit,
        ss_sold_date_sk)
    VALUES (
        s.ss_sold_time_sk,
        s.ss_item_sk2,
        s.ss_customer_sk2,
        s.ss_cdemo_sk,
        s.ss_hdemo_sk,
        s.ss_addr_sk,
        s.ss_store_sk,
        s.ss_promo_sk,
        s.ss_ticket_number,
        s.ss_quantity,
        s.ss_wholesale_cost,
        s.ss_list_price,
        s.ss_sales_price,
        s.ss_ext_discount_amt,
        s.ss_ext_sales_price,
        s.ss_ext_wholesale_cost,
        s.ss_ext_list_price,
        s.ss_ext_tax,
        s.ss_coupon_amt,
        s.ss_net_paid,
        s.ss_net_paid_inc_tax,
        s.ss_net_profit,
        "2451181"
    );
    
select * from store_sales;     

explain
MERGE INTO store_sales t
    USING ssv s
ON (t.ss_item_sk = s.ss_item_sk2
    AND t.ss_customer_sk=s.ss_customer_sk2
    AND t.ss_sold_date_sk = "2451181"
    AND ((Floor((s.ss_item_sk2) / 1000) * 1000) BETWEEN 1000 AND 2000)
    AND s.ss_ext_discount_amt < 0.0) WHEN matched
    AND t.ss_ext_discount_amt IS NULL
THEN UPDATE
    SET ss_ext_discount_amt = 0.0
WHEN NOT matched THEN
    INSERT (ss_sold_time_sk,
        ss_item_sk,
        ss_customer_sk,
        ss_cdemo_sk,
        ss_hdemo_sk,
        ss_addr_sk,
        ss_store_sk,
        ss_promo_sk,
        ss_ticket_number,
        ss_quantity,
        ss_wholesale_cost,
        ss_list_price,
        ss_sales_price,
        ss_ext_discount_amt,
        ss_ext_sales_price,
        ss_ext_wholesale_cost,
        ss_ext_list_price,
        ss_ext_tax,
        ss_coupon_amt,
        ss_net_paid,
        ss_net_paid_inc_tax,
        ss_net_profit,
        ss_sold_date_sk)
    VALUES (
        s.ss_sold_time_sk,
        s.ss_item_sk2,
        s.ss_customer_sk2,
        s.ss_cdemo_sk,
        s.ss_hdemo_sk,
        s.ss_addr_sk,
        s.ss_store_sk,
        s.ss_promo_sk,
        s.ss_ticket_number,
        s.ss_quantity,
        s.ss_wholesale_cost,
        s.ss_list_price,
        s.ss_sales_price,
        s.ss_ext_discount_amt,
        s.ss_ext_sales_price,
        s.ss_ext_wholesale_cost,
        s.ss_ext_list_price,
        s.ss_ext_tax,
        s.ss_coupon_amt,
        s.ss_net_paid,
        s.ss_net_paid_inc_tax,
        s.ss_net_profit,
        "2451181"
    );

MERGE INTO store_sales t 
    USING ssv s 
ON (t.ss_item_sk = s.ss_item_sk2
    AND t.ss_customer_sk=s.ss_customer_sk2
    AND t.ss_sold_date_sk = "2451181"
    AND ((Floor((s.ss_item_sk2) / 1000) * 1000) BETWEEN 1000 AND 2000)
    AND s.ss_ext_discount_amt < 0.0) WHEN matched
    AND t.ss_ext_discount_amt IS NULL 
THEN UPDATE
    SET ss_ext_discount_amt = 0.0 
WHEN NOT matched THEN
    INSERT (ss_sold_time_sk,
        ss_item_sk,
        ss_customer_sk,
        ss_cdemo_sk,
        ss_hdemo_sk,
        ss_addr_sk,
        ss_store_sk,
        ss_promo_sk,
        ss_ticket_number,
        ss_quantity,
        ss_wholesale_cost,
        ss_list_price,
        ss_sales_price,
        ss_ext_discount_amt,
        ss_ext_sales_price,
        ss_ext_wholesale_cost,
        ss_ext_list_price,
        ss_ext_tax,
        ss_coupon_amt,
        ss_net_paid,
        ss_net_paid_inc_tax,
        ss_net_profit,
        ss_sold_date_sk)
    VALUES (
        s.ss_sold_time_sk,
        s.ss_item_sk2,
        s.ss_customer_sk2,
        s.ss_cdemo_sk,
        s.ss_hdemo_sk,
        s.ss_addr_sk,
        s.ss_store_sk,
        s.ss_promo_sk,
        s.ss_ticket_number,
        s.ss_quantity,
        s.ss_wholesale_cost,
        s.ss_list_price,
        s.ss_sales_price,
        s.ss_ext_discount_amt,
        s.ss_ext_sales_price,
        s.ss_ext_wholesale_cost,
        s.ss_ext_list_price,
        s.ss_ext_tax,
        s.ss_coupon_amt,
        s.ss_net_paid,
        s.ss_net_paid_inc_tax,
        s.ss_net_profit,
        "2451181"
    );

select * from store_sales;