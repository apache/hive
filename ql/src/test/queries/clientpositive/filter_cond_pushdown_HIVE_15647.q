set hive.cbo.enable=false;

CREATE TABLE sales_HIVE_15647 (store_id INTEGER, store_number INTEGER, customer_id INTEGER);
CREATE TABLE store_HIVE_15647 (store_id INTEGER, salad_bar BOOLEAN);

explain select count(*) from
sales_HIVE_15647 as sales 
join store_HIVE_15647 as store on sales.store_id = store.store_id
where ((store.salad_bar)) and ((sales.store_number) <=> (sales.customer_id));

explain select count(*) from
sales_HIVE_15647 as sales 
join store_HIVE_15647 as store on sales.store_id = store.store_id
where ((store.salad_bar)) and ((sales.store_number) = (sales.customer_id));

explain select count(*) from
sales_HIVE_15647 as sales 
join store_HIVE_15647 as store on sales.store_id = store.store_id
where ((store.salad_bar = true)) and ((sales.store_number) <=> (sales.customer_id));

explain select count(*) from
sales_HIVE_15647 as sales 
join store_HIVE_15647 as store on sales.store_id = store.store_id
where ((store.salad_bar = false)) and ((sales.store_number) > (sales.customer_id));