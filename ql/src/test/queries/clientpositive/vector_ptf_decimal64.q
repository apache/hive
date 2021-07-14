set hive.auto.convert.join=true;
set hive.vectorized.testing.reducer.batch.size=1;

CREATE TABLE store_sales_repro(ss_ext_sales_price decimal(7,2), ss_item_sk bigint);
CREATE TABLE item_repro(i_class char(50), i_item_sk bigint);

INSERT INTO store_sales_repro VALUES (4721.57, 1);
INSERT INTO store_sales_repro VALUES (4721.58, 1);

INSERT INTO item_repro VALUES ('shirts', 1);

explain vectorization detail 
select i_class,
min(ss_ext_sales_price)*100 / min(min(ss_ext_sales_price)) over (partition by i_class) as revenueratio
from store_sales_repro, item_repro
where ss_item_sk = i_item_sk
group by i_class, ss_ext_sales_price;

select i_class,
min(ss_ext_sales_price)*100 / min(min(ss_ext_sales_price)) over (partition by i_class) as revenueratio
from store_sales_repro, item_repro
where ss_item_sk = i_item_sk
group by i_class, ss_ext_sales_price;

explain vectorization detail
select i_class,
min(ss_ext_sales_price) over (partition by i_class) as min_revenueratio,
max(ss_ext_sales_price) over (partition by i_class) as max_revenueratio,
first_value(ss_ext_sales_price) over (partition by i_class) as first_revenueratio
from store_sales_repro, item_repro
where ss_item_sk = i_item_sk
group by i_class, ss_ext_sales_price;

select i_class,
min(ss_ext_sales_price) over (partition by i_class) as min_revenueratio,
max(ss_ext_sales_price) over (partition by i_class) as max_revenueratio,
first_value(ss_ext_sales_price) over (partition by i_class) as first_revenueratio
from store_sales_repro, item_repro
where ss_item_sk = i_item_sk
group by i_class, ss_ext_sales_price;

explain vectorization detail
select i_class,
sum(ss_ext_sales_price) over (partition by i_class) as sum_revenueratio,
avg(ss_ext_sales_price) over (partition by i_class) as avg_revenueratio
from store_sales_repro, item_repro
where ss_item_sk = i_item_sk
group by i_class, ss_ext_sales_price;

select i_class,
sum(ss_ext_sales_price) over (partition by i_class) as sum_revenueratio,
avg(ss_ext_sales_price) over (partition by i_class) as avg_revenueratio
from store_sales_repro, item_repro
where ss_item_sk = i_item_sk
group by i_class, ss_ext_sales_price;
