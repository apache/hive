set hive.auto.convert.join=true;
set hive.vectorized.testing.reducer.batch.size=1;

CREATE TABLE store_sales_repro(ss_ext_sales_price decimal(7,2), ss_item_sk bigint);
CREATE TABLE item_repro(i_class char(50), i_item_sk bigint);

INSERT INTO store_sales_repro VALUES (4721.57, 1);
INSERT INTO store_sales_repro VALUES (4721.58, 1);

INSERT INTO item_repro VALUES ('shirts', 1);

explain vectorization detail 
select i_class,
sum(ss_ext_sales_price)*100 / sum(sum(ss_ext_sales_price)) over (partition by i_class) as revenueratio
from store_sales_repro, item_repro
where ss_item_sk = i_item_sk
group by i_class, ss_ext_sales_price;

select i_class,
sum(ss_ext_sales_price)*100 / sum(sum(ss_ext_sales_price)) over (partition by i_class) as revenueratio
from store_sales_repro, item_repro
where ss_item_sk = i_item_sk
group by i_class, ss_ext_sales_price;
