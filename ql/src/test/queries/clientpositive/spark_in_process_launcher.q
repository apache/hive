--! qt:dataset:src

set hive.spark.client.type=spark-launcher;

explain select key, count(*) from src group by key order by key limit 10;
select key, count(*) from src group by key order by key limit 10;
