set hive.spark.optimize.shuffle.serde=true;

set hive.spark.use.groupby.shuffle=true;
select key, count(*) from src group by key order by key limit 100;

set hive.spark.use.groupby.shuffle=false;
select key, count(*) from src group by key order by key limit 100;
