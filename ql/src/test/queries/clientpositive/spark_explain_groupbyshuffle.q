set hive.spark.use.groupby.shuffle=true;

explain select key, count(value) from src group by key;


set hive.spark.use.groupby.shuffle=false;

explain select key, count(value) from src group by key;
