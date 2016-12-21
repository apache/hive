SET hive.fetch.task.conversion=more;
SET hive.explain.user=false;
SET hive.stats.fetch.column.stats=false;

select * from src where key is null;
select * from srcpart where key is null;
explain select * from src where key is null;
explain select key,value from srcpart where key is null;

SET hive.fetch.task.conversion.threshold=1000;
select * from src where key is null;
select * from srcpart where key is null;
explain select * from src where key is null;
explain select key,value from srcpart where key is null;
