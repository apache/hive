SET hive.fetch.task.conversion=more;
SET hive.explain.user=false;

-- will not print tez counters as tasks will not be launched
select * from src where key is null;
select * from srcpart where key is null;
explain select * from src where key is null;
explain select * from srcpart where key is null;

SET hive.fetch.task.conversion.threshold=1000;
-- will print tez counters as tasks will be launched
select * from src where key is null;
select * from srcpart where key is null;
explain select * from src where key is null;
explain select * from srcpart where key is null;
