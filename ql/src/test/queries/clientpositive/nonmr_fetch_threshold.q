set hive.fetch.task.conversion=more;

explain select * from srcpart where ds='2008-04-08' AND hr='11' limit 10;
explain select cast(key as int) * 10, upper(value) from src limit 10;

set hive.fetch.task.conversion.threshold=10000;

explain select * from srcpart where ds='2008-04-08' AND hr='11' limit 10;
explain select cast(key as int) * 10, upper(value) from src limit 10;
-- Scans without limit (should be Fetch task now)
explain select concat(key, value)  from src;

set hive.fetch.task.conversion.threshold=100;

-- from HIVE-7397, limit + partition pruning filter
explain select * from srcpart where ds='2008-04-08' AND hr='11' limit 10;
explain select cast(key as int) * 10, upper(value) from src limit 10;
-- Scans without limit (should not be Fetch task now)
explain select concat(key, value)  from src;
