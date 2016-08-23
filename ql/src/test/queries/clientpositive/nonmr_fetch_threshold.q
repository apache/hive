set hive.fetch.task.conversion=more;
set hive.explain.user=true;
set hive.mapred.mode=nonstrict;
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
-- Simple Scans without limit (will be  Fetch task now)
explain select key, value  from src;
explain select key  from src;
explain select *    from src;
explain select key,1 from src;
explain select cast(key as char(20)),1 from src;
