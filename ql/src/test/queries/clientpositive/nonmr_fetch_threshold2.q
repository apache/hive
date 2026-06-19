--! qt:dataset:srcpart
--! qt:dataset:src
set hive.fetch.task.conversion=more;
set hive.explain.user=true;
set hive.mapred.mode=nonstrict;
set hive.fetch.task.caching=true;
explain select * from srcpart where ds='2008-04-08' AND hr='11' limit 10;

set hive.fetch.task.conversion.threshold=10000;

explain select * from srcpart where ds='2008-04-08' AND hr='11' limit 10;

-- with caching enabled, fetch task will be dropped, unlike when caching is not enabled (see: nonmr_fetch_threshold.q)
set hive.fetch.task.conversion.threshold=100;

explain select * from srcpart where ds='2008-04-08' AND hr='11' limit 10;
