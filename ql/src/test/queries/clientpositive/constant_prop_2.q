--! qt:dataset:srcpart
--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.compute.query.using.stats=true;
set hive.stats.autogather=true;

explain select count('1') from src group by '1';
select count('1') from src group by '1';

explain 
analyze table srcpart partition (ds='2008-04-08',hr=11) compute statistics for columns key, value;
