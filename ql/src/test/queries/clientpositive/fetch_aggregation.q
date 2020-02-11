--! qt:dataset:src
set hive.fetch.task.aggr=true;
set hive.exec.submitviachild=true;
set hive.exec.submit.local.task.via.child=true;

explain
select count(key),sum(key),avg(key),min(key),max(key),std(key),variance(key) from src;

select count(key),sum(key),avg(key),min(key),max(key),std(key),variance(key) from src;
