--! qt:disabled:flaky HIVE-24112
--! qt:dataset:src
set hive.explain.user=false;
set hive.tez.dynamic.partition.pruning=true;
set hive.tez.dynamic.semijoin.reduction=true;
set hive.tez.bigtable.minsize.semijoin.reduction=1;
set hive.tez.min.bloom.filter.entries=1;

create table dynamic_semijoin_reduction_on_aggcol(id int, outcome string, eventid int) stored as orc;
insert into dynamic_semijoin_reduction_on_aggcol select key, value, key from src;

explain select a.id, b.outcome from (select id, max(eventid) as event_id_max from dynamic_semijoin_reduction_on_aggcol where id = 0 group by id) a 
LEFT OUTER JOIN dynamic_semijoin_reduction_on_aggcol b 
on a.event_id_max = b.eventid;

select a.id, b.outcome from (select id, max(eventid) as event_id_max from dynamic_semijoin_reduction_on_aggcol where id = 0 group by id) a 
LEFT OUTER JOIN dynamic_semijoin_reduction_on_aggcol b 
on a.event_id_max = b.eventid;
