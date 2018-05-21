--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.map.aggr = true;

-- SORT_QUERY_RESULTS

-- union case: both subqueries are map-reduce jobs on same input, followed by filesink


create table tmptable_n12(key string, value int);

explain 
insert overwrite table tmptable_n12
  select unionsrc.key, unionsrc.value FROM (select 'tst1' as key, count(1) as value from src s1
                                        UNION  ALL  
                                            select 'tst2' as key, count(1) as value from src s2) unionsrc;

insert overwrite table tmptable_n12
select unionsrc.key, unionsrc.value FROM (select 'tst1' as key, count(1) as value from src s1
                                        UNION  ALL  
                                          select 'tst2' as key, count(1) as value from src s2) unionsrc;

select * from tmptable_n12 x sort by x.key;


