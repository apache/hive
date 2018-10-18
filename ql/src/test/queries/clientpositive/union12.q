--! qt:dataset:srcbucket
--! qt:dataset:src1
--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.map.aggr = true;

-- SORT_QUERY_RESULTS

-- union case: all subqueries are a map-reduce jobs, 3 way union, different inputs for all sub-queries, followed by filesink

create table tmptable_n10(key string, value int);

explain 
insert overwrite table tmptable_n10
  select unionsrc.key, unionsrc.value FROM (select 'tst1' as key, count(1) as value from src s1
                                        UNION  ALL  
                                            select 'tst2' as key, count(1) as value from src1 s2
                                        UNION ALL
                                            select 'tst3' as key, count(1) as value from srcbucket s3) unionsrc;


insert overwrite table tmptable_n10
  select unionsrc.key, unionsrc.value FROM (select 'tst1' as key, count(1) as value from src s1
                                        UNION  ALL  
                                            select 'tst2' as key, count(1) as value from src1 s2
                                        UNION ALL
                                            select 'tst3' as key, count(1) as value from srcbucket s3) unionsrc;

select * from tmptable_n10 x sort by x.key;
