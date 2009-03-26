set hive.map.aggr = true;

-- union case: 1 subquery is a map-reduce job, different inputs for sub-queries, followed by filesink

explain 
  select unionsrc.key, unionsrc.value FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                                        UNION  ALL  
                                            select s2.key as key, s2.value as value from src1 s2) unionsrc;


select unionsrc.key, unionsrc.value FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                                      UNION  ALL  
                                          select s2.key as key, s2.value as value from src1 s2) unionsrc;

