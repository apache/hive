explain 
  select count(1) FROM (select s1.key as key, s1.value as value from src s1 UNION  ALL  
                        select s2.key as key, s2.value as value from src s2) unionsrc;

select count(1) FROM (select s1.key as key, s1.value as value from src s1 UNION  ALL  
                      select s2.key as key, s2.value as value from src s2) unionsrc;
