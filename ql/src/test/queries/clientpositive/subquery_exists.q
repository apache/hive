set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
-- SORT_QUERY_RESULTS

-- no agg, corr
-- SORT_QUERY_RESULTS
explain
select * 
from src b 
where exists 
  (select a.key 
  from src a 
  where b.value = a.value  and a.key = b.key and a.value > 'val_9'
  )
;

select * 
from src b 
where exists 
  (select a.key 
  from src a 
  where b.value = a.value  and a.key = b.key and a.value > 'val_9'
  )
;

-- view test
create view cv1 as 
select * 
from src b 
where exists
  (select a.key 
  from src a 
  where b.value = a.value  and a.key = b.key and a.value > 'val_9')
;

select * from cv1
;

-- sq in from
select * 
from (select * 
      from src b 
      where exists 
          (select a.key 
          from src a 
          where b.value = a.value  and a.key = b.key and a.value > 'val_9')
     ) a
;
