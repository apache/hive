set hive.cbo.enable=false;

-- no agg, corr
explain rewrite
select * 
from src b 
where exists 
  (select a.key 
  from src a 
  where b.value = a.value  and a.key = b.key and a.value > 'val_9'
  )
;

-- sq in from
explain rewrite
select * 
from (select * 
      from src b 
      where exists 
          (select a.key 
          from src a 
          where b.value = a.value  and a.key = b.key and a.value > 'val_9')
     ) a
;