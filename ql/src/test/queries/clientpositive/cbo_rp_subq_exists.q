--! qt:disabled:disabled by 382dc2084224 in 2016
--! qt:dataset:src_cbo
set hive.cbo.enable=true;
set hive.cbo.returnpath.hiveop=true;
set hive.exec.check.crossproducts=false;

set hive.stats.fetch.column.stats=true;
set hive.auto.convert.join=false;

-- 18. SubQueries Not Exists
-- distinct, corr
select * 
from src_cbo b 
where not exists 
  (select distinct a.key 
  from src_cbo a 
  where b.value = a.value and a.value > 'val_2'
  )
;

-- no agg, corr, having
select * 
from src_cbo b 
group by key, value
having not exists 
  (select a.key 
  from src_cbo a 
  where b.value = a.value  and a.key = b.key and a.value > 'val_12'
  )
;

-- 19. SubQueries Exists
-- view test
create view cv1_n4 as 
select * 
from src_cbo b 
where exists
  (select a.key 
  from src_cbo a 
  where b.value = a.value  and a.key = b.key and a.value > 'val_9')
;

select * from cv1_n4
;

-- sq in from
select * 
from (select * 
      from src_cbo b 
      where exists 
          (select a.key 
          from src_cbo a 
          where b.value = a.value  and a.key = b.key and a.value > 'val_9')
     ) a
;

-- sq in from, having
select *
from (select b.key, count(*) 
  from src_cbo b 
  group by b.key
  having exists 
    (select a.key 
    from src_cbo a 
    where a.key = b.key and a.value > 'val_9'
    )
) a
;

