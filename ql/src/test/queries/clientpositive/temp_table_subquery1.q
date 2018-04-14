--! qt:dataset:src

create temporary table src_temp as select * from src;

-- subquery exists
select *
from src_temp b
where exists
  (select a.key
  from src_temp a
  where b.value = a.value  and a.key = b.key and a.value > 'val_9'
  )
;

-- subquery in
select *
from src_temp
where src_temp.key in (select key from src_temp s1 where s1.key > '9')
;

select b.key, min(b.value)
from src_temp b
group by b.key
having b.key in ( select a.key
                from src_temp a
                where a.value > 'val_9' and a.value = min(b.value)
                )
;

drop table src_temp;
