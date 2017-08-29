set hive.mapred.mode=nonstrict;


-- no agg, corr
explain
select * 
from src b 
where not exists 
  (select a.key 
  from src a 
  where b.value = a.value  and a.key = b.key and a.value > 'val_2'
  )
;

select * 
from src b 
where not exists 
  (select a.key 
  from src a 
  where b.value = a.value  and a.key = b.key and a.value > 'val_2'
  )
;

-- distinct, corr
explain
select * 
from src b 
where not exists 
  (select distinct a.key 
  from src a 
  where b.value = a.value and a.value > 'val_2'
  )
;

select * 
from src b 
where not exists 
  (select a.key 
  from src a 
  where b.value = a.value and a.value > 'val_2'
  )
;

-- non equi predicate
explain
select *
from src b
where not exists
  (select a.key
  from src a
  where b.value <> a.value  and a.key > b.key and a.value > 'val_2'
  )
;
select *
from src b
where not exists
  (select a.key
  from src a
  where b.value <> a.value  and a.key > b.key and a.value > 'val_2'
  )
;

--  bug in decorrelation where HiveProject gets multiple column with same name
explain SELECT p1.p_name FROM part p1 LEFT JOIN (select p_type as p_col from part ) p2 WHERE NOT EXISTS
                (select pp1.p_type as p_col from part pp1 where pp1.p_partkey = p2.p_col);
SELECT p1.p_name FROM part p1 LEFT JOIN (select p_type as p_col from part ) p2 WHERE NOT EXISTS
                (select pp1.p_type as p_col from part pp1 where pp1.p_partkey = p2.p_col);
