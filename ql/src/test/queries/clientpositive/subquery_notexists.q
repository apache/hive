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

-- decorrelation should not mangle up the result schema
create table tschema(id int, name string,dept string);
insert into tschema values(1,'a','it'),(2,'b','eee'),(NULL, 'c', 'cse');
explain cbo select distinct 'empno' as eid, a.id from tschema a
    where NOT EXISTS (select c.id from tschema c where a.id=c.id);
select distinct 'empno' as eid, a.id from tschema a
    where NOT EXISTS (select c.id from tschema c where a.id=c.id);
drop table tschema;
