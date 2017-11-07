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

-- upper case in subq
explain
select *
from src b
where exists
  (select a.key
  from src a
  where b.VALUE = a.VALUE
  )
;

-- uncorr exists
explain
select *
from src b
where exists
  (select a.key
  from src a
  where a.value > 'val_9'
  );

select *
from src b
where exists
  (select a.key
  from src a
  where a.value > 'val_9'
  );

-- uncorr, aggregate in sub which produces result irrespective of zero rows
create table t(i int);
insert into t values(1);
insert into t values(0);

explain select * from t where exists (select count(*) from src where 1=2);
select * from t where exists (select count(*) from src where 1=2);

drop table t;

drop table if exists tx1;
create table tx1 (a integer,b integer);
insert into tx1	values  (1, 1),
                         (1, 2),
                         (1, 3);

select count(*) as result,3 as expected from tx1 u
    where exists (select * from tx1 v where u.a=v.a and u.b <> v.b);
explain select count(*) as result,3 as expected from tx1 u
    where exists (select * from tx1 v where u.a=v.a and u.b <> v.b);

drop table tx1;

create table t1(i int, j int);
insert into t1 values(4,1);

create table t2(i int, j int);
insert into t2 values(4,2),(4,3),(4,5);

explain select * from t1 where t1.i in (select t2.i from t2 where t2.j <> t1.j);
select * from t1 where t1.i in (select t2.i from t2 where t2.j <> t1.j);
drop table t1;
drop table t2;