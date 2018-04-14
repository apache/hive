--! qt:dataset:src
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
create view cv1_n1 as 
select * 
from src b 
where exists
  (select a.key 
  from src a 
  where b.value = a.value  and a.key = b.key and a.value > 'val_9')
;

select * from cv1_n1
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
create table t_n12(i int);
insert into t_n12 values(1);
insert into t_n12 values(0);

explain select * from t_n12 where exists (select count(*) from src where 1=2);
select * from t_n12 where exists (select count(*) from src where 1=2);

drop table t_n12;

drop table if exists tx1_n0;
create table tx1_n0 (a integer,b integer);
insert into tx1_n0	values  (1, 1),
                         (1, 2),
                         (1, 3);

select count(*) as result,3 as expected from tx1_n0 u
    where exists (select * from tx1_n0 v where u.a=v.a and u.b <> v.b);
explain select count(*) as result,3 as expected from tx1_n0 u
    where exists (select * from tx1_n0 v where u.a=v.a and u.b <> v.b);

drop table tx1_n0;

create table t1_n68(i int, j int);
insert into t1_n68 values(4,1);

create table t2_n41(i int, j int);
insert into t2_n41 values(4,2),(4,3),(4,5);

explain select * from t1_n68 where t1_n68.i in (select t2_n41.i from t2_n41 where t2_n41.j <> t1_n68.j);
select * from t1_n68 where t1_n68.i in (select t2_n41.i from t2_n41 where t2_n41.j <> t1_n68.j);
drop table t1_n68;
drop table t2_n41;