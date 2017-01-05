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
