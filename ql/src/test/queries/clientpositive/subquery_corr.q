--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;

-- inner query has non-equi correlated predicate, this shouldn't have value gen
explain select * from src b where b.key in (select key from src a where b.value > a.value);
select * from src b where b.key in (select key from src a where b.value > a.value);

explain select * from src b where b.key in (select key from src a where b.value <= a.value);
select * from src b where b.key in (select key from src a where b.value <= a.value);

explain select * from src b where b.key in (select key from src a where b.value > a.value and b.key < a.key) ;
select * from src b where b.key in (select key from src a where b.value > a.value and b.key < a.key) ;
