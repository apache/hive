set hive.mapred.mode=nonstrict;
set hive.explain.user=false;

create table t1 (v string, k int);
insert into t1 values ('people', 10), ('strangers', 20), ('parents', 30);

create table t2 (v string, k double);
insert into t2 values ('people', 10), ('strangers', 20), ('parents', 30);

-- should not produce exceptions
explain
select * from t1 where t1.k in (select t2.k from t2 where t2.v='people') and t1.k<15;

select * from t1 where t1.k in (select t2.k from t2 where t2.v='people') and t1.k<15;


