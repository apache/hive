set hive.mapred.mode=nonstrict;
set hive.explain.user=false;

create table t1_n97 (v string, k int);
insert into t1_n97 values ('people', 10), ('strangers', 20), ('parents', 30);

create table t2_n60 (v string, k double);
insert into t2_n60 values ('people', 10), ('strangers', 20), ('parents', 30);

-- should not produce exceptions
explain
select * from t1_n97 where t1_n97.k in (select t2_n60.k from t2_n60 where t2_n60.v='people') and t1_n97.k<15;

select * from t1_n97 where t1_n97.k in (select t2_n60.k from t2_n60 where t2_n60.v='people') and t1_n97.k<15;


