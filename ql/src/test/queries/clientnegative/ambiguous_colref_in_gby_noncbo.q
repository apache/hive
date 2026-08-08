set hive.cbo.enable=false;
create table t1gnc (a int);
select s.a from (select a, a from t1gnc) s group by s.a;
