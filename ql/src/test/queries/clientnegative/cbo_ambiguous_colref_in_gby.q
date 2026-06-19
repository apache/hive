create table t1 (a int);

explain cbo
select s.a from
  (select a, a from t1) s
group by s.a
;
