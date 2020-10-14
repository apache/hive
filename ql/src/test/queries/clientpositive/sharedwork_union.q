
create table t (a integer);

explain
select min(a) from t union all select max(a) from t;

select min(a) from t union all select max(a) from t;
