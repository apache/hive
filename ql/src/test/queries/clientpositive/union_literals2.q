


create table t (a string, b string);
insert into t values(9,9);

explain
  select cast(a as integer) from t
union all
  select cast(1 as integer)
union all
  select cast(2 as integer)
union all
  select cast(3 as integer)
;

  select cast(a as integer) from t
union all
  select cast(1 as integer)
union all
  select cast(2 as integer)
union all
  select cast(3 as integer)
;

  select cast(a as integer) from t;
