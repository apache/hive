set hive.strict.checks.type.safety=true;
create table t(a struct<t:timestamp>);
select cast(a.t as integer) from t;
