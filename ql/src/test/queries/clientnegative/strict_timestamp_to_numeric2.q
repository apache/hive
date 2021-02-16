set hive.strict.checks.type.safety=true;
create table t(a timestamp);
select cast(a as integer) from t;
