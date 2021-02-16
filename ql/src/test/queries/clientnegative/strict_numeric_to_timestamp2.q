set hive.strict.checks.type.safety=true;
create table t (a integer);
select cast(a as timestamp) from t;
