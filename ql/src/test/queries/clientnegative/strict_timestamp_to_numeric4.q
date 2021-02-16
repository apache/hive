set hive.strict.checks.type.safety=true;
create table t(a timestamp);
select 1 from t where a=1000;
