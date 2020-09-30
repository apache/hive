set hive.strict.timestamp.conversion=true;
create table t(a timestamp);
select cast(a as integer) from t;
