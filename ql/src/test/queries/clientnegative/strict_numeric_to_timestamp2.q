set hive.strict.timestamp.conversion=true;
create table t (a integer);
select cast(a as timestamp) from t;
