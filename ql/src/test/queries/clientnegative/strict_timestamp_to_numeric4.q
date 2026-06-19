set hive.strict.timestamp.conversion=true;
create table t(a timestamp);
select 1 from t where a=1000;
