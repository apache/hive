set hive.strict.timestamp.conversion=true;
create table t(a struct<t:timestamp>);
select cast(a.t as integer) from t;
