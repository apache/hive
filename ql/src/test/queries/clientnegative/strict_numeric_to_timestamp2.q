set hive.strict.timestamp.conversion=true;
create table t (a integer);
insert into t values(123);
select cast(a as timestamp) from t;
