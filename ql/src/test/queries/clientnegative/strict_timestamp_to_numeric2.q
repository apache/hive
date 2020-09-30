set hive.strict.timestamp.conversion=true;
create table t(a timestamp);
insert into t values ('2011-11-11');

select cast(a as integer) from t;
