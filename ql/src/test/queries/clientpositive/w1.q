set hive.fetch.task.conversion=none;

create table t (a string);

insert into t values ('a'),('b'),('c');

explain analyze
select a from t;

explain analyze
select substr(a,1,4) from t;
