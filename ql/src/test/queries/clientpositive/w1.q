set hive.fetch.task.conversion=none;

create table t (a string);

insert into t values 
('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'),
('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'),
('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');

explain analyze
select a from t;

explain analyze
select substr(a,1,4) from t;
