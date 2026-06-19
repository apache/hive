create table t (a string);

insert into t values ('1'),('x'),('2.0');

explain
select * from t where a in (1.0,'x',2);
select * from t where a in (1.0,'x',2);


create table ax(s char(1),t char(10));
insert into ax values ('a','a'),('a','a '),('b','bb');
select 'expected 1',count(*) from ax where ((s,t) in (('a','a'),(null, 'bb'))) is null;
