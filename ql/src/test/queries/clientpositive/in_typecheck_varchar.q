
create table ax(s varchar(1),t varchar(10));

insert into ax values ('a','a'),('a','a '),('b','bb');

explain
select 'expected 1',count(*) from ax where s = 'a' and t = 'a';
select 'expected 1',count(*) from ax where s = 'a' and t = 'a';

explain
select 'expected 2',count(*) from ax where (s,t) in (('a','a'),('b','bb'));
select 'expected 2',count(*) from ax where (s,t) in (('a','a'),('b','bb'));

select 'expected 0',count(*) from ax where t = 'a         ';
select 'expected 0',count(*) from ax where t = 'a          ';
select 'expected 0',count(*) from ax where t = 'a          d';
