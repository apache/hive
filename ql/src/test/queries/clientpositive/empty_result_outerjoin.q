set hive.cli.print.header=true;
set hive.auto.convert.anti.join=true;

create table t1 (a int, b varchar(10));
create table t2 (c int, d varchar(10));

insert into t1 values (2, 'four'), (1, 'four'), (NULL, NULL), (NULL, 'nothing');
insert into t2 values (1, 'three'), (3, 'three'), (NULL, NULL), (NULL, 'nothing2');


-- LOJ, left branch is empty -> no result
explain cbo
select * from (select a, b from t1 where 0=1) s
left join t2 on s.a = t2.c;

select * from (select a, b from t1 where 0=1) s
left join t2 on s.a = t2.c;


-- LOJ, right branch is empty -> take the left branch only
explain cbo
select * from t1
left join (select c, d from t2 where 1=0) s on t1.a = s.c;

select * from t1
left join (select c, d from t2 where 1=0) s on t1.a = s.c;

-- LOJ, both branches are empty -> no result
explain cbo
select * from (select a, b from t1 where 0=1) s1
left join (select c, d from t2 where 0=1) s2 on s1.a = s2.c;

select * from (select a, b from t1 where 0=1) s1
left join (select c, d from t2 where 0=1) s2 on s1.a = s2.c;



-- ROJ, right branch is empty -> empty result
explain cbo
select * from t1
right join (select c, d from t2 where 1=0) s on t1.a = s.c;

select * from t1
right join (select c, d from t2 where 1=0) s on t1.a = s.c;


-- ROJ, left branch is empty -> take the right branch only
explain cbo
select * from (select a, b from t1 where 0=1) s
right join t2 on s.a = t2.c;

select * from (select a, b from t1 where 0=1) s
right join t2 on s.a = t2.c;


-- LOJ, both branches are empty -> no result
explain cbo
select * from (select a, b from t1 where 0=1) s1
right join (select c, d from t2 where 0=1) s2 on s1.a = s2.c;

select * from (select a, b from t1 where 0=1) s1
right join (select c, d from t2 where 0=1) s2 on s1.a = s2.c;



-- FOJ, left branch is empty -> take the right branch only
explain cbo
select * from (select a, b from t1 where 0=1) s
full outer join t2 on s.a = t2.c;

select * from (select a, b from t1 where 0=1) s
full outer join t2 on s.a = t2.c;


-- LOJ, right branch is empty -> take the left branch only
explain cbo
select * from t1
full outer join (select c, d from t2 where 1=0) s on t1.a = s.c;

select * from t1
full outer join (select c, d from t2 where 1=0) s on t1.a = s.c;

-- FOJ, both branches are empty -> no result
explain cbo
select * from (select a, b from t1 where 0=1) s1
full outer join (select c, d from t2 where 0=1) s2 on s1.a = s2.c;

select * from (select a, b from t1 where 0=1) s1
full outer join (select c, d from t2 where 0=1) s2 on s1.a = s2.c;



-- Anti join, right branch is empty -> take the left branch only
explain cbo
select t1.a from t1 left join (select c, d from t2 where 1=0) s on s.c = t1.a where s.c is null;

select t1.a from t1 left join (select c, d from t2 where 1=0) s on s.c = t1.a where s.c is null;
