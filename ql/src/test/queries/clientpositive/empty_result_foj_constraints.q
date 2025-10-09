set hive.cli.print.header=true;

create table t1 (a int not null, b varchar(10) not null);
create table t2 (c int not null, d varchar(10) not null);

insert into t1 values (2, 'four'), (1, 'four');
insert into t2 values (1, 'three'), (3, 'three');


explain cbo
select * from (select a, b from t1 where 0=1) s
full outer join t2 on s.a = t2.c;

select * from (select a, b from t1 where 0=1) s
full outer join t2 on s.a = t2.c;


explain cbo
select * from t1
full outer join (select c, d from t2 where 0=1) s on t1.a = s.c;

select * from t1
full outer join (select c, d from t2 where 0=1) s on t1.a = s.c;
