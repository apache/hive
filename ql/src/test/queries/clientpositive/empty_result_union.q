set hive.cli.print.header=true;

create table t1 (a1 int, b1 string);
create table t2 (a2 int, b2 string);

insert into t1 values (2, 'four'), (1, 'four'), (NULL, NULL), (NULL, 'nothing');
insert into t2 values (1, 'three'), (3, 'three'), (NULL, NULL), (NULL, 'nothing2');

explain cbo
select a2, b2 from t2 where 1=0
union
select a1, b1 from t1;


explain cbo
select a1, b1 from t1
union
select a2, b2 from t2 where 1=0;

explain
select a1, b1 from t1
union
select a2, b2 from t2 where 1=0;

select a1, b1 from t1
union
select a2, b2 from t2 where 1=0;
