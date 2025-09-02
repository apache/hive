
drop table if exists t;
drop table if exists n;

create table t(a string) stored as orc;
create table n(a string) stored as orc;

insert into t values ('a'),('1'),('2'),(null);
insert into n values ('a'),('b'),('1'),('3'),(null);

explain
select n.* from n left outer join t on (n.a=t.a) where cast(t.a as float) is null;
select n.* from n left outer join t on (n.a=t.a) where cast(t.a as float) is null;
select assert_true(count(1)=4) from n left outer join t on (n.a=t.a) where cast(t.a as float) is null;

set hive.auto.convert.anti.join=false;
explain
select n.* from n left outer join t on (n.a=t.a) where cast(t.a as float) is null;
select n.* from n left outer join t on (n.a=t.a) where cast(t.a as float) is null;
select assert_true(count(1)=4) from n left outer join t on (n.a=t.a) where cast(t.a as float) is null;


create table tab1 (col1 int, col2 int, col3 int, col4 int);
create table tab2 (col1 int, col2 int, col3 int, col4 int);

insert into tab1 values (123, 1000, 5000, 9), (456, 1000, 7000, 7), (789, 1000, 5000, 8);
insert into tab2 values (123, 1000, 5000, 2), (456, 1000, 7000, 7), (123, 5000, 4000, 2);

select t1.col1, t1.col2, t1.col3 from tab2 t1
left join tab1 t2
on t2.col3=t1.col2 AND t2.col1=t1.col1
left join tab2 t3
on t3.col1=t1.col1 AND t2.col3=t1.col3
where t1.col4=2 AND t3.col1 is null;

set hive.auto.convert.anti.join=true;

explain CBO select t1.col1, t1.col2, t1.col3 from tab2 t1
left join tab1 t2
on t2.col3=t1.col2 AND t2.col1=t1.col1
left join tab2 t3
on t3.col1=t1.col1 AND t2.col3=t1.col3
where t1.col4=2 AND t3.col1 is null;

select t1.col1, t1.col2, t1.col3 from tab2 t1
left join tab1 t2
on t2.col3=t1.col2 AND t2.col1=t1.col1
left join tab2 t3
on t3.col1=t1.col1 AND t2.col3=t1.col3
where t1.col4=2 AND t3.col1 is null;
