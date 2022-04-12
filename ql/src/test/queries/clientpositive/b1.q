
drop table if exists t;
drop table if exists n;

create table t(a string) stored as orc;
create table n(a string) stored as orc;

insert into t values ('a'),('1'),('2'),(null);
insert into n values ('a'),('b'),('1'),('3'),(null);


explain select n.* from n left outer join t on (n.a=t.a) where assert_true(t.a is null) is null;
explain select n.* from n left outer join t on (n.a=t.a) where cast(t.a as float) is null;


select n.* from n left outer join t on (n.a=t.a) where cast(t.a as float) is null;
set hive.auto.convert.anti.join=false;
select n.* from n left outer join t on (n.a=t.a) where cast(t.a as float) is null;


--explain select n.* from n left outer join t on (n.a=t.a) where coalesce(t.a,t.b) is null;

-- explain insert into t select n.* from n left outer join t on (n.a=t.a) where assert_true(t.a is null);

