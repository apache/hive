set hive.explain.user=false;

drop table if exists p1;
drop table if exists t;

create table t (a int);
insert into t values (1);

create table p1 (a int) partitioned by (p int);

insert into p1 partition (p=1) values (1);
insert into p1 partition (p=2) values (1);

truncate table p1;

insert into p1 partition (p=1) values (1);

explain
select * from p1 join t on (t.a=p1.a);

describe formatted p1;

