set hive.explain.user=false;

drop table if exists p1_n0;
drop table if exists t_n32;

create table t_n32 (a int);
insert into t_n32 values (1);

create table p1_n0 (a int) partitioned by (p int);

insert into p1_n0 partition (p=1) values (1);
insert into p1_n0 partition (p=2) values (1);

truncate table p1_n0;

insert into p1_n0 partition (p=1) values (1);

explain
select * from p1_n0 join t_n32 on (t_n32.a=p1_n0.a);

describe formatted p1_n0;

