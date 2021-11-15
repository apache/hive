set hive.stats.column.autogather=false;
set hive.mapred.mode=nonstrict;

drop table p;

CREATE TABLE p(a int, b int) partitioned by (c int);

desc formatted p;

insert into p partition (c=1) values (1,2);

desc formatted p partition (c=1) a;

desc formatted p partition (c=1);

explain select max(a) from p where c=1;

analyze table p partition(c=1) compute statistics for columns a;

explain select max(a) from p where c=1;

insert into p partition (c) values (2,3,4);

insert into p partition (c) values (4,5,6);

desc formatted p partition(c=4);

explain select max(a) from p where c=4;

alter table p add partition (c=100);

desc formatted p partition (c=100);

explain select max(a) from p where c=100;

analyze table p partition(c=100) compute statistics for columns a;

explain select max(a) from p where c=100;

desc formatted p partition(c=100);

insert into p partition (c=100) values (1,2);

analyze table p partition(c=100) compute statistics for columns a;

explain select max(a) from p where c=100;

