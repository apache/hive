--! qt:dataset:part
--! qt:dataset:alltypesorc
set hive.merge.nway.joins=false;

drop table foo_n1;

create table foo_n1 (i int) partitioned by (s string);
insert overwrite table foo_n1 partition(s='foo_n1') select cint from alltypesorc limit 10;
insert overwrite table foo_n1 partition(s='bar') select cint from alltypesorc limit 10;

explain
select *
from foo_n1 f1
join part p1 on (p1.p_partkey = f1.i)
join foo_n1 f2 on (f1.i = f2.i)
where f1.s='foo_n1' and f2.s='bar';

explain
select *
from foo_n1 f1
join part p1 on (p1.p_partkey = f1.i)
join foo_n1 f2 on (f1.i = f2.i)
where f1.s='foo_n1' and f2.s='foo_n1';

drop table foo_n1;
