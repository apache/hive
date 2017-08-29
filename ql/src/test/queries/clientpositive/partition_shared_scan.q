set hive.merge.nway.joins=false;

drop table foo;

create table foo (i int) partitioned by (s string);
insert overwrite table foo partition(s='foo') select cint from alltypesorc limit 10;
insert overwrite table foo partition(s='bar') select cint from alltypesorc limit 10;

explain
select *
from foo f1
join part p1 on (p1.p_partkey = f1.i)
join foo f2 on (f1.i = f2.i)
where f1.s='foo' and f2.s='bar';

explain
select *
from foo f1
join part p1 on (p1.p_partkey = f1.i)
join foo f2 on (f1.i = f2.i)
where f1.s='foo' and f2.s='foo';

drop table foo;
