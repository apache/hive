
drop table foo;

create table foo (i int) partitioned by (s string);

insert overwrite table foo partition(s='foo') select cint from alltypesorc limit 10;
insert overwrite table foo partition(s='bar') select cint from alltypesorc limit 10;

explain select * from foo where s not in ('bar');
select * from foo where s not in ('bar');


drop table foo;