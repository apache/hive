--! qt:dataset:alltypesorc

drop table foo_n5;

create table foo_n5 (i int) partitioned by (s string);

insert overwrite table foo_n5 partition(s='foo_n5') select cint from alltypesorc limit 10;
insert overwrite table foo_n5 partition(s='bar') select cint from alltypesorc limit 10;

explain select * from foo_n5 where s not in ('bar');
select * from foo_n5 where s not in ('bar');


drop table foo_n5;