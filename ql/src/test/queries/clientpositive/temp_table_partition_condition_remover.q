--! qt:dataset:alltypesorc

drop table foo_n5_temp;

create temporary table foo_n5_temp (i int) partitioned by (s string);

insert overwrite table foo_n5_temp partition(s='foo_n5_temp') select cint from alltypesorc limit 10;
insert overwrite table foo_n5_temp partition(s='bar') select cint from alltypesorc limit 10;

explain select * from foo_n5_temp where s not in ('bar');
select * from foo_n5_temp where s not in ('bar');


drop table foo_n5_temp;