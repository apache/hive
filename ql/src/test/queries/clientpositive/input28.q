--! qt:dataset:src
set hive.mapred.mode=nonstrict;

create table tst_n0(a string, b string) partitioned by (d string);
alter table tst_n0 add partition (d='2009-01-01');

insert overwrite table tst_n0 partition(d='2009-01-01')
select tst_n0.a, src.value from tst_n0 join src ON (tst_n0.a = src.key);

select * from tst_n0 where tst_n0.d='2009-01-01';


