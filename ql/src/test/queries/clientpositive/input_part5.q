--! qt:dataset:srcpart
set hive.mapred.mode=nonstrict;

create table tmptable_n2(key string, value string, hr string, ds string);

EXPLAIN
insert overwrite table tmptable_n2
SELECT x.* FROM SRCPART x WHERE x.ds = '2008-04-08' and x.key < 100;

insert overwrite table tmptable_n2
SELECT x.* FROM SRCPART x WHERE x.ds = '2008-04-08' and x.key < 100;

select * from tmptable_n2 x sort by x.key,x.value,x.ds,x.hr;

