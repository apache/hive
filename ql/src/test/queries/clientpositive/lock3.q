--! qt:dataset:srcpart
drop table tstsrcpart_n4;
create table tstsrcpart_n4 like srcpart;

from srcpart
insert overwrite table tstsrcpart_n4 partition (ds='2008-04-08',hr='11')
select key, value where ds='2008-04-08' and hr='11';

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;


from srcpart
insert overwrite table tstsrcpart_n4 partition (ds, hr) select key, value, ds, hr where ds <= '2008-04-08';

from srcpart
insert overwrite table tstsrcpart_n4 partition (ds ='2008-04-08', hr) select key, value, hr where ds = '2008-04-08';


SHOW LOCKS;
SHOW LOCKS tstsrcpart_n4;

drop table tstsrcpart_n4;

drop table tst1_n4;
create table tst1_n4 (key string, value string) partitioned by (a string, b string, c string, d string);


from srcpart
insert overwrite table tst1_n4 partition (a='1', b='2', c, d) select key, value, ds, hr where ds = '2008-04-08';


drop table tst1_n4;
