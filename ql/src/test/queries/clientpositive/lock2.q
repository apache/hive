--! qt:dataset:srcpart
--! qt:dataset:src
drop table tstsrc;
create table tstsrc like src;
insert overwrite table tstsrc select key, value from src;

drop table tstsrcpart_n0;
create table tstsrcpart_n0 like srcpart;

insert overwrite table tstsrcpart_n0 partition (ds='2008-04-08', hr='11')
select key, value from srcpart where ds='2008-04-08' and hr='11';

LOCK TABLE tstsrc SHARED;
LOCK TABLE tstsrcpart_n0 SHARED;
LOCK TABLE tstsrcpart_n0 PARTITION(ds='2008-04-08', hr='11') EXCLUSIVE;
SHOW LOCKS;
SHOW LOCKS tstsrcpart_n0;
SHOW LOCKS tstsrcpart_n0 PARTITION(ds='2008-04-08', hr='11');
SHOW LOCKS tstsrcpart_n0 PARTITION(ds='2008-04-08', hr='11') extended;

UNLOCK TABLE tstsrc;
SHOW LOCKS;
SHOW LOCKS tstsrcpart_n0;
SHOW LOCKS tstsrcpart_n0 PARTITION(ds='2008-04-08', hr='11');

UNLOCK TABLE tstsrcpart_n0;
SHOW LOCKS;
SHOW LOCKS tstsrcpart_n0;
SHOW LOCKS tstsrcpart_n0 PARTITION(ds='2008-04-08', hr='11');

UNLOCK TABLE tstsrcpart_n0 PARTITION(ds='2008-04-08', hr='11');
SHOW LOCKS;
SHOW LOCKS tstsrcpart_n0;
SHOW LOCKS tstsrcpart_n0 PARTITION(ds='2008-04-08', hr='11');


drop table tstsrc;
drop table tstsrcpart_n0;
