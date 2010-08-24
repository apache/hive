drop table tstsrc;
create table tstsrc like src;
insert overwrite table tstsrc select key, value from src;

drop table tstsrcpart;
create table tstsrcpart like srcpart;

insert overwrite table tstsrcpart partition (ds='2008-04-08', hr='11')
select key, value from srcpart where ds='2008-04-08' and hr='11';

LOCK TABLE tstsrc SHARED;
LOCK TABLE tstsrcpart SHARED;
LOCK TABLE tstsrcpart PARTITION(ds='2008-04-08', hr='11') EXCLUSIVE;
SHOW LOCKS;
UNLOCK TABLE tstsrc;
SHOW LOCKS;
UNLOCK TABLE tstsrcpart;
SHOW LOCKS;
UNLOCK TABLE tstsrcpart PARTITION(ds='2008-04-08', hr='11');
SHOW LOCKS;


drop table tstsrc;
drop table tstsrcpart;
