drop table tstsrc;
create table tstsrc like src;
insert overwrite table tstsrc select key, value from src;

SHOW LOCKS;

LOCK TABLE tstsrc shared;
SHOW LOCKS;
UNLOCK TABLE tstsrc;
SHOW LOCKS;
lock TABLE tstsrc SHARED;
SHOW LOCKS;
LOCK TABLE tstsrc SHARED;
SHOW LOCKS;
UNLOCK TABLE tstsrc;
SHOW LOCKS;

drop table tstsrc;
