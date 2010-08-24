drop table tstsrc;
create table tstsrc like src;
insert overwrite table tstsrc select key, value from src;

LOCK TABLE tstsrc SHARED;
LOCK TABLE tstsrc SHARED;
LOCK TABLE tstsrc EXCLUSIVE;
