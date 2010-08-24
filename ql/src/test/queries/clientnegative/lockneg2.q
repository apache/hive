drop table tstsrc;
create table tstsrc like src;
insert overwrite table tstsrc select key, value from src;

UNLOCK TABLE tstsrc;
