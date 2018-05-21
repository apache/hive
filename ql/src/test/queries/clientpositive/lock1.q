--! qt:dataset:src
drop table tstsrc_n1;
create table tstsrc_n1 like src;
insert overwrite table tstsrc_n1 select key, value from src;

SHOW LOCKS;
SHOW LOCKS tstsrc_n1;

LOCK TABLE tstsrc_n1 shared;
SHOW LOCKS;
SHOW LOCKS tstsrc_n1;
SHOW LOCKS tstsrc_n1 extended;

UNLOCK TABLE tstsrc_n1;
SHOW LOCKS;
SHOW LOCKS extended;
SHOW LOCKS tstsrc_n1;
lock TABLE tstsrc_n1 SHARED;
SHOW LOCKS;
SHOW LOCKS extended;
SHOW LOCKS tstsrc_n1;
LOCK TABLE tstsrc_n1 SHARED;
SHOW LOCKS;
SHOW LOCKS extended;
SHOW LOCKS tstsrc_n1;
UNLOCK TABLE tstsrc_n1;
SHOW LOCKS;
SHOW LOCKS extended;
SHOW LOCKS tstsrc_n1;
drop table tstsrc_n1;
