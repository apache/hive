--! qt:dataset:src
set hive.support.concurrency=true;
drop table tstsrc;
create table tstsrc like src;
insert overwrite table tstsrc select key, value from src;

set hive.unlock.numretries=0;
UNLOCK TABLE tstsrc;
