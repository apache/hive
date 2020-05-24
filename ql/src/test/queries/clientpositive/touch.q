--! qt:dataset:srcpart
--! qt:dataset:src
create database tc;

create table tc.tstsrc like default.src;
insert overwrite table tc.tstsrc select key, value from default.src;

create table tc.tstsrcpart like default.srcpart;
insert overwrite table tc.tstsrcpart partition (ds='2008-04-08', hr='12')
select key, value from default.srcpart where ds='2008-04-08' and hr='12';

EXPLAIN ALTER TABLE tc.tstsrc TOUCH;
ALTER TABLE tc.tstsrc TOUCH;
ALTER TABLE tc.tstsrcpart TOUCH;
EXPLAIN ALTER TABLE tc.tstsrcpart TOUCH PARTITION (ds='2008-04-08', hr='12');
ALTER TABLE tc.tstsrcpart TOUCH PARTITION (ds='2008-04-08', hr='12');

drop table tc.tstsrc;
drop table tc.tstsrcpart;

drop database tc;
