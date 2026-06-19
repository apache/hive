--! qt:dataset:srcpart
--! qt:dataset:src
set hive.mapred.mode=nonstrict;
drop table tstsrc_n0;

set hive.exec.pre.hooks = org.apache.hadoop.hive.ql.hooks.PreExecutePrinter,org.apache.hadoop.hive.ql.hooks.EnforceReadOnlyTables,org.apache.hadoop.hive.ql.hooks.UpdateInputAccessTimeHook$PreExec;

create table tstsrc_n0 as select * from src;
desc extended tstsrc_n0;
select count(1) from tstsrc_n0;
desc extended tstsrc_n0;
drop table tstsrc_n0;

drop table tstsrcpart_n1;
create table tstsrcpart_n1 like srcpart;

set hive.exec.dynamic.partition=true;


insert overwrite table tstsrcpart_n1 partition (ds, hr) select key, value, ds, hr from srcpart;

desc extended tstsrcpart_n1;
desc extended tstsrcpart_n1 partition (ds='2008-04-08', hr='11');
desc extended tstsrcpart_n1 partition (ds='2008-04-08', hr='12');

select count(1) from tstsrcpart_n1 where ds = '2008-04-08' and hr = '11';

desc extended tstsrcpart_n1;
desc extended tstsrcpart_n1 partition (ds='2008-04-08', hr='11');
desc extended tstsrcpart_n1 partition (ds='2008-04-08', hr='12');

drop table tstsrcpart_n1;

set hive.exec.pre.hooks = org.apache.hadoop.hive.ql.hooks.PreExecutePrinter;

ANALYZE TABLE src COMPUTE STATISTICS;
ANALYZE TABLE src COMPUTE STATISTICS FOR COLUMNS key,value;