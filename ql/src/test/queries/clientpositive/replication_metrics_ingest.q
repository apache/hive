--! qt:authorizer
--! qt:scheduledqueryservice
--! qt:sysdb

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/repl/sentinel;
dfs -rmr  ${system:test.tmp.dir}/repl;
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/repl;

set user.name=hive_admin_user;
set role ADMIN;

drop database if exists src cascade;
drop database if exists destination cascade;

create database src with DBPROPERTIES ('repl.source.for' = '1,2,3');

create table src.t(id int, cnt int);

-- add data to table
insert into src.t values(1,1);

create scheduled query repl1 every 15 minutes as repl dump src
with ('hive.repl.rootdir'= '${system:test.tmp.dir}/repl');

alter scheduled query repl1 execute;

!sleep 50;

alter scheduled query repl1 disabled;

create scheduled query repl2 every 15 minutes as repl load src into destination
with ('hive.repl.rootdir'= '${system:test.tmp.dir}/repl');

alter scheduled query repl2 execute;

!sleep 50;

alter scheduled query repl2 disabled;

show databases;

use sys;

select t1.POLICY_NAME, t1.DUMP_EXECUTION_ID, t1.METADATA, t1.PROGRESS, t2.PROGRESS, t1.MESSAGE_FORMAT
from replication_metrics_orig as t1 join replication_metrics as t2 where
t1.scheduled_execution_id=t2.scheduled_execution_id AND t2.progress not like ('%SKIPPED%') order by t1.dump_execution_id;
