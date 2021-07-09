--! qt:authorizer
--! qt:scheduledqueryservice
--! qt:sysdb

set hive.repl.rootdir=${system:test.tmp.dir}/repl;

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

create scheduled query repl2 every 15 minutes as repl load src into destination
with ('hive.repl.rootdir'= '${system:test.tmp.dir}/repl');

alter scheduled query repl2 execute;

!sleep 50;

show databases;

select policy_name, dump_execution_id from sys.replication_metrics;

select count(*) from sys.replication_metrics where scheduled_execution_id > 0;
