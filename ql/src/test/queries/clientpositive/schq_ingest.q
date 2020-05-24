--! qt:authorizer
--! qt:scheduledqueryservice
--! qt:sysdb

set user.name=hive_admin_user;
set role admin;

drop table if exists t;
drop table if exists s;
 
-- suppose that this table is an external table or something
-- which supports the pushdown of filter condition on the id column
create table s(id integer, cnt integer);
 
-- create an internal table and an offset table
create table t(id integer, cnt integer);
create table t_offset(offset integer);
insert into t_offset values(0);
 
-- pretend that data is added to s
insert into s values(1,1);
 
-- run an ingestion...
from (select id==offset as first,* from s
join t_offset on id>=offset) s1
insert into t select id,cnt where not first
insert overwrite table t_offset select max(s1.id);
 
-- configure to run ingestion - in the far future
create scheduled query ingest cron '0 0 0 1 * ? 2030' defined as
from (select id==offset as first,* from s
join t_offset on id>=offset) s1
insert into t select id,cnt where not first
insert overwrite table t_offset select max(s1.id);
 
-- add some new values
insert into s values(2,2),(3,3);
 
-- pretend that a timeout have happened
alter scheduled query ingest execute;

!sleep 30;
select state,error_message from sys.scheduled_executions;

select * from t order by id;
