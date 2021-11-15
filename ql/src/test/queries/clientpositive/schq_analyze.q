--! qt:authorizer
--! qt:scheduledqueryservice
--! qt:sysdb

set user.name=hive_admin_user;
set role admin;

-- create external table
create external table t (a integer);
 
-- disable autogather
set hive.stats.autogather=false;
 
insert into t values (1),(2),(3);

-- basic stats show that the table has "0" rows
desc formatted t;

-- create a schedule to compute stats in the far future
create scheduled query t_analyze cron '0 0 0 1 * ? 2030' as analyze table t compute statistics for columns;

alter scheduled query t_analyze execute;

!sleep 30;
 
select * from information_schema.scheduled_executions s where schedule_name='ex_analyze' order by scheduled_execution_id desc limit 3;
 
-- and the numrows have been updated
desc formatted t;
 

