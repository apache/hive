--! qt:authorizer
--! qt:scheduledqueryservice
--! qt:dataset:src
--! qt:sysdb

set user.name=hive_admin_user;
set role admin;

create scheduled query asd cron '* * * * * ? *' defined as select 1;

!sleep 10;

use sys;

desc formatted scheduled_queries;

select 
	scheduled_query_id,
	schedule_name,
	enabled,
	cluster_namespace,
	`schedule`,
	`user`,
	query,
	next_execution>0
 from scheduled_queries;

select	scheduled_execution_id,
	scheduled_query_id,
	state,
	start_time>0,
	end_time>=start_time,
	error_message,
	last_update_time>=start_time
		from scheduled_executions order by SCHEDULED_EXECUTION_ID limit 1;

alter scheduled query asd disable;

select schedule_name,enabled from scheduled_queries;

!sleep 1;
