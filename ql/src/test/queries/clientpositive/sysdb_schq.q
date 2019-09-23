--! qt:dataset:src

source ../../metastore/scripts/upgrade/hive/hive-schema-4.0.0.hive.sql;

use sys;

-- FIXME: fix this in qtests? service has already started...hard to change it
-- set hive.metastore.scheduled.queries.cron.syntax=QUARTZ;

-- select bucket_col_name, integer_idx from bucketing_cols order by bucket_col_name, integer_idx limit 5;

create scheduled query asd cron '* * * * * ? *' defined as select 1;

!sleep 2;

desc formatted scheduled_queries;

select 
	*
 from scheduled_queries;
select	scheduled_execution_id,
	scheduled_query_id,
	instr(executor_query_id,'_'),
	state,
	start_time>0,
	end_time>=start_time,
	error_message,
	last_update_time>=start_time
		from scheduled_executions order by SCHEDULED_EXECUTION_ID limit 1;
