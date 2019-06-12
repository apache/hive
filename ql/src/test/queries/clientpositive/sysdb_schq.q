--! qt:dataset:src

source ../../metastore/scripts/upgrade/hive/hive-schema-4.0.0.hive.sql;

use sys;

-- set hive.metastore.scheduled.queries.cron.syntax=QUARTZ;

select bucket_col_name, integer_idx from bucketing_cols order by bucket_col_name, integer_idx limit 5;

create scheduled query asd cron '* * * * * ? *' defined as select 1;



select * from scheduled_queries;
