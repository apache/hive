--! qt:dataset:src

source ../../metastore/scripts/upgrade/hive/hive-schema-4.0.0.hive.sql;

use sys;

select bucket_col_name, integer_idx from bucketing_cols order by bucket_col_name, integer_idx limit 5;

create scheduled query asd cron '1 * * * *' defined as select 1;

select * from scheduled_queries;
