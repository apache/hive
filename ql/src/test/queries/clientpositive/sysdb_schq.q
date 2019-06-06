--! qt:dataset:src

source ../../metastore/scripts/upgrade/hive/hive-schema-4.0.0.hive.sql;

use sys;

select bucket_col_name, integer_idx from bucketing_cols order by bucket_col_name, integer_idx limit 5;

select * from scheduled_queries;
