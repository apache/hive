--! qt:dataset:src
USE default;

-- Test of hive.exec.max.dynamic.partitions.pernode

CREATE TABLE max_parts(key STRING) PARTITIONED BY (value STRING);

set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=10;

-- Tez may create multiple tasks for the final reducer. Sometimes more than
-- one of them fail approximately the same time which affects the error output.
-- In order to get a predictable error, we set the number of reducers to 1 here.
set mapred.reduce.tasks=1;

INSERT OVERWRITE TABLE max_parts PARTITION(value)
SELECT key, value
FROM src
LIMIT 50;
