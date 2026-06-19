--! qt:dataset:src

set hive.vectorized.execution.enabled=false;
set hive.stats.column.autogather=false;
set hive.exec.mode.local.auto=false;
set mapred.reduce.tasks = 10;

-- This test sets number of mapred tasks to 10 for a database with 50 buckets, 
-- and uses a post-hook to confirm that 10 tasks were created

CREATE TABLE bucket_nr(key int, value string) CLUSTERED BY (key) INTO 50 BUCKETS;

explain extended insert overwrite table bucket_nr
  select * from src;
insert overwrite table bucket_nr
select * from src;

drop table bucket_nr;
