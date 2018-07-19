--! qt:dataset:src

set hive.vectorized.execution.enabled=false;
set hive.exec.mode.local.auto=false;
set hive.exec.reducers.max = 2;

-- This test sets the maximum number of reduce tasks to 2 for overwriting a
-- table with 3 buckets, and uses a post-hook to confirm that 1 reducer was used

CREATE TABLE test_table_n4(key int, value string) CLUSTERED BY (key) INTO 3 BUCKETS;

explain extended insert overwrite table test_table_n4
  select * from src;
insert overwrite table test_table_n4
select * from src;

drop table test_table_n4;
