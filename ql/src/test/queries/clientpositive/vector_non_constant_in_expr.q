--! qt:dataset:alltypesorc
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

explain vectorization SELECT * FROM alltypesorc WHERE cint in (ctinyint, cbigint);