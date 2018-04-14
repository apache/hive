--! qt:dataset:alltypesorc
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.optimize.index.filter=true;
set hive.fetch.task.conversion=none;

explain vectorization SELECT AVG(cbigint) FROM alltypesorc WHERE cbigint < cdouble;
SELECT AVG(cbigint) FROM alltypesorc WHERE cbigint < cdouble;
