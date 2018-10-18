--! qt:dataset:src
set hive.exec.parallel=true;

create table t_n25 as select * from src;

explain analyze table t_n25 compute statistics for columns;

analyze table t_n25 compute statistics for columns;
