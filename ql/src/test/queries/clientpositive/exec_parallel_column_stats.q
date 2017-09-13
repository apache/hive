set hive.exec.parallel=true;

create table t as select * from src;

explain analyze table t compute statistics for columns;

analyze table t compute statistics for columns;
