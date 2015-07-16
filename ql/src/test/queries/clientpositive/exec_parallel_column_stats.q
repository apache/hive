set hive.exec.parallel=true;

explain analyze table src compute statistics for columns;

analyze table src compute statistics for columns;