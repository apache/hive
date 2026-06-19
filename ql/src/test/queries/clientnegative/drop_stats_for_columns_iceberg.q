create table t1(i int, j int, k int) stored by iceberg;

alter table t1 drop statistics for columns;
