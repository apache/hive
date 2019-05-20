set hive.mapred.mode=nonstrict;

create table t1_n53 (key int, data struct<name:string, id: string>, value string);

explain analyze table t1_n53 compute statistics for columns;

analyze table t1_n53 compute statistics for columns;

desc formatted t1_n53 value;
