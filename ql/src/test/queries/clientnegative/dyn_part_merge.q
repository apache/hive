set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.mergejob.maponly=false;
set hive.merge.mapfiles=true;

create table dyn_merge(key string, value string) partitioned by (ds string);

insert overwrite table dyn_merge partition(ds) select key, value, ds from srcpart where ds is not null;
