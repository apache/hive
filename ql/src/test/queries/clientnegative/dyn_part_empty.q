set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.stats.autogether=false;
set hive.error.on.empty.partition=true;

create table dyn_err(key string, value string) partitioned by (ds string);

insert overwrite table dyn_err partition(ds) select key, value, ds from srcpart where ds is not null and key = 'no exists';
