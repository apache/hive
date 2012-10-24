SET hive.metastore.pre.event.listeners=org.apache.hadoop.hive.metastore.PartitionNameWhitelistPreEventListener;
SET hive.metastore.partition.name.whitelist.pattern=[A-Za-z]*;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

create table source_table like srcpart;

create table dest_table like srcpart;

load data local inpath '../data/files/srcbucket20.txt' INTO TABLE source_table partition(ds='2008-04-08', hr=11);

insert overwrite table dest_table partition (ds, hr) select key, value, hr from source_table where ds='2008-04-08';

