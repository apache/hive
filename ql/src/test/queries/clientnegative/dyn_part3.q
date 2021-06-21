--! qt:disabled:HIVE-24767
--! qt:dataset:src
set hive.exec.max.dynamic.partitions=100;
set hive.exec.dynamic.partition=true;

create table nzhang_part( key string) partitioned by (value string);

insert overwrite table nzhang_part partition(value) select key, value from src;
