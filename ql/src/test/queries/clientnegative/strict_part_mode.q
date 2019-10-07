--! qt:dataset:src
SET hive.exec.dynamic.partition.mode=strict;

create table strict_partition(key string) partitioned by (value string);
insert into table strict_partition partition(value) select key, value from src;
