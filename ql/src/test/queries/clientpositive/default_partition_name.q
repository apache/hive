create table default_partition_name (key int, value string) partitioned by (ds string);

alter table default_partition_name set default partition to 'some_other_default_partition_name';

alter table default_partition_name add partition(ds='__HIVE_DEFAULT_PARTITION__');

show partitions default_partition_name;
