create temporary table default_partition_name_temp (key int, value string) partitioned by (ds string);

alter table default_partition_name_temp set default partition to 'some_other_default_partition_name_temp';

alter table default_partition_name_temp add partition(ds='__HIVE_DEFAULT_PARTITION__');

show partitions default_partition_name_temp;
