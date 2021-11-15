create temporary table alter_table_partition_clusterby_sortby_temp (a int, b int) partitioned by (c string) clustered by (a, b) sorted by (a desc, b asc) into 4 buckets;
alter table alter_table_partition_clusterby_sortby_temp add partition(c='abc');

-- Turn off sorting for a partition

alter table alter_table_partition_clusterby_sortby_temp partition(c='abc') not sorted;
desc formatted alter_table_partition_clusterby_sortby_temp partition(c='abc');

-- Modify clustering for a partition

alter table alter_table_partition_clusterby_sortby_temp partition(c='abc') clustered by (b) sorted by (b desc) into 4 buckets;
desc formatted alter_table_partition_clusterby_sortby_temp partition(c='abc');

-- Turn off clustering for a partition

alter table alter_table_partition_clusterby_sortby_temp partition(c='abc') not clustered;
desc formatted alter_table_partition_clusterby_sortby_temp partition(c='abc');

-- Table properties should be unchanged

desc formatted alter_table_partition_clusterby_sortby_temp;

drop table alter_table_partition_clusterby_sortby_temp;
