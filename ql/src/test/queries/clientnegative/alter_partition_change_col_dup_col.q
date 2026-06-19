create table alter_partition_change_col_dup_col (c1 string, c2 decimal(10,0)) partitioned by (p1 string);
alter table alter_partition_change_col_dup_col add partition (p1='abc');
-- should fail because of duplicate name c1
alter table alter_partition_change_col_dup_col change c2 c1 decimal(14,4);
