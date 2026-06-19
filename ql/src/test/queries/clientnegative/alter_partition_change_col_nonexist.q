create table alter_partition_change_col_nonexist (c1 string, c2 decimal(10,0)) partitioned by (p1 string);
alter table alter_partition_change_col_nonexist add partition (p1='abc');
-- should fail because of nonexistent column c3
alter table alter_partition_change_col_nonexist change c3 c4 decimal(14,4);

