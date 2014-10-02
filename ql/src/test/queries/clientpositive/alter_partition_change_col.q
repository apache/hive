SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

create table alter_partition_change_col0 (c1 string, c2 string);
load data local inpath '../../data/files/dec.txt' overwrite into table alter_partition_change_col0;

create table alter_partition_change_col1 (c1 string, c2 string) partitioned by (p1 string);

insert overwrite table alter_partition_change_col1 partition (p1)
  select c1, c2, 'abc' from alter_partition_change_col0
  union all
  select c1, c2, null from alter_partition_change_col0;
  
show partitions alter_partition_change_col1;
select * from alter_partition_change_col1;

-- Change c2 to decimal(10,0)
alter table alter_partition_change_col1 change c2 c2 decimal(10,0);
alter table alter_partition_change_col1 partition (p1='abc') change c2 c2 decimal(10,0);
alter table alter_partition_change_col1 partition (p1='__HIVE_DEFAULT_PARTITION__') change c2 c2 decimal(10,0);
select * from alter_partition_change_col1;

-- Change the column type at the table level. Table-level describe shows the new type, but the existing partition does not.
alter table alter_partition_change_col1 change c2 c2 decimal(14,4);
describe alter_partition_change_col1;
describe alter_partition_change_col1 partition (p1='abc');
select * from alter_partition_change_col1;

-- now change the column type of the existing partition
alter table alter_partition_change_col1 partition (p1='abc') change c2 c2 decimal(14,4);
describe alter_partition_change_col1 partition (p1='abc');
select * from alter_partition_change_col1;

-- change column for default partition value
alter table alter_partition_change_col1 partition (p1='__HIVE_DEFAULT_PARTITION__') change c2 c2 decimal(14,4);
describe alter_partition_change_col1 partition (p1='__HIVE_DEFAULT_PARTITION__');
select * from alter_partition_change_col1;

-- Try out replace columns
alter table alter_partition_change_col1 partition (p1='abc') replace columns (c1 string);
describe alter_partition_change_col1;
describe alter_partition_change_col1 partition (p1='abc');
select * from alter_partition_change_col1;
alter table alter_partition_change_col1 replace columns (c1 string);
describe alter_partition_change_col1;
select * from alter_partition_change_col1;

-- Try add columns
alter table alter_partition_change_col1 add columns (c2 decimal(14,4));
describe alter_partition_change_col1;
describe alter_partition_change_col1 partition (p1='abc');
select * from alter_partition_change_col1;

alter table alter_partition_change_col1 partition (p1='abc') add columns (c2 decimal(14,4));
describe alter_partition_change_col1 partition (p1='abc');
select * from alter_partition_change_col1;

