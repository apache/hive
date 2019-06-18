set hive.metastore.disallow.incompatible.col.type.changes=false;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- SORT_QUERY_RESULTS

create table alter_partition_change_col0 (c1 string, c2 string);
load data local inpath '../../data/files/dec.txt' overwrite into table alter_partition_change_col0;

create table alter_partition_change_col1 (c1 string, c2 string) partitioned by (p1 string comment 'Column p1', p2 string comment 'Column p2');

insert overwrite table alter_partition_change_col1 partition (p1, p2)
  select c1, c2, 'abc', '123' from alter_partition_change_col0
  union all
  select c1, c2, cast(null as string), '123' from alter_partition_change_col0;
  
show partitions alter_partition_change_col1;
select * from alter_partition_change_col1 where p1='abc';
select * from alter_partition_change_col1 where p1='__HIVE_DEFAULT_PARTITION__';
select * from alter_partition_change_col1 where p1='__HIVE_DEFAULT_PARTITION__' or lower(p1)='a';

-- Change c2 to decimal(10,0)
alter table alter_partition_change_col1 change c2 c2 decimal(10,0);
alter table alter_partition_change_col1 partition (p1='abc', p2='123') change c2 c2 decimal(10,0);
alter table alter_partition_change_col1 partition (p1='__HIVE_DEFAULT_PARTITION__', p2='123') change c2 c2 decimal(10,0);
select * from alter_partition_change_col1 where p1='abc';
select * from alter_partition_change_col1 where p1='__HIVE_DEFAULT_PARTITION__';

-- Change the column type at the table level. Table-level describe shows the new type, but the existing partition does not.
alter table alter_partition_change_col1 change c2 c2 decimal(14,4);
describe alter_partition_change_col1;
describe alter_partition_change_col1 partition (p1='abc', p2='123');
select * from alter_partition_change_col1 where p1='abc';
select * from alter_partition_change_col1 where p1='__HIVE_DEFAULT_PARTITION__';

-- change the comment on a partition column without changing type or renaming it
alter table alter_partition_change_col1 partition column (p1 string comment 'Changed comment for p1');
describe alter_partition_change_col1;

-- now change the column type of the existing partition
alter table alter_partition_change_col1 partition (p1='abc', p2='123') change c2 c2 decimal(14,4);
describe alter_partition_change_col1 partition (p1='abc', p2='123');
select * from alter_partition_change_col1 where p1='abc';
select * from alter_partition_change_col1 where p1='__HIVE_DEFAULT_PARTITION__';

-- change column for default partition value
alter table alter_partition_change_col1 partition (p1='__HIVE_DEFAULT_PARTITION__', p2='123') change c2 c2 decimal(14,4);
describe alter_partition_change_col1 partition (p1='__HIVE_DEFAULT_PARTITION__', p2='123');
select * from alter_partition_change_col1 where p1='abc';
select * from alter_partition_change_col1 where p1='__HIVE_DEFAULT_PARTITION__';

-- Try out replace columns
alter table alter_partition_change_col1 partition (p1='abc', p2='123') replace columns (c1 string);
describe alter_partition_change_col1;
describe alter_partition_change_col1 partition (p1='abc', p2='123');
select * from alter_partition_change_col1 where p1='abc';
select * from alter_partition_change_col1 where p1='__HIVE_DEFAULT_PARTITION__';

alter table alter_partition_change_col1 replace columns (c1 string);
describe alter_partition_change_col1;
select * from alter_partition_change_col1 where p1='abc';
select * from alter_partition_change_col1 where p1='__HIVE_DEFAULT_PARTITION__';

-- Try add columns
alter table alter_partition_change_col1 add columns (c2 decimal(14,4));
describe alter_partition_change_col1;
describe alter_partition_change_col1 partition (p1='abc', p2='123');
select * from alter_partition_change_col1 where p1='abc';
select * from alter_partition_change_col1 where p1='__HIVE_DEFAULT_PARTITION__';

alter table alter_partition_change_col1 partition (p1='abc', p2='123') add columns (c2 decimal(14,4));
describe alter_partition_change_col1 partition (p1='abc', p2='123');
select * from alter_partition_change_col1 where p1='abc';
select * from alter_partition_change_col1 where p1='__HIVE_DEFAULT_PARTITION__';

-- Try changing column for all partitions at once
alter table alter_partition_change_col1 partition (p1, p2='123') change column c2 c2 decimal(10,0);
describe alter_partition_change_col1 partition (p1='abc', p2='123');
describe alter_partition_change_col1 partition (p1='__HIVE_DEFAULT_PARTITION__', p2='123');
select * from alter_partition_change_col1 where p1='abc';
select * from alter_partition_change_col1 where p1='__HIVE_DEFAULT_PARTITION__';
