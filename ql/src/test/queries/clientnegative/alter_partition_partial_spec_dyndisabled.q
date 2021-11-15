SET hive.exec.dynamic.partition = true;

create table alter_partition_partial_spec_dyndisabled0 (c1 string) partitioned by (p1 string, p2 string);

alter table alter_partition_partial_spec_dyndisabled0 add partition (p1='abc', p2='123');
alter table alter_partition_partial_spec_dyndisabled0 partition (p1, p2) change c1 c1 int;

describe alter_partition_partial_spec_dyndisabled0 partition (p1='abc', p2='123');

SET hive.exec.dynamic.partition = false;
-- Same statement should fail if dynamic partitioning disabled
alter table alter_partition_partial_spec_dyndisabled0 partition (p1, p2) change c1 c1 int;
