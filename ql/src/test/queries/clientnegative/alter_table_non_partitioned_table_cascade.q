drop table if exists alter_table_non_partition_cascade;
create table alter_table_non_partitioned_cascade(c1 string, c2 string);
describe alter_table_non_partitioned_cascade;
alter table alter_table_non_partitioned_cascade add columns (c3 string) cascade;