-- Truncate table on an partitioned Iceberg V1 table with partition evolution must result in an exception since deletes are not possible on Iceberg V1 table.
create external table test_truncate_part_evolution (id int, value string) stored by iceberg stored as orc tblproperties ('format-version'='1');
insert into test_truncate_part_evolution values (1, 'one'),(2,'two'),(3,'three'),(4,'four'),(5,'five');
alter table test_truncate_part_evolution set partition spec(id);
alter table test_truncate_part_evolution set tblproperties('external.table.purge'='true');
truncate test_truncate_part_evolution partition(id=1);
