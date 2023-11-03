set hive.vectorized.execution.enabled=false;

-- Truncate table on an unpartitioned table must result in an exception.
drop table if exists test_truncate_neg2;
create external table test_truncate_neg2 (id int, value string) stored by iceberg stored as orc;
alter table test_truncate_neg2 set tblproperties('external.table.purge'='true');
insert into test_truncate_neg2 values (1, 'one'),(2,'two'),(3,'three'),(4,'four'),(5,'five'); 

truncate test_truncate_neg2 partition(id=1);