set hive.vectorized.execution.enabled=false;

drop table if exists test_truncate_neg1;
create external table test_truncate_neg1 (id int, value string) stored by iceberg stored as orc;
alter table test_truncate_neg1 set tblproperties('external.table.purge'='false');
insert into test_truncate_neg1 values (1, 'one'),(2,'two'),(3,'three'),(4,'four'),(5,'five'); 

truncate test_truncate_neg1;