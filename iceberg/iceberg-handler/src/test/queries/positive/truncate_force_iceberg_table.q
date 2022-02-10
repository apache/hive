-- SORT_QUERY_RESULTS
-- Mask the totalSize value as it can have slight variability, causing test flakiness
--! qt:replace:/(\s+totalSize\s+)\S+(\s+)/$1#Masked#$2/

set hive.vectorized.execution.enabled=false;

drop table if exists test_truncate;
create external table test_truncate (id int, value string) stored by iceberg stored as parquet;
alter table test_truncate set tblproperties('external.table.purge'='false');
insert into test_truncate values (1, 'one'),(2,'two'),(3,'three'),(4,'four'),(5,'five'); 
insert into test_truncate values (6, 'six'), (7, 'seven');
insert into test_truncate values (8, 'eight'), (9, 'nine'), (10, 'ten');
analyze table test_truncate compute statistics;

select * from test_truncate;
describe formatted test_truncate;

truncate test_truncate force;

select count(*) from test_truncate;
select * from test_truncate;
describe formatted test_truncate;

drop table if exists test_truncate;