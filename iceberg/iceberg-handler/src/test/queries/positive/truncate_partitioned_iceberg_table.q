-- SORT_QUERY_RESULTS
-- Mask the totalSize value as it can have slight variability, causing test flakiness
--! qt:replace:/(\s+totalSize\s+)\S+(\s+)/$1#Masked#$2/

set hive.vectorized.execution.enabled=false;

drop table if exists test_truncate;
create external table test_truncate(a int) partitioned by (b string) stored as avro;
alter table test_truncate set tblproperties('external.table.purge'='true');
insert into table test_truncate partition (b='one') values (1), (2), (3);
insert into table test_truncate partition (b='two') values (4), (5);
insert into table test_truncate partition (b='three') values (6), (7), (8);
insert into table test_truncate partition (b='four') values (9);
alter table test_truncate set tblproperties ('storage_handler'='org.apache.iceberg.mr.hive.HiveIcebergStorageHandler');

analyze table test_truncate compute statistics;
describe formatted test_truncate;
select * from test_truncate;

truncate test_truncate;
select count(*) from test_truncate;
select * from test_truncate;
describe formatted test_truncate;

drop table if exists test_truncate;