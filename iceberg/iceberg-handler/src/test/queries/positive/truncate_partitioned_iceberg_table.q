-- SORT_QUERY_RESULTS
-- Mask the totalSize value as it can have slight variability, causing test flakiness
--! qt:replace:/(\s+totalSize\s+)\S+(\s+)/$1#Masked#$2/
-- Mask random uuid
--! qt:replace:/(\s+uuid\s+)\S+(\s*)/$1#Masked#$2/
-- Mask a random snapshot id
--! qt:replace:/(\s+current-snapshot-id\s+)\S+(\s*)/$1#Masked#/
-- Mask added file size
--! qt:replace:/(\S\"added-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask total file size
--! qt:replace:/(\S\"total-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask current-snapshot-timestamp-ms
--! qt:replace:/(\s+current-snapshot-timestamp-ms\s+)\S+(\s*)/$1#Masked#$2/
-- Mask removed file size
--! qt:replace:/(\S\"removed-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/

set hive.vectorized.execution.enabled=false;

drop table if exists test_truncate;
create external table test_truncate(a int) partitioned by (b string) stored as avro;
alter table test_truncate set tblproperties('external.table.purge'='true');
insert into table test_truncate partition (b='one') values (1), (2), (3);
insert into table test_truncate partition (b='two') values (4), (5);
insert into table test_truncate partition (b='three') values (6), (7), (8);
insert into table test_truncate partition (b='four') values (9);
alter table test_truncate convert to iceberg;

analyze table test_truncate compute statistics;
describe formatted test_truncate;
select * from test_truncate;

truncate test_truncate;
select count(*) from test_truncate;
select * from test_truncate;
describe formatted test_truncate;

drop table if exists test_truncate;