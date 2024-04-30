-- SORT_QUERY_RESULTS
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
create external table test_truncate (id int, value string) stored by iceberg stored as orc;
alter table test_truncate set tblproperties('external.table.purge'='true');
insert into test_truncate values (1, 'one'),(2,'two'),(3,'three'),(4,'four'),(5,'five'); 
insert into test_truncate values (6, 'six'), (7, 'seven');
insert into test_truncate values (8, 'eight'), (9, 'nine'), (10, 'ten');
analyze table test_truncate compute statistics;

select * from test_truncate;
describe formatted test_truncate;

truncate test_truncate;

select count(*) from test_truncate;
select * from test_truncate;
describe formatted test_truncate;

insert into test_truncate values (1, 'one'),(2,'two'),(3,'three'),(4,'four'),(5,'five'); 
select * from test_truncate;
describe formatted test_truncate;

truncate test_truncate;

select count(*) from test_truncate;
select * from test_truncate;
describe formatted test_truncate;

insert into test_truncate values (1, 'one'),(2,'two'),(3,'three'),(4,'four'),(5,'five');
alter table test_truncate set tblproperties('external.table.purge'='false');

truncate test_truncate;

select count(*) from test_truncate;
select * from test_truncate;
describe formatted test_truncate;

drop table if exists test_truncate;