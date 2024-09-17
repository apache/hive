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
-- Mask number of files
--! qt:replace:/(\s+numFiles\s+)\S+(\s+)/$1#Masked#$2/
-- Mask total data files
--! qt:replace:/(\S\"total-data-files\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
set hive.explain.user=false;
create table testice1000 (a int, b string) stored by iceberg stored as orc;
insert into testice1000 values (11, 'ddd'), (22, 'ttt');
alter table testice1000 set partition spec(truncate(2, b));
insert into testice1000 values (33, 'rrfdfdf');
insert into table testice1000 select * from testice1000;

explain insert into testice1000 partition(b = 'rtyuiy') values (33);
insert into testice1000 partition(b = 'rtyuiy') values (33);
describe formatted testice1000;
select * from testice1000;
