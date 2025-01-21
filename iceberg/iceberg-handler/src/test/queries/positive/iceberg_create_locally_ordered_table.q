-- Mask neededVirtualColumns due to non-strict order
--! qt:replace:/(\s+neededVirtualColumns:\s)(.*)/$1#Masked#/
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
-- Mask removed file size
--! qt:replace:/(\S\"removed-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask current-snapshot-timestamp-ms
--! qt:replace:/(\s+current-snapshot-timestamp-ms\s+)\S+(\s*)/$1#Masked#$2/
--! qt:replace:/(MAJOR\s+succeeded\s+)[a-zA-Z0-9\-\.\s+]+(\s+manual)/$1#Masked#$2/
-- Mask iceberg version
--! qt:replace:/(\S\"iceberg-version\\\":\\\")(\w+\s\w+\s\d+\.\d+\.\d+\s\(\w+\s\w+\))(\\\")/$1#Masked#$3/
set hive.llap.io.enabled=true;
set hive.vectorized.execution.enabled=true;
set hive.optimize.shared.work.merge.ts.schema=true;

create table ice_orc (id int, text string) stored by iceberg stored as orc;

insert into ice_orc values (3, "3"),(2, "2"),(4, "4"),(5, "5"),(1, "1"),(2, "3"),(3,null),(2,null),(null,"a");

describe formatted ice_orc;
describe extended ice_orc;
set hive.fetch.task.conversion=more;
select * from ice_orc;

create table ice_orc_sorted (id int, text string) write locally ordered by id desc nulls first, text asc nulls last stored by iceberg stored as orc;

insert into ice_orc_sorted values (3, "3"),(2, "2"),(4, "4"),(5, "5"),(1, "1"),(2, "3"),(3,null),(2,null),(null,"a");

describe formatted ice_orc_sorted;
describe extended ice_orc_sorted;
set hive.fetch.task.conversion=more;
select * from ice_orc_sorted;

drop table ice_orc;
drop table ice_orc_sorted;

