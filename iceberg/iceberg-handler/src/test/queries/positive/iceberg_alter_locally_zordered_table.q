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
set hive.vectorized.execution.enabled=true;

-- Test ALTER TABLE SET WRITE [LOCALLY] ORDERED BY ZORDER
create table ice_orc_zorder (id int, name string, age int, city string) stored by iceberg stored as orc;

describe formatted ice_orc_zorder;

explain insert into ice_orc_zorder values (1, 'Alice', 30, 'NYC'),(2, 'Bob', 25, 'LA'),(3, 'Charlie', 35, 'SF');

-- Add Z-order via ALTER TABLE command
alter table ice_orc_zorder set WRITE ORDERED BY ZORDER (id, age);

describe formatted ice_orc_zorder;

explain insert into ice_orc_zorder values (4, 'David', 28, 'Seattle'),(5, 'Eve', 32, 'Boston'),(6, 'Frank', 29, 'Austin'),(7, 'Grace', 32, 'Denver');
insert into ice_orc_zorder values (4, 'David', 28, 'Seattle'),(5, 'Eve', 32, 'Boston'),(6, 'Frank', 29, 'Austin'),(7, 'Grace', 32, 'Denver');
select * from ice_orc_zorder;

drop table ice_orc_zorder;
