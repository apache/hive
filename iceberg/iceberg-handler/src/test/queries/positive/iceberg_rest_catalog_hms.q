-- SORT_QUERY_RESULTS
-- Mask neededVirtualColumns due to non-strict order
--! qt:replace:/(\s+neededVirtualColumns:\s)(.*)/$1#Masked#/
-- Mask random uuid
--! qt:replace:/(\s+'uuid'=')\S+('\s*)/$1#Masked#$2/
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
--! qt:replace:/(MAJOR\s+succeeded\s+)[a-zA-Z0-9\-\.\s+]+(\s+manual)/$1#Masked#$2/
--! qt:replace:/(MAJOR\s+refused\s+)[a-zA-Z0-9\-\.\s+]+(\s+manual)/$1#Masked#$2/
-- Mask removed file size
--! qt:replace:/(\S\"removed-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask iceberg version
--! qt:replace:/(\S\"iceberg-version\\\":\\\")(\w+\s\w+\s\d+\.\d+\.\d+\s\(\w+\s\w+\))(\\\")/$1#Masked#$3/

set hive.stats.autogather=false;
set metastore.client.impl=org.apache.iceberg.hive.client.HiveRESTCatalogClient;
set metastore.catalog.default=ice01;
set iceberg.catalog.ice01.type=rest;

--! This config is set in the driver setup (see TestIcebergRESTCatalogLlapLocalCliDriver.java)   
--! conf.set('iceberg.catalog.ice01.uri', <RESTServer URI>);

create database ice_rest;
use ice_rest;

-----------------------------------------------------------------------------
--! Creating a table without a catalog name in table properties
-----------------------------------------------------------------------------

create table ice_orc1 (
    first_name string, 
    last_name string,
    dept_id bigint,
    team_id bigint
 )
partitioned by (company_id bigint)
stored by iceberg stored as orc;

-----------------------------------------------------------------------------
--! Creating a table with a valid catalog name in table properties
-----------------------------------------------------------------------------

create table ice_orc2 (
    first_name string, 
    last_name string,
    dept_id bigint,
    team_id bigint
 )
partitioned by (company_id bigint)
stored by iceberg stored as orc
TBLPROPERTIES('format-version'='2', 'iceberg.catalog'='ice01');

--! Output should contain: 'type' = 'rest'
show create table ice_orc2;

insert into ice_orc2 partition (company_id=100) 
VALUES ('fn1','ln1', 1, 10), ('fn2','ln2', 2, 20), ('fn3','ln3', 3, 30);

describe formatted ice_orc2;
select * from ice_orc2;

---------------------------------------------------------------------------------------------------------------------
--! Iceberg Native View tests
---------------------------------------------------------------------------------------------------------------------

-- Enable once CBO supports Iceberg native views on partitioned tables
set hive.cbo.enable=false;  

-----------------------------------------------------------------------------------------------
--! Iceberg native view (with STORED BY ICEBERG in the SQL command) on a REST catalog table 
--! without a catalog name in table properties
-----------------------------------------------------------------------------------------------

create view ice_v1 stored by iceberg as select * from ice_orc1;
select * from ice_v1;
desc formatted ice_v1;
drop view ice_v1;

-----------------------------------------------------------------------------------------------
--! Iceberg native view (with STORED BY ICEBERG in the SQL command) on a REST catalog table 
--! with a catalog name in table properties
-----------------------------------------------------------------------------------------------

create view ice_v2 stored by iceberg as select * from ice_orc2;
select * from ice_v2;
desc formatted ice_v2;
drop view ice_v2;

-----------------------------------------------------------------------------------------------
--! Native view when STORED BY is omitted and 'hive.default.storage.handler.class' Hive conf 
--! is set to 'HiveIcebergStorageHandler' on a REST catalog table without catalog name in table properties
-----------------------------------------------------------------------------------------------

set hive.default.storage.handler.class=org.apache.iceberg.mr.hive.HiveIcebergStorageHandler;

create view ice_v3 stored by iceberg as select * from ice_orc1;
select * from ice_v3;
desc formatted ice_v3;
drop view ice_v3;

-----------------------------------------------------------------------------------------------
--! Native view when STORED BY is omitted and 'hive.default.storage.handler.class' Hive conf 
--! is set to 'HiveIcebergStorageHandler' on a REST catalog table with a catalog name in table properties
-----------------------------------------------------------------------------------------------

create view ice_v4 stored by iceberg as select * from ice_orc2;
select * from ice_v4;
desc formatted ice_v4;
drop view ice_v4;

-----------------------------------------------------------------------------

show tables;
drop table ice_orc1;
drop table ice_orc2;
show tables;

show databases;
drop database ice_rest;
show databases;