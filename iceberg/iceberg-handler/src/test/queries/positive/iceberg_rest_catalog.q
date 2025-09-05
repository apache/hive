-- SORT_QUERY_RESULTS
-- Mask neededVirtualColumns due to non-strict order
--! qt:replace:/(\s+neededVirtualColumns:\s)(.*)/$1#Masked#/
-- Mask random uuid
--! qt:replace:/(\s+'uuid'=')\S+('\s*)/$1#Masked#$2/
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
-- Mask compaction id as they will be allocated in parallel threads
--! qt:replace:/^[0-9]/#Masked#/
-- Mask removed file size
--! qt:replace:/(\S\"removed-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask iceberg version
--! qt:replace:/(\S\"iceberg-version\\\":\\\")(\w+\s\w+\s\d+\.\d+\.\d+\s\(\w+\s\w+\))(\\\")/$1#Masked#$3/

set metastore.client.impl=org.apache.iceberg.hive.client.HiveRESTCatalogClient;
set metastore.catalog.default=ice01;
set iceberg.catalog.ice01.type=rest;

--! This config is set in the driver setup (see TestIcebergRESTCatalogLlapLocalCliDriver.java)   
--! conf.set('iceberg.catalog.ice01.uri', <RESTServer URI>);

create database ice_rest;
use ice_rest;

--! Creating table without catalog name in table properties
create table ice_orc1 (
    first_name string, 
    last_name string,
    dept_id bigint,
    team_id bigint
 )
partitioned by (company_id bigint)
stored by iceberg stored as orc;

--! Creating table with a valid catalog name in table properties
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

show tables;
drop table ice_orc1;
drop table ice_orc2;
show tables;

show databases;
drop database ice_rest;
show databases;