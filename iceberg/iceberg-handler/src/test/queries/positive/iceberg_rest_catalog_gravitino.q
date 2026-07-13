-- SORT_QUERY_RESULTS
-- Mask neededVirtualColumns due to non-strict order
--! qt:replace:/(\s+neededVirtualColumns:\s)(.*)/$1#Masked#/
-- Mask random uuid
--! qt:replace:/(\s+'uuid'=')\S+('\s*)/$1#Masked#$2/
-- Mask Iceberg metadata file name (sequence id + UUID) in show create table
--! qt:replace:/('metadata_location'=')[^']+(')/$1#Masked#$2/
-- Mask metadata_location path in describe formatted ('|' delimiter: regex contains s3:// so '/' breaks QTestReplaceHandler).
--! qt:replace:|(metadata_location)(\s+)(s3://\S+)|$1$2#Masked#|
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
set iceberg.catalog.ice01.header.X-Iceberg-Access-Delegation=vended-credentials;
--! Vended credentials travel via DAG credentials (HIVE-20651); an in-process fetch task cannot
--! restore them, so fetch conversion must be off for credential-vending catalogs.
set hive.fetch.task.conversion=none;

--! REST URI, OAuth, MinIO + Gravitino S3 warehouse / credential vending, and host S3A are set in
--! TestIcebergRESTCatalogGravitinoLlapLocalCliDriver.

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
--! Iceberg views tests
---------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------
--! Iceberg view with TBLPROPERTIES ('view-format'='iceberg') on a REST catalog table
-----------------------------------------------------------------------------------------------------

create view ice_v1 tblproperties ('view-format'='iceberg')
as select first_name, last_name from ice_orc2 where dept_id in (1, 3);

select * from ice_v1;
desc formatted ice_v1;

------- if-not-exists view test - view should not change -------------------------

create view if not exists ice_v1 tblproperties ('view-format'='iceberg')
as select * from ice_orc2 where dept_id = 10000;

select * from ice_v1;
desc formatted ice_v1;

------- replace view test - view should be replaced ------------------------------

create or replace view ice_v1 tblproperties ('view-format'='iceberg')
as select first_name || '-' || dept_id from ice_orc2 where dept_id = 2;

select * from ice_v1;
desc formatted ice_v1;

drop view ice_v1;

-----------------------------------------------------------------------------------------------
--! Iceberg view with default Iceberg storage handler and REST catalog table 
-----------------------------------------------------------------------------------------------

set hive.default.storage.handler.class=org.apache.iceberg.mr.hive.HiveIcebergStorageHandler;

create view ice_v2
as select first_name, last_name || '-' || dept_id from ice_orc2 where team_id in (20, 30);

select * from ice_v2;
desc formatted ice_v2;
drop view ice_v2;

-----------------------------------------------------------------------------

show tables;
drop table ice_orc1;
drop table ice_orc2;
show tables;

show databases;
drop database ice_rest;
show databases;