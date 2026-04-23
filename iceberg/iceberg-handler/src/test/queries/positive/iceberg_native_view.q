-- SORT_QUERY_RESULTS
-- Mask random uuid
--! qt:replace:/(\s+uuid\s+)\S+(\s*)/$1#Masked#$2/

create database ice_native_view_db;
use ice_native_view_db;

create table src_ice (
    first_name string, 
    last_name string
 )
partitioned by (dept_id bigint)
stored by iceberg stored as orc;

INSERT INTO src_ice VALUES
  ('fn1','ln1', 1),
  ('fn2','ln2', 1),
  ('fn3','ln3', 1),
  ('fn4','ln4', 1),
  ('fn5','ln5', 2),
  ('fn6','ln6', 2),
  ('fn7','ln7', 2);

-------------------------------------------------------------------------------
-- Native Iceberg view via TBLPROPERTIES
-------------------------------------------------------------------------------

-- TEST VIEW CREATION --

create view v_ice tblproperties ('view-format'='iceberg') 
as select * from src_ice;

select * from v_ice;

-- TEST VIEW REPLACEMENT --

create or replace view v_ice tblproperties ('view-format'='iceberg') 
as select first_name || '-' || dept_id from src_ice where dept_id = 1;

select * from v_ice;

desc formatted v_ice;
drop view v_ice;

-------------------------------------------------------------------------------
-- Native Iceberg view when default storage handler is Iceberg 
-- and no 'view-format' property in TBLPROPERTIES
-------------------------------------------------------------------------------

set hive.default.storage.handler.class=org.apache.iceberg.mr.hive.HiveIcebergStorageHandler;

-- TEST VIEW CREATION WITH IF EXISTS --

create view if not exists v_def 
as select first_name, last_name, dept_id from src_ice where dept_id = 2;

select * from v_def;

-- TEST VIEW IS NOT CREATED BECAUSE IT ALREADY EXISTS --

create view if not exists v_def 
as select first_name, last_name, dept_id from src_ice;

select * from v_def;

desc formatted v_def;
drop view v_def;

-----------------------------------------------------------------------------------------
-- Classic Hive view when the base table is Iceberg and default storage handler is unset
-----------------------------------------------------------------------------------------

set hive.default.storage.handler.class=;

create view v_hive as select * from src_ice;
select * from v_hive;
desc formatted v_hive;
drop view v_hive;
