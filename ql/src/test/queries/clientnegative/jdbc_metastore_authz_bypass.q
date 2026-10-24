--! qt:authorizer
--! qt:sysdb
--
-- Non-admin users must not create JDBC storage-handler tables with
-- hive.sql.database.type=METASTORE (metastore RDBMS credential binding).

set user.name=hive_admin_user;
set role admin;
create database admin_secret_db;

set user.name=attacker;
create database attacker_db;
use attacker_db;

create external table metastore_exfil (name string)
stored by 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
tblproperties (
  'hive.sql.database.type'='METASTORE',
  'hive.sql.query'='SELECT "NAME" FROM "DBS"'
);
