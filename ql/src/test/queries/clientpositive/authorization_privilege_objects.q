--! qt:authorizer
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set test.hive.authz.sstd.validator.outputPrivObjs=true;
set test.hive.authz.sstd.validator.bypassObjTypes=DATABASE;
set hive.test.authz.sstd.hs2.mode=true;
set user.name=testuser;

CREATE DATABASE test_auth_obj_db;
CREATE TABLE test_auth_obj_db.test_privs(i int);
set user.name=testuser2;
CREATE TABLE test_auth_obj_db.test_privs2(s string, i int);
set user.name=testuser;
SHOW DATABASES LIKE 'test_auth_obj_db';
SHOW TABLES IN test_auth_obj_db;
EXPLAIN SELECT * FROM test_auth_obj_db.test_privs;
EXPLAIN INSERT INTO test_auth_obj_db.test_privs VALUES (1),(2),(3);
set user.name=testuser2;
DROP TABLE test_auth_obj_db.test_privs2;
set user.name=testuser;
DROP TABLE test_auth_obj_db.test_privs;
DROP DATABASE test_auth_obj_db;

set user.name=hive_admin_user;
set role admin;

CREATE TEMPORARY FUNCTION dboutput AS 'org.apache.hadoop.hive.contrib.genericudf.example.GenericUDFDBOutput';

SELECT
dboutput ( 'jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'CREATE TABLE SIMPLE_DERBY_TABLE1 ("ikey" INTEGER, "bkey" BIGINT, "fkey" REAL, "dkey" DOUBLE)' );

CREATE EXTERNAL TABLE ext_simple_derby_table_src
(
 ikey int,
 bkey bigint,
 fkey float,
 dkey double
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "APP",
                "hive.sql.dbcp.password" = "mine",
                "hive.sql.table" = "SIMPLE_DERBY_TABLE1",
                "hive.sql.dbcp.maxActive" = "1"
);

create table ext_simple_derby_table_ctas as select * from ext_simple_derby_table_src;

CREATE EXTERNAL TABLE default.jdbctable_from_ctas
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "APP",
                "hive.sql.dbcp.password" = "mine",
                "hive.sql.table" = "SIMPLE_DERBY_TABLE1",
                "hive.sql.dbcp.maxActive" = "1"
) as select * from default.ext_simple_derby_table_ctas;

drop table default.jdbctable_from_ctas;
drop table default.ext_simple_derby_table_ctas;
drop table default.ext_simple_derby_table_src;
