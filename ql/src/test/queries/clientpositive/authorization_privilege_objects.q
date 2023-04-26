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
