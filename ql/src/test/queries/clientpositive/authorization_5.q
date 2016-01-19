set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;

-- SORT_BEFORE_DIFF

CREATE DATABASE IF NOT EXISTS test_db COMMENT 'Hive test database';
SHOW DATABASES;

GRANT drop ON DATABASE test_db TO USER hive_test_user;
GRANT select ON DATABASE test_db TO USER hive_test_user;

SHOW GRANT USER hive_test_user ON DATABASE test_db;

CREATE ROLE db_TEST_Role;
GRANT ROLE db_TEST_Role TO USER hive_test_user;
SHOW ROLE GRANT USER hive_test_user;

GRANT drop ON DATABASE test_db TO ROLE db_TEST_Role;
GRANT select ON DATABASE test_db TO ROLE db_TEST_Role;

SHOW GRANT ROLE db_TEST_Role ON DATABASE test_db;

DROP DATABASE IF EXISTS test_db;
