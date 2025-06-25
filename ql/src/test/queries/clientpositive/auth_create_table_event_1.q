set hive.metastore.pre.event.listeners=org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthorizer;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.fallback.FallbackHiveAuthorizerFactory;

-- Skip authorization if location is not specified
CREATE DATABASE IF NOT EXISTS test_db COMMENT 'Hive test database';
use test_db;

--Skip DFS_URI auth for table under default DB location
-- Attempt to Create external table without having write permissions on table dir should not result in error
CREATE EXTERNAL TABLE t1(i int) location '${system:test.warehouse.dir}/test_db.db/t1';;
CREATE TABLE t2(i int, name String) stored as ORC;
CREATE TABLE t3(i int, name String) stored as ORC location '${system:test.warehouse.dir}/test_db.db/t3';

