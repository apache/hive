dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/a_ext_create_tab1;
dfs -touchz ${system:test.tmp.dir}/a_ext_create_tab1/1.txt;
dfs -chmod 555 ${system:test.tmp.dir}/a_ext_create_tab1/1.txt;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/a_ext_create_tab2;
dfs -chmod 555 ${system:test.tmp.dir}/a_ext_create_tab2;

set hive.metastore.pre.event.listeners=org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthorizer;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.fallback.FallbackHiveAuthorizerFactory;

-- HIVE-27525 Attempt to Create external table without having write permissions on table dir should not result in error
CREATE EXTERNAL TABLE t1(i int) location '${system:test.tmp.dir}/a_ext_create_tab1';
Select * from t1;

CREATE EXTERNAL TABLE LikeExternalTable LIKE t1 location '${system:test.tmp.dir}/a_ext_create_tab2';

-- Skip authorization if location is not specified
CREATE DATABASE IF NOT EXISTS test_db COMMENT 'Hive test database';
use test_db;

-- HIVE-28736 Skip DFS_URI auth for table under default DB location
-- Attempt to Create external table without having write permissions on table dir should not result in error
CREATE EXTERNAL TABLE t1(i int) location '${system:test.warehouse.dir}/test_db.db/t1';;
CREATE TABLE t2(i int, name String) stored as ORC;
CREATE TABLE t3(i int, name String) stored as ORC location '${system:test.warehouse.dir}/test_db.db/t3';
