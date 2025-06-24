dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/t1;
dfs -touchz ${system:test.tmp.dir}/t1/1.txt;
dfs -chmod 555 ${system:test.tmp.dir}/t1/1.txt;

set hive.metastore.pre.event.listeners=org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthorizer;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.fallback.FallbackHiveAuthorizerFactory;

-- Attempt to Create external table without having write permissions on table dir should not result in error
CREATE EXTERNAL TABLE t1(i int) location '${system:test.tmp.dir}/t1';
Select * from t1;

CREATE EXTERNAL TABLE LikeExternalTable LIKE t1 location '${system:test.warehouse.dir}/LikeExternalTable';

-- Skip authorization if location is not specified
CREATE DATABASE IF NOT EXISTS test_db COMMENT 'Hive test database';
use test_db;

--Skip DFS_URI auth for table under default DB location
CREATE EXTERNAL TABLE t1(i int) location '${system:test.warehouse.dir}/test_db.db/t1';;
CREATE TABLE t2(i int, name String) stored as ORC;
CREATE TABLE t3(i int, name String) stored as ORC location '${system:test.warehouse.dir}/test_db.db/t3';

