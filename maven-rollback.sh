# rollback file, generated with:
set -e
move_source() {
  source=$1
  target=$2
  mkdir -p $(dirname $target)
  mv $source $target
}
move_source itests/src/test/java/org/apache/hadoop/hive/serde2/TestSerdeWithFieldComments.java serde/src/test/org/apache/hadoop/hive/serde2/TestSerdeWithFieldComments.java
move_source itests/src/test/java/org/apache/hadoop/hive/serde2/dynamic_type/TestDynamicSerDe.java serde/src/test/org/apache/hadoop/hive/serde2/dynamic_type/TestDynamicSerDe.java
move_source itests/src/test/java/org/apache/hive/service/cli/thrift/TestThriftHttpCLIService.java service/src/test/org/apache/hive/service/cli/thrift/TestThriftHttpCLIService.java
move_source itests/src/test/java/org/apache/hive/service/auth/TestCustomAuthentication.java service/src/test/org/apache/hive/service/auth/TestCustomAuthentication.java
move_source itests/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreEventListenerOnlyOnCommit.java metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreEventListenerOnlyOnCommit.java
move_source itests/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreEndFunctionListener.java metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreEndFunctionListener.java
move_source itests/src/test/java/org/apache/hadoop/hive/metastore/TestHiveMetaStore.java metastore/src/test/org/apache/hadoop/hive/metastore/TestHiveMetaStore.java
move_source itests/src/test/java/org/apache/hadoop/hive/metastore/TestHiveMetaStoreWithEnvironmentContext.java metastore/src/test/org/apache/hadoop/hive/metastore/TestHiveMetaStoreWithEnvironmentContext.java
move_source itests/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreConnectionUrlHook.java metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreConnectionUrlHook.java
move_source itests/src/test/java/org/apache/hadoop/hive/metastore/TestPartitionNameWhitelistValidation.java metastore/src/test/org/apache/hadoop/hive/metastore/TestPartitionNameWhitelistValidation.java
move_source itests/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreInitListener.java metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreInitListener.java
move_source itests/src/test/java/org/apache/hadoop/hive/metastore/TestMarkPartition.java metastore/src/test/org/apache/hadoop/hive/metastore/TestMarkPartition.java
move_source itests/src/test/java/org/apache/hadoop/hive/metastore/TestMarkPartitionRemote.java metastore/src/test/org/apache/hadoop/hive/metastore/TestMarkPartitionRemote.java
move_source itests/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreEventListener.java metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreEventListener.java
move_source itests/src/test/java/org/apache/hadoop/hive/metastore/TestRemoteHiveMetaStore.java metastore/src/test/org/apache/hadoop/hive/metastore/TestRemoteHiveMetaStore.java
move_source itests/src/test/java/org/apache/hadoop/hive/metastore/TestEmbeddedHiveMetaStore.java metastore/src/test/org/apache/hadoop/hive/metastore/TestEmbeddedHiveMetaStore.java
move_source itests/src/test/java/org/apache/hadoop/hive/metastore/TestSetUGIOnBothClientServer.java metastore/src/test/org/apache/hadoop/hive/metastore/TestSetUGIOnBothClientServer.java
move_source itests/src/test/java/org/apache/hadoop/hive/metastore/TestSetUGIOnOnlyServer.java metastore/src/test/org/apache/hadoop/hive/metastore/TestSetUGIOnOnlyServer.java
move_source itests/src/test/java/org/apache/hadoop/hive/metastore/TestSetUGIOnOnlyClient.java metastore/src/test/org/apache/hadoop/hive/metastore/TestSetUGIOnOnlyClient.java
move_source itests/src/test/java/org/apache/hadoop/hive/metastore/TestMetastoreVersion.java metastore/src/test/org/apache/hadoop/hive/metastore/TestMetastoreVersion.java
move_source itests/src/test/java/org/apache/hadoop/hive/ql/security/TestDefaultHiveMetastoreAuthorizationProvider.java ql/src/test/org/apache/hadoop/hive/ql/security/TestDefaultHiveMetastoreAuthorizationProvider.java
move_source itests/src/test/java/org/apache/hadoop/hive/ql/security/TestAuthorizationPreEventListener.java ql/src/test/org/apache/hadoop/hive/ql/security/TestAuthorizationPreEventListener.java
move_source itests/src/test/java/org/apache/hadoop/hive/ql/history/TestHiveHistory.java ql/src/test/org/apache/hadoop/hive/ql/history/TestHiveHistory.java
move_source itests/src/test/java/org/apache/hadoop/hive/ql/QTestUtil.java ql/src/test/org/apache/hadoop/hive/ql/QTestUtil.java
move_source itests/src/test/java/org/apache/hive/service/server/TestHiveServer2Concurrency.java service/src/test/org/apache/hive/service/server/TestHiveServer2Concurrency.java
move_source itests/src/test/java/org/apache/hadoop/hive/ql/hooks/EnforceReadOnlyTables.java ql/src/test/org/apache/hadoop/hive/ql/hooks/EnforceReadOnlyTables.java
move_source itests/src/test/java/org/apache/hadoop/hive/ql/TestMTQueries.java ql/src/test/org/apache/hadoop/hive/ql/TestMTQueries.java
move_source itests/src/test/java/org/apache/hadoop/hive/ql/TestLocationQueries.java ql/src/test/org/apache/hadoop/hive/ql/TestLocationQueries.java
move_source itests/src/test/java/org/apache/hadoop/hive/hbase/HBaseQTestUtil.java hbase-handler/src/test/org/apache/hadoop/hive/hbase/HBaseQTestUtil.java
move_source itests/src/test/java/org/apache/hadoop/hive/hbase/HBaseTestSetup.java hbase-handler/src/test/org/apache/hadoop/hive/hbase/HBaseTestSetup.java

move_source hcatalog/hcatalog-pig-adapter/pom.xml hcatalog/hcatalog-pig-adapter/pom-new.xml
move_source hcatalog/pom.xml hcatalog/pom-new.xml
move_source hcatalog/storage-handlers/hbase/pom.xml hcatalog/storage-handlers/hbase/pom-new.xml
move_source hcatalog/server-extensions/pom.xml hcatalog/server-extensions/pom-new.xml
move_source hcatalog/core/pom.xml hcatalog/core/pom-new.xml
move_source hcatalog/webhcat/java-client/pom.xml hcatalog/webhcat/java-client/pom-new.xml
move_source hcatalog/webhcat/svr/pom.xml hcatalog/webhcat/svr/pom-new.xml

move_source hcatalog/hcatalog-pig-adapter/pom-old.xml hcatalog/hcatalog-pig-adapter/pom.xml
move_source hcatalog/pom-old.xml hcatalog/pom.xml
move_source hcatalog/storage-handlers/hbase/pom-old.xml hcatalog/storage-handlers/hbase/pom.xml
move_source hcatalog/server-extensions/pom-old.xml hcatalog/server-extensions/pom.xml
move_source hcatalog/core/pom-old.xml hcatalog/core/pom.xml
move_source hcatalog/webhcat/java-client/pom-old.xml hcatalog/webhcat/java-client/pom.xml
move_source hcatalog/webhcat/svr/pom-old.xml hcatalog/webhcat/svr/pom.xml

move_source data/conf/hive-site.xml data/conf/hive-site-new.xml
move_source data/conf/hive-site-old.xml data/conf/hive-site.xml
move_source data/conf/hive-log4j.properties data/conf/hive-log4j-new.properties
move_source data/conf/hive-log4j-old.properties data/conf/hive-log4j.properties

move_source shims/0.20/src/main/java shims/src/0.20/java
move_source shims/0.20S/src/main/java shims/src/0.20S/java
move_source shims/0.23/src/main/java shims/src/0.23/java
move_source shims/common/src/main/java shims/src/common/java
move_source shims/common-secure/src/main/java shims/src/common-secure/java

move_source itests/src/test/java/org/apache/hadoop/hive/thrift/TestDBTokenStore.java shims/src/common-secure/test/org/apache/hadoop/hive/thrift/TestDBTokenStore.java
move_source itests/src/test/java/org/apache/hadoop/hive/thrift/TestHadoop20SAuthBridge.java shims/src/common-secure/test/org/apache/hadoop/hive/thrift/TestHadoop20SAuthBridge.java
move_source itests/src/test/java/org/apache/hadoop/hive/thrift/TestZooKeeperTokenStore.java shims/src/common-secure/test/org/apache/hadoop/hive/thrift/TestZooKeeperTokenStore.java

move_source itests/src/test/java/org/apache/hadoop/hive/jdbc/TestJdbcDriver.java jdbc/src/test/org/apache/hadoop/hive/jdbc/TestJdbcDriver.java
move_source itests/src/test/java/org/apache/hive/jdbc/TestJdbcDriver2.java jdbc/src/test/org/apache/hive/jdbc/TestJdbcDriver2.java

move_source itests/src/test/java/org/apache/hive/service/cli/TestEmbeddedThriftBinaryCLIService.java service/src/test/org/apache/hive/service/cli/TestEmbeddedThriftBinaryCLIService.java
move_source itests/src/test/java/org/apache/hive/service/cli/thrift/TestThriftBinaryCLIService.java service/src/test/org/apache/hive/service/cli/thrift/TestThriftBinaryCLIService.java
move_source itests/src/test/java/org/apache/hadoop/hive/service/TestHiveServer.java service/src/test/org/apache/hadoop/hive/service/TestHiveServer.java
