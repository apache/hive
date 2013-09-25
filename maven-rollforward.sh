set -e
move_source() {
  source=$1
  target=$2
  mkdir -p $(dirname $target)
  mv $source $target
}
move_source serde/src/test/org/apache/hadoop/hive/serde2/TestSerdeWithFieldComments.java itests/src/test/java/org/apache/hadoop/hive/serde2/TestSerdeWithFieldComments.java
move_source serde/src/test/org/apache/hadoop/hive/serde2/dynamic_type/TestDynamicSerDe.java itests/src/test/java/org/apache/hadoop/hive/serde2/dynamic_type/TestDynamicSerDe.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreEventListenerOnlyOnCommit.java itests/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreEventListenerOnlyOnCommit.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreEndFunctionListener.java itests/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreEndFunctionListener.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestHiveMetaStore.java itests/src/test/java/org/apache/hadoop/hive/metastore/TestHiveMetaStore.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestHiveMetaStoreWithEnvironmentContext.java itests/src/test/java/org/apache/hadoop/hive/metastore/TestHiveMetaStoreWithEnvironmentContext.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreConnectionUrlHook.java itests/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreConnectionUrlHook.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestPartitionNameWhitelistValidation.java itests/src/test/java/org/apache/hadoop/hive/metastore/TestPartitionNameWhitelistValidation.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreInitListener.java itests/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreInitListener.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestMarkPartition.java itests/src/test/java/org/apache/hadoop/hive/metastore/TestMarkPartition.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestMarkPartitionRemote.java itests/src/test/java/org/apache/hadoop/hive/metastore/TestMarkPartitionRemote.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreEventListener.java itests/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreEventListener.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestRemoteHiveMetaStore.java itests/src/test/java/org/apache/hadoop/hive/metastore/TestRemoteHiveMetaStore.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestEmbeddedHiveMetaStore.java itests/src/test/java/org/apache/hadoop/hive/metastore/TestEmbeddedHiveMetaStore.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestSetUGIOnBothClientServer.java itests/src/test/java/org/apache/hadoop/hive/metastore/TestSetUGIOnBothClientServer.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestSetUGIOnOnlyServer.java itests/src/test/java/org/apache/hadoop/hive/metastore/TestSetUGIOnOnlyServer.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestSetUGIOnOnlyClient.java itests/src/test/java/org/apache/hadoop/hive/metastore/TestSetUGIOnOnlyClient.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestMetastoreVersion.java itests/src/test/java/org/apache/hadoop/hive/metastore/TestMetastoreVersion.java
move_source ql/src/test/org/apache/hadoop/hive/ql/security/TestDefaultHiveMetastoreAuthorizationProvider.java itests/src/test/java/org/apache/hadoop/ql/security/TestDefaultHiveMetastoreAuthorizationProvider.java
move_source ql/src/test/org/apache/hadoop/hive/ql/security/TestAuthorizationPreEventListener.java itests/src/test/java/org/apache/hadoop/ql/security/TestAuthorizationPreEventListener.java
move_source ql/src/test/org/apache/hadoop/hive/ql/history/TestHiveHistory.java itests/src/test/java/org/apache/hadoop/ql/history/TestHiveHistory.java
move_source ql/src/test/org/apache/hadoop/hive/ql/QTestUtil.java itests/src/test/java/org/apache/hadoop/ql/QTestUtil.java
move_source service/src/test/org/apache/hive/service/server/TestHiveServer2Concurrency.java itests/src/test/java/org/apache/hive/service/server/TestHiveServer2Concurrency.java
move_source ql/src/test/org/apache/hadoop/hive/ql/hooks/EnforceReadOnlyTables.java itests/src/test/java/org/apache/hadoop/hive/ql/hooks/EnforceReadOnlyTables.java
move_source ql/src/test/org/apache/hadoop/hive/ql/TestMTQueries.java itests/src/test/java/org/apache/hadoop/hive/ql/TestMTQueries.java
move_source ql/src/test/org/apache/hadoop/hive/ql/TestLocationQueries.java itests/src/test/java/org/apache/hadoop/hive/ql/TestLocationQueries.java
move_source hbase-handler/src/test/org/apache/hadoop/hive/hbase/HBaseQTestUtil.java itests/src/test/java/org/apache/hadoop/hive/hbase/HBaseQTestUtil.java
move_source hbase-handler/src/test/org/apache/hadoop/hive/hbase/HBaseTestSetup.java itests/src/test/java/org/apache/hadoop/hive/hbase/HBaseTestSetup.java
# move existing pom.xml to pom-old.xml
move_source hcatalog/hcatalog-pig-adapter/pom.xml hcatalog/hcatalog-pig-adapter/pom-old.xml
move_source hcatalog/pom.xml hcatalog/pom-old.xml
move_source hcatalog/storage-handlers/hbase/pom.xml hcatalog/storage-handlers/hbase/pom-old.xml
move_source hcatalog/server-extensions/pom.xml hcatalog/server-extensions/pom-old.xml
move_source hcatalog/core/pom.xml hcatalog/core/pom-old.xml
move_source hcatalog/webhcat/java-client/pom.xml hcatalog/webhcat/java-client/pom-old.xml
move_source hcatalog/webhcat/svr/pom.xml hcatalog/webhcat/svr/pom-old.xml
# move pom-new.xml to pom.xml
move_source hcatalog/hcatalog-pig-adapter/pom-new.xml hcatalog/hcatalog-pig-adapter/pom.xml
move_source hcatalog/pom-new.xml hcatalog/pom.xml
move_source hcatalog/storage-handlers/hbase/pom-new.xml hcatalog/storage-handlers/hbase/pom.xml
move_source hcatalog/server-extensions/pom-new.xml hcatalog/server-extensions/pom.xml
move_source hcatalog/core/pom-new.xml hcatalog/core/pom.xml
move_source hcatalog/webhcat/java-client/pom-new.xml hcatalog/webhcat/java-client/pom.xml
move_source hcatalog/webhcat/svr/pom-new.xml hcatalog/webhcat/svr/pom.xml
