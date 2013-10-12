set -e
move_source() {
  source=$1
  target=$2
  mkdir -p $(dirname $target)
  mv $source $target
}
move_source serde/src/test/org/apache/hadoop/hive/serde2/TestSerdeWithFieldComments.java itests/unit/src/test/java/org/apache/hadoop/hive/serde2/TestSerdeWithFieldComments.java
move_source serde/src/test/org/apache/hadoop/hive/serde2/dynamic_type/TestDynamicSerDe.java itests/unit/src/test/java/org/apache/hadoop/hive/serde2/dynamic_type/TestDynamicSerDe.java
move_source service/src/test/org/apache/hive/service/cli/thrift/TestThriftHttpCLIService.java itests/unit/src/test/java/org/apache/hive/service/cli/thrift/TestThriftHttpCLIService.java
move_source service/src/test/org/apache/hive/service/auth/TestCustomAuthentication.java itests/unit/src/test/java/org/apache/hive/service/auth/TestCustomAuthentication.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreEventListenerOnlyOnCommit.java itests/unit/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreEventListenerOnlyOnCommit.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreEndFunctionListener.java itests/unit/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreEndFunctionListener.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestHiveMetaStore.java itests/unit/src/test/java/org/apache/hadoop/hive/metastore/TestHiveMetaStore.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestHiveMetaStoreWithEnvironmentContext.java itests/unit/src/test/java/org/apache/hadoop/hive/metastore/TestHiveMetaStoreWithEnvironmentContext.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreConnectionUrlHook.java itests/unit/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreConnectionUrlHook.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestPartitionNameWhitelistValidation.java itests/unit/src/test/java/org/apache/hadoop/hive/metastore/TestPartitionNameWhitelistValidation.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreInitListener.java itests/unit/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreInitListener.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestMarkPartition.java itests/unit/src/test/java/org/apache/hadoop/hive/metastore/TestMarkPartition.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestMarkPartitionRemote.java itests/unit/src/test/java/org/apache/hadoop/hive/metastore/TestMarkPartitionRemote.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreEventListener.java itests/unit/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreEventListener.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestRemoteHiveMetaStore.java itests/unit/src/test/java/org/apache/hadoop/hive/metastore/TestRemoteHiveMetaStore.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestEmbeddedHiveMetaStore.java itests/unit/src/test/java/org/apache/hadoop/hive/metastore/TestEmbeddedHiveMetaStore.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestSetUGIOnBothClientServer.java itests/unit/src/test/java/org/apache/hadoop/hive/metastore/TestSetUGIOnBothClientServer.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestSetUGIOnOnlyServer.java itests/unit/src/test/java/org/apache/hadoop/hive/metastore/TestSetUGIOnOnlyServer.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestSetUGIOnOnlyClient.java itests/unit/src/test/java/org/apache/hadoop/hive/metastore/TestSetUGIOnOnlyClient.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestMetastoreVersion.java itests/unit/src/test/java/org/apache/hadoop/hive/metastore/TestMetastoreVersion.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestHiveMetaTool.java itests/unit/src/test/java/org/apache/hadoop/hive/metastore/TestHiveMetaTool.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestRawStoreTxn.java itests/unit/src/test/java/org/apache/hadoop/hive/metastore/TestRawStoreTxn.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreListenersError.java itests/unit/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreListenersError.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestRetryingHMSHandler.java itests/unit/src/test/java/org/apache/hadoop/hive/metastore/TestRetryingHMSHandler.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestRemoteUGIHiveMetaStoreIpAddress.java itests/unit/src/test/java/org/apache/hadoop/hive/metastore/TestRemoteUGIHiveMetaStoreIpAddress.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreAuthorization.java itests/unit/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreAuthorization.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestRemoteHiveMetaStoreIpAddress.java itests/unit/src/test/java/org/apache/hadoop/hive/metastore/TestRemoteHiveMetaStoreIpAddress.java
move_source ql/src/test/org/apache/hadoop/hive/ql/security/TestDefaultHiveMetastoreAuthorizationProvider.java itests/unit/src/test/java/org/apache/hadoop/hive/ql/security/TestDefaultHiveMetastoreAuthorizationProvider.java
move_source ql/src/test/org/apache/hadoop/hive/ql/security/TestAuthorizationPreEventListener.java itests/unit/src/test/java/org/apache/hadoop/hive/ql/security/TestAuthorizationPreEventListener.java
move_source ql/src/test/org/apache/hadoop/hive/ql/history/TestHiveHistory.java itests/unit/src/test/java/org/apache/hadoop/hive/ql/history/TestHiveHistory.java
move_source ql/src/test/org/apache/hadoop/hive/ql/QTestUtil.java itests/util/src/main/java/org/apache/hadoop/hive/ql/QTestUtil.java
move_source service/src/test/org/apache/hive/service/server/TestHiveServer2Concurrency.java itests/unit/src/test/java/org/apache/hive/service/server/TestHiveServer2Concurrency.java
move_source ql/src/test/org/apache/hadoop/hive/ql/TestMTQueries.java itests/unit/src/test/java/org/apache/hadoop/hive/ql/TestMTQueries.java
move_source ql/src/test/org/apache/hadoop/hive/ql/TestLocationQueries.java itests/unit/src/test/java/org/apache/hadoop/hive/ql/TestLocationQueries.java
move_source hbase-handler/src/test/org/apache/hadoop/hive/hbase/HBaseQTestUtil.java itests/util/src/main/java/org/apache/hadoop/hive/hbase/HBaseQTestUtil.java
move_source hbase-handler/src/test/org/apache/hadoop/hive/hbase/HBaseTestSetup.java itests/util/src/main/java/org/apache/hadoop/hive/hbase/HBaseTestSetup.java
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

move_source data/conf/hive-site.xml data/conf/hive-site-old.xml
move_source data/conf/hive-site-new.xml data/conf/hive-site.xml
move_source data/conf/hive-log4j.properties data/conf/hive-log4j-old.properties
move_source data/conf/hive-log4j-new.properties data/conf/hive-log4j.properties

# eclipse doesn't like .. references in it's path to src
move_source shims/src/0.20/java shims/0.20/src/main/java
move_source shims/src/0.20S/java shims/0.20S/src/main/java
move_source shims/src/0.23/java shims/0.23/src/main/java
move_source shims/src/common/java shims/common/src/main/java
move_source shims/src/common-secure/java shims/common-secure/src/main/java
# cyclic deps
move_source shims/src/common-secure/test/org/apache/hadoop/hive/thrift/TestDBTokenStore.java itests/unit/src/test/java/org/apache/hadoop/hive/thrift/TestDBTokenStore.java
move_source shims/src/common-secure/test/org/apache/hadoop/hive/thrift/TestHadoop20SAuthBridge.java itests/unit/src/test/java/org/apache/hadoop/hive/thrift/TestHadoop20SAuthBridge.java
move_source shims/src/common-secure/test/org/apache/hadoop/hive/thrift/TestZooKeeperTokenStore.java itests/unit/src/test/java/org/apache/hadoop/hive/thrift/TestZooKeeperTokenStore.java

move_source jdbc/src/test/org/apache/hadoop/hive/jdbc/TestJdbcDriver.java itests/unit/src/test/java/org/apache/hadoop/hive/jdbc/TestJdbcDriver.java
move_source jdbc/src/test/org/apache/hive/jdbc/TestJdbcDriver2.java itests/unit/src/test/java/org/apache/hive/jdbc/TestJdbcDriver2.java

move_source service/src/test/org/apache/hive/service/cli/TestEmbeddedThriftBinaryCLIService.java itests/unit/src/test/java/org/apache/hive/service/cli/TestEmbeddedThriftBinaryCLIService.java
move_source service/src/test/org/apache/hive/service/cli/thrift/TestThriftBinaryCLIService.java itests/unit/src/test/java/org/apache/hive/service/cli/thrift/TestThriftBinaryCLIService.java
move_source service/src/test/org/apache/hadoop/hive/service/TestHiveServer.java itests/unit/src/test/java/org/apache/hadoop/hive/service/TestHiveServer.java

move_source ql/src/test/org/apache/hadoop/hive/scripts/extracturl.java itests/util/src/main/java/org/apache/hadoop/hive/scripts/extracturl.java

move_source beeline/src/test/org/apache/hive/beeline/src/test/TestSchemaTool.java itests/unit/src/test/java/org/apache/hive/beeline/TestSchemaTool.java

move_source beeline/src/java/org/apache/hive/beeline/sql-keywords.properties beeline/src/main/resources/sql-keywords.properties
move_source beeline/src/java/org/apache/hive/beeline/BeeLine.properties beeline/src/main/resources/BeeLine.properties

move_source ql/src/java/conf/hive-exec-log4j.properties ql/src/main/resources/hive-exec-log4j.properties
move_source common/src/java/conf/hive-log4j.properties common/src/main/resources/hive-log4j.properties
