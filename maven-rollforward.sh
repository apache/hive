set -e
move_source() {
  source=$1
  target=$2
  mkdir -p $(dirname $target)
  mv $source $target
}
move_source serde/src/test/org/apache/hadoop/hive/serde2/TestSerdeWithFieldComments.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/serde2/TestSerdeWithFieldComments.java
move_source serde/src/test/org/apache/hadoop/hive/serde2/dynamic_type/TestDynamicSerDe.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/serde2/dynamic_type/TestDynamicSerDe.java
move_source service/src/test/org/apache/hive/service/cli/thrift/TestThriftHttpCLIService.java itests/hive-unit/src/test/java/org/apache/hive/service/cli/thrift/TestThriftHttpCLIService.java
move_source service/src/test/org/apache/hive/service/auth/TestCustomAuthentication.java itests/hive-unit/src/test/java/org/apache/hive/service/auth/TestCustomAuthentication.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreEventListenerOnlyOnCommit.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreEventListenerOnlyOnCommit.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreEndFunctionListener.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreEndFunctionListener.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestHiveMetaStore.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestHiveMetaStore.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestHiveMetaStoreWithEnvironmentContext.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestHiveMetaStoreWithEnvironmentContext.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreConnectionUrlHook.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreConnectionUrlHook.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestPartitionNameWhitelistValidation.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestPartitionNameWhitelistValidation.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreInitListener.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreInitListener.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestMarkPartition.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestMarkPartition.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestMarkPartitionRemote.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestMarkPartitionRemote.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreEventListener.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreEventListener.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestRemoteHiveMetaStore.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestRemoteHiveMetaStore.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestEmbeddedHiveMetaStore.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestEmbeddedHiveMetaStore.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestSetUGIOnBothClientServer.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestSetUGIOnBothClientServer.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestSetUGIOnOnlyServer.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestSetUGIOnOnlyServer.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestSetUGIOnOnlyClient.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestSetUGIOnOnlyClient.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestMetastoreVersion.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestMetastoreVersion.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestHiveMetaTool.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestHiveMetaTool.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestRawStoreTxn.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestRawStoreTxn.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreListenersError.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreListenersError.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestRetryingHMSHandler.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestRetryingHMSHandler.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestRemoteUGIHiveMetaStoreIpAddress.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestRemoteUGIHiveMetaStoreIpAddress.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreAuthorization.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreAuthorization.java
move_source metastore/src/test/org/apache/hadoop/hive/metastore/TestRemoteHiveMetaStoreIpAddress.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestRemoteHiveMetaStoreIpAddress.java
move_source ql/src/test/org/apache/hadoop/hive/ql/security/TestStorageBasedClientSideAuthorizationProvider.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/ql/security/TestStorageBasedClientSideAuthorizationProvider.java
move_source ql/src/test/org/apache/hadoop/hive/ql/security/TestClientSideAuthorizationProvider.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/ql/security/TestClientSideAuthorizationProvider.java
move_source ql/src/test/org/apache/hadoop/hive/ql/security/TestStorageBasedMetastoreAuthorizationProvider.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/ql/security/TestStorageBasedMetastoreAuthorizationProvider.java
move_source ql/src/test/org/apache/hadoop/hive/ql/security/TestMetastoreAuthorizationProvider.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/ql/security/TestMetastoreAuthorizationProvider.java
move_source ql/src/test/org/apache/hadoop/hive/ql/security/TestAuthorizationPreEventListener.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/ql/security/TestAuthorizationPreEventListener.java
move_source ql/src/test/org/apache/hadoop/hive/ql/history/TestHiveHistory.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/ql/history/TestHiveHistory.java
move_source ql/src/test/org/apache/hadoop/hive/ql/QTestUtil.java itests/util/src/main/java/org/apache/hadoop/hive/ql/QTestUtil.java
move_source service/src/test/org/apache/hive/service/server/TestHiveServer2Concurrency.java itests/hive-unit/src/test/java/org/apache/hive/service/server/TestHiveServer2Concurrency.java
move_source ql/src/test/org/apache/hadoop/hive/ql/BaseTestQueries.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/ql/BaseTestQueries.java
move_source ql/src/test/org/apache/hadoop/hive/ql/TestLocationQueries.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/ql/TestLocationQueries.java
move_source ql/src/test/org/apache/hadoop/hive/ql/TestMTQueries.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/ql/TestMTQueries.java
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
move_source shims/src/common-secure/test/org/apache/hadoop/hive/thrift/TestDBTokenStore.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/thrift/TestDBTokenStore.java
move_source shims/src/common-secure/test/org/apache/hadoop/hive/thrift/TestHadoop20SAuthBridge.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/thrift/TestHadoop20SAuthBridge.java
move_source shims/src/common-secure/test/org/apache/hadoop/hive/thrift/TestZooKeeperTokenStore.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/thrift/TestZooKeeperTokenStore.java

move_source jdbc/src/test/org/apache/hadoop/hive/jdbc/TestJdbcDriver.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/jdbc/TestJdbcDriver.java
move_source jdbc/src/test/org/apache/hive/jdbc/TestJdbcDriver2.java itests/hive-unit/src/test/java/org/apache/hive/jdbc/TestJdbcDriver2.java

move_source service/src/test/org/apache/hive/service/cli/TestEmbeddedThriftBinaryCLIService.java itests/hive-unit/src/test/java/org/apache/hive/service/cli/TestEmbeddedThriftBinaryCLIService.java
move_source service/src/test/org/apache/hive/service/cli/thrift/TestThriftBinaryCLIService.java itests/hive-unit/src/test/java/org/apache/hive/service/cli/thrift/TestThriftBinaryCLIService.java
move_source service/src/test/org/apache/hadoop/hive/service/TestHiveServer.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/service/TestHiveServer.java

move_source ql/src/test/org/apache/hadoop/hive/scripts/extracturl.java itests/util/src/main/java/org/apache/hadoop/hive/scripts/extracturl.java

move_source beeline/src/test/org/apache/hive/beeline/src/test/TestSchemaTool.java itests/hive-unit/src/test/java/org/apache/hive/beeline/TestSchemaTool.java

move_source beeline/src/java/org/apache/hive/beeline/sql-keywords.properties beeline/src/main/resources/sql-keywords.properties
move_source beeline/src/java/org/apache/hive/beeline/BeeLine.properties beeline/src/main/resources/BeeLine.properties

move_source ql/src/java/conf/hive-exec-log4j.properties ql/src/main/resources/hive-exec-log4j.properties
move_source common/src/java/conf/hive-log4j.properties common/src/main/resources/hive-log4j.properties

move_source ql/src/test/org/apache/hadoop/hive/metastore/VerifyingObjectStore.java metastore/src/test/org/apache/hadoop/hive/metastore/VerifyingObjectStore.java
move_source ql/src/test/org/apache/hadoop/hive/ql/hooks/PreExecutePrinter.java ql/src/java/org/apache/hadoop/hive/ql/hooks/PreExecutePrinter.java
move_source ql/src/test/org/apache/hadoop/hive/ql/hooks/PostExecutePrinter.java ql/src/java/org/apache/hadoop/hive/ql/hooks/PostExecutePrinter.java
move_source ql/src/test/org/apache/hadoop/hive/ql/hooks/EnforceReadOnlyTables.java ql/src/java/org/apache/hadoop/hive/ql/hooks/EnforceReadOnlyTables.java
move_source ql/src/test/org/apache/hadoop/hive/ql/security/DummyAuthenticator.java itests/util/src/main/java/org/apache/hadoop/hive/ql/security/DummyAuthenticator.java
move_source ql/src/test/org/apache/hadoop/hive/ql/security/DummyHiveMetastoreAuthorizationProvider.java itests/util/src/main/java/org/apache/hadoop/hive/ql/security/DummyHiveMetastoreAuthorizationProvider.java
move_source ql/src/test/org/apache/hadoop/hive/ql/security/InjectableDummyAuthenticator.java itests/util/src/main/java/org/apache/hadoop/hive/ql/security/InjectableDummyAuthenticator.java

move_source ql/src/test/org/apache/hadoop/hive/ql/hooks/VerifyHiveSortedInputFormatUsedHook.java itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/VerifyHiveSortedInputFormatUsedHook.java
move_source ql/src/test/org/apache/hadoop/hive/ql/hooks/CheckTableAccessHook.java itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/CheckTableAccessHook.java
move_source ql/src/test/org/apache/hadoop/hive/ql/hooks/VerifyContentSummaryCacheHook.java itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/VerifyContentSummaryCacheHook.java
move_source ql/src/test/org/apache/hadoop/hive/ql/hooks/VerifySessionStateLocalErrorsHook.java itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/VerifySessionStateLocalErrorsHook.java
move_source ql/src/test/org/apache/hadoop/hive/ql/hooks/VerifySessionStateStackTracesHook.java itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/VerifySessionStateStackTracesHook.java
move_source ql/src/test/org/apache/hadoop/hive/ql/hooks/MapJoinCounterHook.java itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/MapJoinCounterHook.java
move_source ql/src/test/org/apache/hadoop/hive/ql/hooks/CheckQueryPropertiesHook.java itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/CheckQueryPropertiesHook.java
move_source ql/src/test/org/apache/hadoop/hive/ql/hooks/VerifyHooksRunInOrder.java itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/VerifyHooksRunInOrder.java
move_source ql/src/test/org/apache/hadoop/hive/ql/hooks/VerifyOutputTableLocationSchemeIsFileHook.java itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/VerifyOutputTableLocationSchemeIsFileHook.java
move_source ql/src/test/org/apache/hadoop/hive/ql/hooks/VerifyIsLocalModeHook.java itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/VerifyIsLocalModeHook.java
move_source ql/src/test/org/apache/hadoop/hive/ql/hooks/VerifyCachingPrintStreamHook.java itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/VerifyCachingPrintStreamHook.java
move_source ql/src/test/org/apache/hadoop/hive/ql/hooks/VerifyNumReducersHook.java itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/VerifyNumReducersHook.java
move_source ql/src/test/org/apache/hadoop/hive/ql/hooks/VerifyPartitionIsNotSubdirectoryOfTableHook.java itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/VerifyPartitionIsNotSubdirectoryOfTableHook.java
move_source ql/src/test/org/apache/hadoop/hive/ql/hooks/VerifyOverriddenConfigsHook.java itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/VerifyOverriddenConfigsHook.java
move_source ql/src/test/org/apache/hadoop/hive/ql/hooks/VerifyPartitionIsSubdirectoryOfTableHook.java itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/VerifyPartitionIsSubdirectoryOfTableHook.java
move_source ql/src/test/org/apache/hadoop/hive/ql/hooks/OptrStatGroupByHook.java itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/OptrStatGroupByHook.java
move_source ql/src/test/org/apache/hadoop/hive/ql/hooks/CheckColumnAccessHook.java itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/CheckColumnAccessHook.java
move_source ql/src/test/org/apache/hadoop/hive/ql/hooks/VerifyTableDirectoryIsEmptyHook.java itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/VerifyTableDirectoryIsEmptyHook.java

move_source ql/src/test/org/apache/hadoop/hive/ql/udf/UDFTestErrorOnFalse.java itests/util/src/main/java/org/apache/hadoop/hive/ql/udf/UDFTestErrorOnFalse.java

move_source ql/src/test/org/apache/hadoop/hive/ql/stats/DummyStatsPublisher.java itests/util/src/main/java/org/apache/hadoop/hive/ql/stats/DummyStatsPublisher.java
move_source ql/src/test/org/apache/hadoop/hive/ql/stats/DummyStatsAggregator.java itests/util/src/main/java/org/apache/hadoop/hive/ql/stats/DummyStatsAggregator.java
move_source ql/src/test/org/apache/hadoop/hive/ql/stats/KeyVerifyingStatsAggregator.java itests/util/src/main/java/org/apache/hadoop/hive/ql/stats/KeyVerifyingStatsAggregator.java

move_source ql/src/test/org/apache/hadoop/hive/ql/io/udf/Rot13InputFormat.java itests/util/src/main/java/org/apache/hadoop/hive/ql/io/udf/Rot13InputFormat.java
move_source ql/src/test/org/apache/hadoop/hive/ql/io/udf/Rot13OutputFormat.java itests/util/src/main/java/org/apache/hadoop/hive/ql/io/udf/Rot13OutputFormat.java

move_source ql/src/test/org/apache/hadoop/hive/ql/udf/generic/DummyContextUDF.java itests/util/src/main/java/org/apache/hadoop/hive/ql/udf/generic/DummyContextUDF.java
move_source ql/src/test/org/apache/hadoop/hive/ql/udf/generic/GenericUDAFSumList.java itests/util/src/main/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDAFSumList.java
move_source ql/src/test/org/apache/hadoop/hive/ql/udf/generic/GenericUDFEvaluateNPE.java itests/util/src/main/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDFEvaluateNPE.java
move_source ql/src/test/org/apache/hadoop/hive/ql/udf/generic/GenericUDFTestGetJavaBoolean.java itests/util/src/main/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDFTestGetJavaBoolean.java
move_source ql/src/test/org/apache/hadoop/hive/ql/udf/generic/GenericUDFTestGetJavaString.java itests/util/src/main/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDFTestGetJavaString.java
move_source ql/src/test/org/apache/hadoop/hive/ql/udf/generic/GenericUDFTestTranslate.java itests/util/src/main/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDFTestTranslate.java

move_source ql/src/test/org/apache/hadoop/hive/ql/udf/UDAFTestMax.java itests/util/src/main/java/org/apache/hadoop/hive/ql/udf/UDAFTestMax.java
move_source ql/src/test/org/apache/hadoop/hive/ql/udf/UDFTestLength2.java itests/util/src/main/java/org/apache/hadoop/hive/ql/udf/UDFTestLength2.java
move_source ql/src/test/org/apache/hadoop/hive/ql/udf/UDFTestLength.java itests/util/src/main/java/org/apache/hadoop/hive/ql/udf/UDFTestLength.java


move_source ql/src/test/org/apache/hadoop/hive/ql/metadata/DummySemanticAnalyzerHook.java itests/util/src/main/java/org/apache/hadoop/hive/ql/metadata/DummySemanticAnalyzerHook.java
move_source ql/src/test/org/apache/hadoop/hive/ql/metadata/DummySemanticAnalyzerHook1.java itests/util/src/main/java/org/apache/hadoop/hive/ql/metadata/DummySemanticAnalyzerHook1.java
move_source ql/src/test/org/apache/hadoop/hive/ql/metadata/TestSemanticAnalyzerHookLoading.java itests/hive-unit/src/test/java/org/apache/hadoop/hive/ql/metadata/TestSemanticAnalyzerHookLoading.java

move_source hcatalog/core/src/test/java/org/apache/hive/hcatalog/mapreduce/TestSequenceFileReadWrite.java itests/hcatalog-unit/src/test/java/org/apache/hive/hcatalog/mapreduce/TestSequenceFileReadWrite.java
move_source hcatalog/core/src/test/java/org/apache/hcatalog/mapreduce/TestSequenceFileReadWrite.java itests/hcatalog-unit/src/test/java/org/apache/hcatalog/mapreduce/TestSequenceFileReadWrite.java

move_source hcatalog/core/src/test/java/org/apache/hive/hcatalog/mapreduce/TestHCatHiveCompatibility.java itests/hcatalog-unit/src/test/java/org/apache/hive/hcatalog/mapreduce/TestHCatHiveCompatibility.java
move_source hcatalog/core/src/test/java/org/apache/hcatalog/mapreduce/TestHCatHiveCompatibility.java itests/hcatalog-unit/src/test/java/org/apache/hcatalog/mapreduce/TestHCatHiveCompatibility.java
move_source hcatalog/core/src/test/java/org/apache/hive/hcatalog/mapreduce/TestHCatHiveThriftCompatibility.java itests/hcatalog-unit/src/test/java/org/apache/hive/hcatalog/mapreduce/TestHCatHiveThriftCompatibility.java
move_source hcatalog/core/src/test/java/org/apache/hcatalog/mapreduce/TestHCatHiveThriftCompatibility.java itests/hcatalog-unit/src/test/java/org/apache/hcatalog/mapreduce/TestHCatHiveThriftCompatibility.java

move_source hcatalog/storage-handlers/hbase/src/test/org/apache/hive/hcatalog/hbase/TestPigHBaseStorageHandler.java itests/hcatalog-unit/src/test/java/org/apache/hive/hcatalog/hbase/TestPigHBaseStorageHandler.java

move_source ql/src/test/org/apache/hadoop/hive/serde2/TestSerDe.java itests/test-serde/src/main/java/org/apache/hadoop/hive/serde2/TestSerDe.java
move_source ql/src/test/org/apache/hadoop/hive/serde2/CustomNonSettableListObjectInspector1.java itests/custom-serde/src/main/java/org/apache/hadoop/hive/serde2/CustomNonSettableListObjectInspector1.java
move_source ql/src/test/org/apache/hadoop/hive/serde2/CustomNonSettableStructObjectInspector1.java itests/custom-serde/src/main/java/org/apache/hadoop/hive/serde2/CustomNonSettableStructObjectInspector1.java
move_source ql/src/test/org/apache/hadoop/hive/serde2/CustomNonSettableUnionObjectInspector1.java itests/custom-serde/src/main/java/org/apache/hadoop/hive/serde2/CustomNonSettableUnionObjectInspector1.java
move_source ql/src/test/org/apache/hadoop/hive/serde2/CustomSerDe1.java itests/custom-serde/src/main/java/org/apache/hadoop/hive/serde2/CustomSerDe1.java
move_source ql/src/test/org/apache/hadoop/hive/serde2/CustomSerDe2.java itests/custom-serde/src/main/java/org/apache/hadoop/hive/serde2/CustomSerDe2.java
move_source ql/src/test/org/apache/hadoop/hive/serde2/CustomSerDe3.java itests/custom-serde/src/main/java/org/apache/hadoop/hive/serde2/CustomSerDe3.java
move_source ql/src/test/org/apache/hadoop/hive/serde2/CustomSerDe4.java itests/custom-serde/src/main/java/org/apache/hadoop/hive/serde2/CustomSerDe4.java
move_source ql/src/test/org/apache/hadoop/hive/serde2/CustomSerDe5.java itests/custom-serde/src/main/java/org/apache/hadoop/hive/serde2/CustomSerDe5.java

