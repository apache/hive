# rollback file, generated with:
set -e
move_source() {
  source=$1
  target=$2
  mkdir -p $(dirname $target)
  mv $source $target
}
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/serde2/TestSerdeWithFieldComments.java serde/src/test/org/apache/hadoop/hive/serde2/TestSerdeWithFieldComments.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/serde2/dynamic_type/TestDynamicSerDe.java serde/src/test/org/apache/hadoop/hive/serde2/dynamic_type/TestDynamicSerDe.java
move_source itests/hive-unit/src/test/java/org/apache/hive/service/cli/thrift/TestThriftHttpCLIService.java service/src/test/org/apache/hive/service/cli/thrift/TestThriftHttpCLIService.java
move_source itests/hive-unit/src/test/java/org/apache/hive/service/auth/TestCustomAuthentication.java service/src/test/org/apache/hive/service/auth/TestCustomAuthentication.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreEventListenerOnlyOnCommit.java metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreEventListenerOnlyOnCommit.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreEndFunctionListener.java metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreEndFunctionListener.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestHiveMetaStore.java metastore/src/test/org/apache/hadoop/hive/metastore/TestHiveMetaStore.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestHiveMetaStoreWithEnvironmentContext.java metastore/src/test/org/apache/hadoop/hive/metastore/TestHiveMetaStoreWithEnvironmentContext.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreConnectionUrlHook.java metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreConnectionUrlHook.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestPartitionNameWhitelistValidation.java metastore/src/test/org/apache/hadoop/hive/metastore/TestPartitionNameWhitelistValidation.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreInitListener.java metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreInitListener.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestMarkPartition.java metastore/src/test/org/apache/hadoop/hive/metastore/TestMarkPartition.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestMarkPartitionRemote.java metastore/src/test/org/apache/hadoop/hive/metastore/TestMarkPartitionRemote.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreEventListener.java metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreEventListener.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestRemoteHiveMetaStore.java metastore/src/test/org/apache/hadoop/hive/metastore/TestRemoteHiveMetaStore.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestEmbeddedHiveMetaStore.java metastore/src/test/org/apache/hadoop/hive/metastore/TestEmbeddedHiveMetaStore.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestSetUGIOnBothClientServer.java metastore/src/test/org/apache/hadoop/hive/metastore/TestSetUGIOnBothClientServer.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestSetUGIOnOnlyServer.java metastore/src/test/org/apache/hadoop/hive/metastore/TestSetUGIOnOnlyServer.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestSetUGIOnOnlyClient.java metastore/src/test/org/apache/hadoop/hive/metastore/TestSetUGIOnOnlyClient.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestMetastoreVersion.java metastore/src/test/org/apache/hadoop/hive/metastore/TestMetastoreVersion.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestHiveMetaTool.java metastore/src/test/org/apache/hadoop/hive/metastore/TestHiveMetaTool.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestRawStoreTxn.java metastore/src/test/org/apache/hadoop/hive/metastore/TestRawStoreTxn.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreListenersError.java metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreListenersError.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestRetryingHMSHandler.java metastore/src/test/org/apache/hadoop/hive/metastore/TestRetryingHMSHandler.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestRemoteUGIHiveMetaStoreIpAddress.java metastore/src/test/org/apache/hadoop/hive/metastore/TestRemoteUGIHiveMetaStoreIpAddress.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestMetaStoreAuthorization.java metastore/src/test/org/apache/hadoop/hive/metastore/TestMetaStoreAuthorization.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/metastore/TestRemoteHiveMetaStoreIpAddress.java metastore/src/test/org/apache/hadoop/hive/metastore/TestRemoteHiveMetaStoreIpAddress.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/ql/security/TestStorageBasedClientSideAuthorizationProvider.java ql/src/test/org/apache/hadoop/hive/ql/security/TestStorageBasedClientSideAuthorizationProvider.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/ql/security/TestClientSideAuthorizationProvider.java ql/src/test/org/apache/hadoop/hive/ql/security/TestClientSideAuthorizationProvider.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/ql/security/TestStorageBasedMetastoreAuthorizationProvider.java ql/src/test/org/apache/hadoop/hive/ql/security/TestStorageBasedMetastoreAuthorizationProvider.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/ql/security/TestMetastoreAuthorizationProvider.java ql/src/test/org/apache/hadoop/hive/ql/security/TestMetastoreAuthorizationProvider.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/ql/security/TestAuthorizationPreEventListener.java ql/src/test/org/apache/hadoop/hive/ql/security/TestAuthorizationPreEventListener.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/ql/history/TestHiveHistory.java ql/src/test/org/apache/hadoop/hive/ql/history/TestHiveHistory.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/QTestUtil.java ql/src/test/org/apache/hadoop/hive/ql/QTestUtil.java
move_source itests/hive-unit/src/test/java/org/apache/hive/service/server/TestHiveServer2Concurrency.java service/src/test/org/apache/hive/service/server/TestHiveServer2Concurrency.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/ql/BaseTestQueries.java ql/src/test/org/apache/hadoop/hive/ql/BaseTestQueries.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/ql/TestLocationQueries.java ql/src/test/org/apache/hadoop/hive/ql/TestLocationQueries.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/ql/TestMTQueries.java ql/src/test/org/apache/hadoop/hive/ql/TestMTQueries.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/hbase/HBaseQTestUtil.java hbase-handler/src/test/org/apache/hadoop/hive/hbase/HBaseQTestUtil.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/hbase/HBaseTestSetup.java hbase-handler/src/test/org/apache/hadoop/hive/hbase/HBaseTestSetup.java

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

move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/thrift/TestDBTokenStore.java shims/src/common-secure/test/org/apache/hadoop/hive/thrift/TestDBTokenStore.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/thrift/TestHadoop20SAuthBridge.java shims/src/common-secure/test/org/apache/hadoop/hive/thrift/TestHadoop20SAuthBridge.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/thrift/TestZooKeeperTokenStore.java shims/src/common-secure/test/org/apache/hadoop/hive/thrift/TestZooKeeperTokenStore.java

move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/jdbc/TestJdbcDriver.java jdbc/src/test/org/apache/hadoop/hive/jdbc/TestJdbcDriver.java
move_source itests/hive-unit/src/test/java/org/apache/hive/jdbc/TestJdbcDriver2.java jdbc/src/test/org/apache/hive/jdbc/TestJdbcDriver2.java

move_source itests/hive-unit/src/test/java/org/apache/hive/service/cli/TestEmbeddedThriftBinaryCLIService.java service/src/test/org/apache/hive/service/cli/TestEmbeddedThriftBinaryCLIService.java
move_source itests/hive-unit/src/test/java/org/apache/hive/service/cli/thrift/TestThriftBinaryCLIService.java service/src/test/org/apache/hive/service/cli/thrift/TestThriftBinaryCLIService.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/service/TestHiveServer.java service/src/test/org/apache/hadoop/hive/service/TestHiveServer.java

move_source itests/util/src/main/java/org/apache/hadoop/hive/scripts/extracturl.java ql/src/test/org/apache/hadoop/hive/scripts/extracturl.java

move_source itests/hive-unit/src/test/java/org/apache/hive/beeline/TestSchemaTool.java beeline/src/test/org/apache/hive/beeline/src/test/TestSchemaTool.java

move_source beeline/src/main/resources/sql-keywords.properties beeline/src/java/org/apache/hive/beeline/sql-keywords.properties
move_source beeline/src/main/resources/BeeLine.properties beeline/src/java/org/apache/hive/beeline/BeeLine.properties

move_source ql/src/main/resources/hive-exec-log4j.properties ql/src/java/conf/hive-exec-log4j.properties
move_source common/src/main/resources/hive-log4j.properties common/src/java/conf/hive-log4j.properties

move_source metastore/src/test/org/apache/hadoop/hive/metastore/VerifyingObjectStore.java ql/src/test/org/apache/hadoop/hive/metastore/VerifyingObjectStore.java
move_source ql/src/java/org/apache/hadoop/hive/ql/hooks/PreExecutePrinter.java ql/src/test/org/apache/hadoop/hive/ql/hooks/PreExecutePrinter.java
move_source ql/src/java/org/apache/hadoop/hive/ql/hooks/PostExecutePrinter.java ql/src/test/org/apache/hadoop/hive/ql/hooks/PostExecutePrinter.java
move_source ql/src/java/org/apache/hadoop/hive/ql/hooks/EnforceReadOnlyTables.java ql/src/test/org/apache/hadoop/hive/ql/hooks/EnforceReadOnlyTables.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/security/DummyAuthenticator.java ql/src/test/org/apache/hadoop/hive/ql/security/DummyAuthenticator.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/security/DummyHiveMetastoreAuthorizationProvider.java ql/src/test/org/apache/hadoop/hive/ql/security/DummyHiveMetastoreAuthorizationProvider.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/security/InjectableDummyAuthenticator.java ql/src/test/org/apache/hadoop/hive/ql/security/InjectableDummyAuthenticator.java


move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/VerifyHiveSortedInputFormatUsedHook.java ql/src/test/org/apache/hadoop/hive/ql/hooks/VerifyHiveSortedInputFormatUsedHook.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/CheckTableAccessHook.java ql/src/test/org/apache/hadoop/hive/ql/hooks/CheckTableAccessHook.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/VerifyContentSummaryCacheHook.java ql/src/test/org/apache/hadoop/hive/ql/hooks/VerifyContentSummaryCacheHook.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/VerifySessionStateLocalErrorsHook.java ql/src/test/org/apache/hadoop/hive/ql/hooks/VerifySessionStateLocalErrorsHook.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/VerifySessionStateStackTracesHook.java ql/src/test/org/apache/hadoop/hive/ql/hooks/VerifySessionStateStackTracesHook.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/MapJoinCounterHook.java ql/src/test/org/apache/hadoop/hive/ql/hooks/MapJoinCounterHook.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/CheckQueryPropertiesHook.java ql/src/test/org/apache/hadoop/hive/ql/hooks/CheckQueryPropertiesHook.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/VerifyHooksRunInOrder.java ql/src/test/org/apache/hadoop/hive/ql/hooks/VerifyHooksRunInOrder.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/VerifyOutputTableLocationSchemeIsFileHook.java ql/src/test/org/apache/hadoop/hive/ql/hooks/VerifyOutputTableLocationSchemeIsFileHook.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/VerifyIsLocalModeHook.java ql/src/test/org/apache/hadoop/hive/ql/hooks/VerifyIsLocalModeHook.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/VerifyCachingPrintStreamHook.java ql/src/test/org/apache/hadoop/hive/ql/hooks/VerifyCachingPrintStreamHook.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/VerifyNumReducersHook.java ql/src/test/org/apache/hadoop/hive/ql/hooks/VerifyNumReducersHook.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/VerifyPartitionIsNotSubdirectoryOfTableHook.java ql/src/test/org/apache/hadoop/hive/ql/hooks/VerifyPartitionIsNotSubdirectoryOfTableHook.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/VerifyOverriddenConfigsHook.java ql/src/test/org/apache/hadoop/hive/ql/hooks/VerifyOverriddenConfigsHook.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/VerifyPartitionIsSubdirectoryOfTableHook.java ql/src/test/org/apache/hadoop/hive/ql/hooks/VerifyPartitionIsSubdirectoryOfTableHook.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/OptrStatGroupByHook.java ql/src/test/org/apache/hadoop/hive/ql/hooks/OptrStatGroupByHook.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/CheckColumnAccessHook.java ql/src/test/org/apache/hadoop/hive/ql/hooks/CheckColumnAccessHook.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/hooks/VerifyTableDirectoryIsEmptyHook.java ql/src/test/org/apache/hadoop/hive/ql/hooks/VerifyTableDirectoryIsEmptyHook.java

move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/udf/UDFTestErrorOnFalse.java ql/src/test/org/apache/hadoop/hive/ql/udf/UDFTestErrorOnFalse.java

move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/stats/DummyStatsPublisher.java ql/src/test/org/apache/hadoop/hive/ql/stats/DummyStatsPublisher.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/stats/DummyStatsAggregator.java ql/src/test/org/apache/hadoop/hive/ql/stats/DummyStatsAggregator.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/stats/KeyVerifyingStatsAggregator.java ql/src/test/org/apache/hadoop/hive/ql/stats/KeyVerifyingStatsAggregator.java

move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/io/udf/Rot13InputFormat.java ql/src/test/org/apache/hadoop/hive/ql/io/udf/Rot13InputFormat.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/io/udf/Rot13OutputFormat.java ql/src/test/org/apache/hadoop/hive/ql/io/udf/Rot13OutputFormat.java

move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/udf/generic/DummyContextUDF.java ql/src/test/org/apache/hadoop/hive/ql/udf/generic/DummyContextUDF.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDAFSumList.java ql/src/test/org/apache/hadoop/hive/ql/udf/generic/GenericUDAFSumList.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDFEvaluateNPE.java ql/src/test/org/apache/hadoop/hive/ql/udf/generic/GenericUDFEvaluateNPE.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDFTestGetJavaBoolean.java ql/src/test/org/apache/hadoop/hive/ql/udf/generic/GenericUDFTestGetJavaBoolean.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDFTestGetJavaString.java ql/src/test/org/apache/hadoop/hive/ql/udf/generic/GenericUDFTestGetJavaString.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDFTestTranslate.java ql/src/test/org/apache/hadoop/hive/ql/udf/generic/GenericUDFTestTranslate.java

move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/udf/UDAFTestMax.java ql/src/test/org/apache/hadoop/hive/ql/udf/UDAFTestMax.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/udf/UDFTestLength2.java ql/src/test/org/apache/hadoop/hive/ql/udf/UDFTestLength2.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/udf/UDFTestLength.java ql/src/test/org/apache/hadoop/hive/ql/udf/UDFTestLength.java

move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/metadata/DummySemanticAnalyzerHook.java ql/src/test/org/apache/hadoop/hive/ql/metadata/DummySemanticAnalyzerHook.java
move_source itests/util/src/main/java/org/apache/hadoop/hive/ql/metadata/DummySemanticAnalyzerHook1.java ql/src/test/org/apache/hadoop/hive/ql/metadata/DummySemanticAnalyzerHook1.java
move_source itests/hive-unit/src/test/java/org/apache/hadoop/hive/ql/metadata/TestSemanticAnalyzerHookLoading.java ql/src/test/org/apache/hadoop/hive/ql/metadata/TestSemanticAnalyzerHookLoading.java

move_source itests/hcatalog-unit/src/test/java/org/apache/hive/hcatalog/mapreduce/TestSequenceFileReadWrite.java hcatalog/core/src/test/java/org/apache/hive/hcatalog/mapreduce/TestSequenceFileReadWrite.java
move_source itests/hcatalog-unit/src/test/java/org/apache/hcatalog/mapreduce/TestSequenceFileReadWrite.java hcatalog/core/src/test/java/org/apache/hcatalog/mapreduce/TestSequenceFileReadWrite.java

move_source itests/hcatalog-unit/src/test/java/org/apache/hive/hcatalog/mapreduce/TestHCatHiveCompatibility.java hcatalog/core/src/test/java/org/apache/hive/hcatalog/mapreduce/TestHCatHiveCompatibility.java
move_source itests/hcatalog-unit/src/test/java/org/apache/hcatalog/mapreduce/TestHCatHiveCompatibility.java hcatalog/core/src/test/java/org/apache/hcatalog/mapreduce/TestHCatHiveCompatibility.java
move_source itests/hcatalog-unit/src/test/java/org/apache/hive/hcatalog/mapreduce/TestHCatHiveThriftCompatibility.java  hcatalog/core/src/test/java/org/apache/hive/hcatalog/mapreduce/TestHCatHiveThriftCompatibility.java
move_source itests/hcatalog-unit/src/test/java/org/apache/hcatalog/mapreduce/TestHCatHiveThriftCompatibility.java hcatalog/core/src/test/java/org/apache/hcatalog/mapreduce/TestHCatHiveThriftCompatibility.java

move_source itests/hcatalog-unit/src/test/java/org/apache/hive/hcatalog/hbase/TestPigHBaseStorageHandler.java hcatalog/storage-handlers/hbase/src/test/org/apache/hive/hcatalog/hbase/TestPigHBaseStorageHandler.java

move_source itests/test-serde/src/main/java/org/apache/hadoop/hive/serde2/TestSerDe.java ql/src/test/org/apache/hadoop/hive/serde2/TestSerDe.java
move_source itests/custom-serde/src/main/java/org/apache/hadoop/hive/serde2/CustomNonSettableListObjectInspector1.java ql/src/test/org/apache/hadoop/hive/serde2/CustomNonSettableListObjectInspector1.java
move_source itests/custom-serde/src/main/java/org/apache/hadoop/hive/serde2/CustomNonSettableStructObjectInspector1.java ql/src/test/org/apache/hadoop/hive/serde2/CustomNonSettableStructObjectInspector1.java
move_source itests/custom-serde/src/main/java/org/apache/hadoop/hive/serde2/CustomNonSettableUnionObjectInspector1.java ql/src/test/org/apache/hadoop/hive/serde2/CustomNonSettableUnionObjectInspector1.java
move_source itests/custom-serde/src/main/java/org/apache/hadoop/hive/serde2/CustomSerDe1.java ql/src/test/org/apache/hadoop/hive/serde2/CustomSerDe1.java
move_source itests/custom-serde/src/main/java/org/apache/hadoop/hive/serde2/CustomSerDe2.java ql/src/test/org/apache/hadoop/hive/serde2/CustomSerDe2.java
move_source itests/custom-serde/src/main/java/org/apache/hadoop/hive/serde2/CustomSerDe3.java ql/src/test/org/apache/hadoop/hive/serde2/CustomSerDe3.java
move_source itests/custom-serde/src/main/java/org/apache/hadoop/hive/serde2/CustomSerDe4.java ql/src/test/org/apache/hadoop/hive/serde2/CustomSerDe4.java
move_source itests/custom-serde/src/main/java/org/apache/hadoop/hive/serde2/CustomSerDe5.java ql/src/test/org/apache/hadoop/hive/serde2/CustomSerDe5.java

