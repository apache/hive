/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.cli.control;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QTestMiniClusters;
import org.apache.hadoop.hive.ql.QTestMiniClusters.MiniClusterType;
import org.apache.hadoop.hive.ql.parse.CoreParseNegative;

public class CliConfigs {

  private static URL testConfigProps = getTestPropsURL();

  private static URL getTestPropsURL() {
    try {
      return new File(
          AbstractCliConfig.HIVE_ROOT + "/itests/src/test/resources/testconfiguration.properties")
              .toURI().toURL();
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  public static class CliConfig extends AbstractCliConfig {

    public CliConfig() {
      super(CoreCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientpositive");

        includesFrom(testConfigProps, "mr.query.files");

        setResultsDir("ql/src/test/results/clientpositive");
        setLogDir("itests/qtest/target/qfile-results/clientpositive");

        setInitScript("q_test_init.sql");
        setCleanupScript("q_test_cleanup.sql");

        setHiveConfDir("data/conf/mr");
        setClusterType(MiniClusterType.NONE);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class ParseNegativeConfig extends AbstractCliConfig {
    public ParseNegativeConfig() {
      super(CoreParseNegative.class);
      try {
        setQueryDir("ql/src/test/queries/negative");

        setResultsDir("ql/src/test/results/compiler/errors");
        setLogDir("itests/qtest/target/qfile-results/negative");

        setInitScript("q_test_init_parse.sql");
        setCleanupScript("q_test_cleanup.sql");

        // TODO: HIVE-28031: Adapt some cli driver tests to Tez where it's applicable
        setHiveConfDir("data/conf/mr");
        setClusterType(MiniClusterType.NONE);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class MinimrCliConfig extends AbstractCliConfig {
    public MinimrCliConfig() {
      super(CoreCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientpositive");

        includesFrom(testConfigProps, "minimr.query.files");

        setResultsDir("ql/src/test/results/clientpositive");
        setLogDir("itests/qtest/target/qfile-results/clientpositive");

        setInitScript("q_test_init_for_minimr.sql");
        setCleanupScript("q_test_cleanup.sql");

        setHiveConfDir("data/conf/mr");
        setClusterType(MiniClusterType.MR);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class MiniTezCliConfig extends AbstractCliConfig {
    public MiniTezCliConfig() {
      super(CoreCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientpositive");

        includesFrom(testConfigProps, "minitez.query.files");
        includesFrom(testConfigProps, "minitez.query.files.shared");

        setResultsDir("ql/src/test/results/clientpositive/tez");
        setLogDir("itests/qtest/target/qfile-results/clientpositive");

        setInitScript("q_test_init_tez.sql");
        setCleanupScript("q_test_cleanup_tez.sql");

        setHiveConfDir("data/conf/tez");
        setClusterType(MiniClusterType.TEZ);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class MiniLlapCliConfig extends AbstractCliConfig {
    public MiniLlapCliConfig() {
      super(CoreCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientpositive");

        includesFrom(testConfigProps, "minillap.query.files");

        setResultsDir("ql/src/test/results/clientpositive/llap");
        setLogDir("itests/qtest/target/qfile-results/clientpositive");

        setInitScript("q_test_init.sql");
        setCleanupScript("q_test_cleanup.sql");

        setHiveConfDir("data/conf/llap");
        setClusterType(MiniClusterType.LLAP);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class MiniDruidCliConfig extends AbstractCliConfig {
    public MiniDruidCliConfig() {
      super(CoreCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientpositive");

        includesFrom(testConfigProps, "druid.query.files");

        setResultsDir("ql/src/test/results/clientpositive/druid");
        setLogDir("itests/qtest/target/tmp/log");

        setInitScript("q_test_druid_init.sql");
        setCleanupScript("q_test_cleanup_druid.sql");
        setHiveConfDir("data/conf/llap");
        setClusterType(MiniClusterType.DRUID);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class MiniDruidKafkaCliConfig extends AbstractCliConfig {
    public MiniDruidKafkaCliConfig() {
      super(CoreCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientpositive");
        includesFrom(testConfigProps, "druid.kafka.query.files");
        setResultsDir("ql/src/test/results/clientpositive/druid");
        setLogDir("itests/qtest/target/tmp/log");

        setInitScript("q_test_druid_init.sql");
        setCleanupScript("q_test_cleanup_druid.sql");
        setHiveConfDir("data/conf/llap");
        setClusterType(MiniClusterType.DRUID_KAFKA);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class MiniKafkaCliConfig extends AbstractCliConfig {
    public MiniKafkaCliConfig() {
      super(CoreCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientpositive");
        includesFrom(testConfigProps, "hive.kafka.query.files");
        setResultsDir("ql/src/test/results/clientpositive/kafka");
        setLogDir("itests/qtest/target/tmp/log");
        setHiveConfDir("data/conf/llap");
        setClusterType(MiniClusterType.KAFKA);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class MiniLlapLocalCliConfig extends AbstractCliConfig {

    public MiniLlapLocalCliConfig() {
      super(CoreCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientpositive");

        excludesFrom(testConfigProps, "mr.query.files");
        excludesFrom(testConfigProps, "minimr.query.files");
        excludesFrom(testConfigProps, "minillap.query.files");
        excludesFrom(testConfigProps, "minitez.query.files");
        excludesFrom(testConfigProps, "encrypted.query.files");
        excludesFrom(testConfigProps, "druid.query.files");
        excludesFrom(testConfigProps, "druid.kafka.query.files");
        excludesFrom(testConfigProps, "hive.kafka.query.files");
        excludesFrom(testConfigProps, "erasurecoding.only.query.files");
        excludesFrom(testConfigProps, "beeline.positive.include");
        excludesFrom(testConfigProps, "compaction.query.files");
        
        setResultsDir("ql/src/test/results/clientpositive/llap");
        setLogDir("itests/qtest/target/qfile-results/clientpositive");

        setInitScript("q_test_init.sql");
        setCleanupScript("q_test_cleanup.sql");

        setHiveConfDir("data/conf/llap");
        setClusterType(MiniClusterType.LLAP_LOCAL);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }
  
  public static class MiniLlapLocalCompactorCliConfig extends AbstractCliConfig {

    public MiniLlapLocalCompactorCliConfig() {
      super(CoreCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientpositive");

        includesFrom(testConfigProps, "compaction.query.files");
        setResultsDir("ql/src/test/results/clientpositive/llap");
        setLogDir("itests/qtest/target/qfile-results/clientpositive");

        setInitScript("q_test_init.sql");
        setCleanupScript("q_test_cleanup.sql");

        setHiveConfDir("data/conf/llap");
        setClusterType(MiniClusterType.LLAP_LOCAL);
        setCustomConfigValueMap(createConfVarsStringMap());
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }

    private static Map<HiveConf.ConfVars, String> createConfVarsStringMap() {
      Map<HiveConf.ConfVars,String> customConfigValueMap = new HashMap<>();
      customConfigValueMap.put(HiveConf.ConfVars.COMPACTOR_CRUD_QUERY_BASED, "true");
      customConfigValueMap.put(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, "true");
      customConfigValueMap.put(HiveConf.ConfVars.HIVE_TXN_MANAGER, "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
      customConfigValueMap.put(HiveConf.ConfVars.HIVE_COMPACTOR_GATHER_STATS, "false");
      customConfigValueMap.put(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER, "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest");
      return customConfigValueMap;
    }
  }
  public static class EncryptedHDFSCliConfig extends AbstractCliConfig {
    public EncryptedHDFSCliConfig() {
      super(CoreCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientpositive");

        includesFrom(testConfigProps, "encrypted.query.files");

        setResultsDir("ql/src/test/results/clientpositive/encrypted");
        setLogDir("itests/qtest/target/qfile-results/clientpositive");

        setInitScript("q_test_init_src.sql");
        setCleanupScript("q_test_cleanup_src.sql");


        setClusterType(MiniClusterType.MR);
        setFsType(QTestMiniClusters.FsType.ENCRYPTED_HDFS);
        // TODO: HIVE-28031: Adapt some cli driver tests to Tez where it's applicable
        setHiveConfDir("data/conf/mr");
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class ContribCliConfig extends AbstractCliConfig {
    public ContribCliConfig() {
      super(CoreCliDriver.class);
      try {
        setQueryDir("contrib/src/test/queries/clientpositive");

        setResultsDir("contrib/src/test/results/clientpositive");
        setLogDir("itests/qtest/target/qfile-results/contribclientpositive");

        setInitScript("q_test_init_contrib.sql");
        setCleanupScript("q_test_cleanup_contrib.sql");

        // TODO: HIVE-28031: Adapt some cli driver tests to Tez where it's applicable
        setHiveConfDir("data/conf/mr");
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class TezTPCDS30TBCliConfig extends AbstractCliConfig {
    public TezTPCDS30TBCliConfig() {
      super(CorePerfCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientpositive/perf");
        setLogDir("itests/qtest/target/qfile-results/clientpositive/perf/tpcds30tb/tez");
        setResultsDir("ql/src/test/results/clientpositive/perf/tpcds30tb/tez");
        setHiveConfDir("data/conf/perf/tpcds30tb/tez");
        setClusterType(MiniClusterType.LLAP_LOCAL);
        setMetastoreType("postgres.tpcds");
        // The metastore in this configuration can be used only for reading statistics.
        // We cannot exploit the information for running queries so it is impossible to
        // create views or perform other similar operations.
        excludesFrom(testConfigProps, "tez.perf.disabled.query.files");
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class NegativeLlapLocalCliConfig extends AbstractCliConfig {
    public NegativeLlapLocalCliConfig() {
      super(CoreNegativeCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientnegative");

        excludesFrom(testConfigProps, "llap.query.negative.files");

        setResultsDir("ql/src/test/results/clientnegative");
        setLogDir("itests/qtest/target/qfile-results/clientnegative");

        setInitScript("q_test_init.sql");
        setCleanupScript("q_test_cleanup.sql");

        setHiveConfDir("data/conf/llap");
        setClusterType(MiniClusterType.LLAP_LOCAL);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class NegativeLlapCliDriver extends AbstractCliConfig {
    public NegativeLlapCliDriver() {
      super(CoreNegativeCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientnegative");

        includesFrom(testConfigProps, "llap.query.negative.files");

        setResultsDir("ql/src/test/results/clientnegative");
        setLogDir("itests/qtest/target/qfile-results/clientnegative");

        setInitScript("q_test_init_src.sql");
        setCleanupScript("q_test_cleanup_src.sql");

        setHiveConfDir("data/conf/llap");
        setClusterType(MiniClusterType.LLAP);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class HBaseCliConfig extends AbstractCliConfig {
    public HBaseCliConfig() {
      super(CoreHBaseCliDriver.class);
      try {
        setQueryDir("hbase-handler/src/test/queries/positive");

        setResultsDir("hbase-handler/src/test/results/positive");
        setLogDir("itests/qtest/target/qfile-results/hbase-handler/positive");

        setInitScript("q_test_init_src_with_stats.sql");
        setCleanupScript("q_test_cleanup_src.sql");

        // TODO: HIVE-28031: Adapt some cli driver tests to Tez where it's applicable
        setHiveConfDir("data/conf/mr");
        setClusterType(MiniClusterType.NONE);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class HBaseNegativeCliConfig extends AbstractCliConfig {
    public HBaseNegativeCliConfig() {
      super(CoreHBaseNegativeCliDriver.class);
      try {
        setQueryDir("hbase-handler/src/test/queries/negative");

        setResultsDir("hbase-handler/src/test/results/negative");
        setLogDir("itests/qtest/target/qfile-results/hbase-handler/negative");

        setInitScript("q_test_init_src.sql");
        setCleanupScript("q_test_cleanup_src.sql");

        // TODO: HIVE-28031: Adapt some cli driver tests to Tez where it's applicable
        setHiveConfDir("data/conf/mr");
        setClusterType(MiniClusterType.NONE);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class ContribNegativeCliConfig extends AbstractCliConfig {
    public ContribNegativeCliConfig() {
      super(CoreNegativeCliDriver.class);
      try {
        setQueryDir("contrib/src/test/queries/clientnegative");

        setResultsDir("contrib/src/test/results/clientnegative");
        setLogDir("itests/qtest/target/qfile-results/contribclientnegative");

        setInitScript("q_test_init.sql");
        setCleanupScript("q_test_cleanup.sql");

        // TODO: HIVE-28031: Adapt some cli driver tests to Tez where it's applicable
        setHiveConfDir("data/conf/mr");
        setClusterType(MiniClusterType.NONE);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class BeeLineConfig extends AbstractCliConfig {
    public BeeLineConfig() {
      super(CoreBeeLineDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientpositive");

        includesFrom(testConfigProps, "beeline.positive.include");
        includesFrom(testConfigProps, "beeline.query.files.shared");

        setResultsDir("ql/src/test/results/clientpositive/beeline");
        setLogDir("itests/qtest/target/qfile-results/beelinepositive");

        setInitScript("q_test_init_src.sql");
        setCleanupScript("q_test_cleanup_src.sql");

        // TODO: HIVE-28031: Adapt some cli driver tests to Tez where it's applicable
        setHiveConfDir("data/conf/mr");
        setClusterType(MiniClusterType.NONE);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class AccumuloCliConfig extends AbstractCliConfig {
    public AccumuloCliConfig() {
      super(CoreAccumuloCliDriver.class);
      try {
        setQueryDir("accumulo-handler/src/test/queries/positive");

        setResultsDir("accumulo-handler/src/test/results/positive");
        setLogDir("itests/qtest/target/qfile-results/accumulo-handler/positive");

        setInitScript("q_test_init_src_with_stats.sql");
        setCleanupScript("q_test_cleanup_src.sql");

        // TODO: HIVE-28031: Adapt some cli driver tests to Tez where it's applicable
        setHiveConfDir("data/conf/mr");
        setClusterType(MiniClusterType.NONE);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class BlobstoreCliConfig extends AbstractCliConfig {
    public BlobstoreCliConfig() {
      super(CoreBlobstoreCliDriver.class);
      try {
        setQueryDir("itests/hive-blobstore/src/test/queries/clientpositive");

        setResultsDir("itests/hive-blobstore/src/test/results/clientpositive");
        setLogDir("itests/hive-blobstore/target/qfile-results/clientpositive");

        setInitScript("blobstore_test_init.q");
        setCleanupScript("blobstore_test_cleanup.q");

        setHiveConfDir("itests/hive-blobstore/src/test/resources");
        setClusterType(MiniClusterType.NONE);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class BlobstoreNegativeCliConfig extends AbstractCliConfig {
    public BlobstoreNegativeCliConfig() {
      super(CoreBlobstoreNegativeCliDriver.class);
      try {
        setQueryDir("itests/hive-blobstore/src/test/queries/clientnegative");

        setResultsDir("itests/hive-blobstore/src/test/results/clientnegative");
        setLogDir("itests/hive-blobstore/target/qfile-results/clientnegative");

        setInitScript("blobstore_test_init.q");
        setCleanupScript("blobstore_test_cleanup.q");

        setHiveConfDir("itests/hive-blobstore/src/test/resources");
        setClusterType(MiniClusterType.NONE);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  /**
   * Configuration for TestErasureCodingHDFSCliDriver.
   */
  public static class ErasureCodingHDFSCliConfig extends AbstractCliConfig {
    public ErasureCodingHDFSCliConfig() {
      super(CoreCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientpositive");

        includesFrom(testConfigProps, "erasurecoding.only.query.files");

        setResultsDir("ql/src/test/results/clientpositive/erasurecoding");
        setLogDir("itests/qtest/target/qfile-results/clientpositive");

        setInitScript("q_test_init_src.sql");
        setCleanupScript("q_test_cleanup_src.sql");

        setClusterType(MiniClusterType.MR);
        setFsType(QTestMiniClusters.FsType.ERASURE_CODED_HDFS); // override default FsType.HDFS
        setHiveConfDir(getClusterType());
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }

    /**
     * Set the appropriate conf dir based on the cluster type.
     */
    private void setHiveConfDir(MiniClusterType clusterType) {
      switch (clusterType) {
      case TEZ:
        setHiveConfDir("data/conf/tez");
        break;
      default:
        // TODO: HIVE-28031: Adapt some cli driver tests to Tez where it's applicable
        setHiveConfDir("data/conf/mr");
        break;
      }
    }
  }

  public static class MiniDruidLlapLocalCliConfig extends AbstractCliConfig {
    public MiniDruidLlapLocalCliConfig() {
      super(CoreCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientpositive");

        includesFrom(testConfigProps, "druid.llap.local.query.files");

        setResultsDir("ql/src/test/results/clientpositive/druid");
        setLogDir("itests/qtest/target/tmp/log");

        setInitScript("q_test_druid_init.sql");
        setCleanupScript("q_test_cleanup_druid.sql");
        setHiveConfDir("data/conf/llap");
        setClusterType(MiniClusterType.DRUID_LOCAL);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  /**
   * The CliConfig implementation for Kudu.
   */
  public static class KuduCliConfig extends AbstractCliConfig {
    public KuduCliConfig() {
      super(CoreKuduCliDriver.class);
      try {
        setQueryDir("kudu-handler/src/test/queries/positive");

        setResultsDir("kudu-handler/src/test/results/positive");
        setLogDir("itests/qtest/target/qfile-results/kudu/positive");

        setInitScript("q_test_init_src.sql");
        setCleanupScript("q_test_cleanup_src.sql");

        setHiveConfDir("data/conf/llap");
        setClusterType(MiniClusterType.TEZ_LOCAL);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class KuduNegativeCliConfig extends AbstractCliConfig {
    public KuduNegativeCliConfig() {
      super(CoreKuduNegativeCliDriver.class);
      try {
        setQueryDir("kudu-handler/src/test/queries/negative");

        setResultsDir("kudu-handler/src/test/results/negative");
        setLogDir("itests/qtest/target/qfile-results/kudu/negative");

        setInitScript("q_test_init_src.sql");
        setCleanupScript("q_test_cleanup_src.sql");

        setHiveConfDir("data/conf/llap");
        setClusterType(MiniClusterType.TEZ_LOCAL);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class IcebergCliConfig extends AbstractCliConfig {

    public IcebergCliConfig() {
      super(CoreCliDriver.class);
      try {
        setQueryDir("iceberg/iceberg-handler/src/test/queries/positive");
        excludesFrom(testConfigProps, "iceberg.llap.only.query.files");
        excludesFrom(testConfigProps, "iceberg.llap.query.compactor.files");

        setResultsDir("iceberg/iceberg-handler/src/test/results/positive");
        setLogDir("itests/qtest/target/qfile-results/iceberg-handler/positive");
        setInitScript("q_test_init_tez.sql");
        setCleanupScript("q_test_cleanup_tez.sql");
        setHiveConfDir("data/conf/iceberg/tez");
        setClusterType(MiniClusterType.TEZ);
      } catch (Exception e) {
        throw new RuntimeException("can't contruct cliconfig", e);
      }
    }
  }

  public static class IcebergNegativeCliConfig extends AbstractCliConfig {

    public IcebergNegativeCliConfig() {
      super(CoreNegativeCliDriver.class);
      try {
        setQueryDir("iceberg/iceberg-handler/src/test/queries/negative");
        setResultsDir("iceberg/iceberg-handler/src/test/results/negative");
        setLogDir("itests/qtest/target/qfile-results/iceberg-handler/negative");
        setInitScript("q_test_init_tez.sql");
        setCleanupScript("q_test_cleanup_tez.sql");
        setHiveConfDir("data/conf/iceberg/tez");
        setClusterType(MiniClusterType.TEZ);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class IcebergLlapLocalCliConfig extends AbstractCliConfig {

    public IcebergLlapLocalCliConfig() {
      super(CoreCliDriver.class);
      try {
        setQueryDir("iceberg/iceberg-handler/src/test/queries/positive");
        includesFrom(testConfigProps, "iceberg.llap.query.files");

        setResultsDir("iceberg/iceberg-handler/src/test/results/positive/llap");
        setLogDir("itests/qtest/target/qfile-results/iceberg-handler/positive");

        setInitScript("q_test_init_tez.sql");
        setCleanupScript("q_test_cleanup_tez.sql");

        setHiveConfDir("data/conf/iceberg/llap");
        setClusterType(MiniClusterType.LLAP_LOCAL);
      } catch (Exception e) {
        throw new RuntimeException("can't contruct cliconfig", e);
      }
    }
  }

  public static class IcebergLlapLocalCompactorCliConfig extends AbstractCliConfig {

    public IcebergLlapLocalCompactorCliConfig() {
      super(CoreCliDriver.class);
      try {
        setQueryDir("iceberg/iceberg-handler/src/test/queries/positive");

        includesFrom(testConfigProps, "iceberg.llap.query.compactor.files");

        setResultsDir("iceberg/iceberg-handler/src/test/results/positive/llap");
        setLogDir("itests/qtest/target/qfile-results/iceberg-handler/positive");

        setInitScript("q_test_init_tez.sql");
        setCleanupScript("q_test_cleanup_tez.sql");

        setHiveConfDir("data/conf/iceberg/llap");
        setClusterType(MiniClusterType.LLAP_LOCAL);
      } catch (Exception e) {
        throw new RuntimeException("can't contruct cliconfig", e);
      }
    }
  }
}
