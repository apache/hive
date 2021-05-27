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

import org.apache.hadoop.hive.ql.QTestUtil;
import org.apache.hadoop.hive.ql.QTestUtil.MiniClusterType;
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

        setHiveConfDir("");
        setClusterType(MiniClusterType.none);
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

        setHiveConfDir("data/conf/perf-reg/");
        setClusterType(MiniClusterType.none);
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

        setHiveConfDir("");
        setClusterType(MiniClusterType.mr);
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
        setClusterType(MiniClusterType.tez);
        setMetastoreType(MetastoreType.sql);
        setFsType(QTestUtil.FsType.hdfs);
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
        setClusterType(MiniClusterType.llap);
        setMetastoreType(MetastoreType.sql);
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
        setClusterType(MiniClusterType.druid);
        setMetastoreType(MetastoreType.sql);
        setFsType(QTestUtil.FsType.hdfs);
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
        setClusterType(MiniClusterType.druidKafka);
        setMetastoreType(MetastoreType.sql);
        setFsType(QTestUtil.FsType.hdfs);
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
        setClusterType(MiniClusterType.kafka);
        setMetastoreType(MetastoreType.sql);
        setFsType(QTestUtil.FsType.hdfs);
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
        excludesFrom(testConfigProps, "spark.only.query.files");
        excludesFrom(testConfigProps, "localSpark.only.query.files");
        excludesFrom(testConfigProps, "miniSparkOnYarn.only.query.files");

        setResultsDir("ql/src/test/results/clientpositive/llap");
        setLogDir("itests/qtest/target/qfile-results/clientpositive");

        setInitScript("q_test_init.sql");
        setCleanupScript("q_test_cleanup.sql");

        setHiveConfDir("data/conf/llap");
        setClusterType(MiniClusterType.llap_local);
        setMetastoreType(MetastoreType.sql);
        setFsType(QTestUtil.FsType.local);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
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


        setClusterType(MiniClusterType.mr);
        setFsType(QTestUtil.FsType.encrypted_hdfs);
        if (getClusterType() == MiniClusterType.tez) {
          setHiveConfDir("data/conf/tez");
        } else {
          setHiveConfDir("data/conf");
        }

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

        setHiveConfDir("");
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class ImpalaCliConfig extends AbstractCliConfig {
    public ImpalaCliConfig() {
      super(CoreImpalaCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientpositive/impala");

        setResultsDir("ql/src/test/results/clientpositive/impala");
        setLogDir("itests/qtest/target/qfile-results/clientpositive/impala");

        setHiveConfDir("data/conf/impala");
        setClusterType(MiniClusterType.mr);
        setMetastoreType(MetastoreType.sql);
        setFsType(QTestUtil.FsType.hdfs);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class ImpalaNegativeCliConfig extends AbstractCliConfig {
    public ImpalaNegativeCliConfig() {
      super(CoreNegativeCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientnegative/impala");

        setResultsDir("ql/src/test/results/clientnegative/impala");
        setLogDir("itests/qtest/target/qfile-results/clientnegative/impala");

        setHiveConfDir("data/conf/impala");
        setClusterType(MiniClusterType.mr);
        setMetastoreType(MetastoreType.sql);
        setFsType(QTestUtil.FsType.hdfs);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class TezPerfCliConfig extends AbstractCliConfig {
    public TezPerfCliConfig(boolean useConstraints) {
      super(CorePerfCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientpositive/perf");

        if (useConstraints) {
          excludesFrom(testConfigProps, "tez.perf.constraints.disabled.query.files");
        } else {
          excludesFrom(testConfigProps, "tez.perf.disabled.query.files");
        }

        excludesFrom(testConfigProps, "minimr.query.files");
        excludesFrom(testConfigProps, "minitez.query.files");
        excludesFrom(testConfigProps, "encrypted.query.files");
        excludesFrom(testConfigProps, "erasurecoding.only.query.files");

        setLogDir("itests/qtest/target/qfile-results/clientpositive/tez");

        if (useConstraints) {
          setInitScript("q_perf_test_init_constraints.sql");
          setResultsDir("ql/src/test/results/clientpositive/perf/tez/constraints");
        } else {
          setInitScript("q_perf_test_init.sql");
          setResultsDir("ql/src/test/results/clientpositive/perf/tez");
        }
        setCleanupScript("q_perf_test_cleanup.sql");

        setHiveConfDir("data/conf/perf-reg/tez");
        setClusterType(MiniClusterType.tez);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class SparkPerfCliConfig extends AbstractCliConfig {
    public SparkPerfCliConfig() {
      super(CorePerfCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientpositive/perf");

        excludesFrom(testConfigProps, "spark.perf.disabled.query.files");

        setResultsDir("ql/src/test/results/clientpositive/perf/spark");
        setLogDir("itests/qtest/target/qfile-results/clientpositive/spark");

        setInitScript("q_perf_test_init.sql");
        setCleanupScript("q_perf_test_cleanup.sql");

        setHiveConfDir("data/conf/perf-reg/spark");
        setClusterType(MiniClusterType.spark);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class CompareCliConfig extends AbstractCliConfig {
    public CompareCliConfig() {
      super(CoreCompareCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientcompare");

        setResultsDir("ql/src/test/results/clientcompare");
        setLogDir("itests/qtest/target/qfile-results/clientcompare");

        setInitScript("q_test_init_compare.sql");
        setCleanupScript("q_test_cleanup_compare.sql");

        setHiveConfDir("");
        setClusterType(MiniClusterType.none);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class NegativeCliConfig extends AbstractCliConfig {
    public NegativeCliConfig() {
      super(CoreNegativeCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientnegative");

        excludesFrom(testConfigProps, "minimr.query.negative.files");

        setResultsDir("ql/src/test/results/clientnegative");
        setLogDir("itests/qtest/target/qfile-results/clientnegative");

        setInitScript("q_test_init.sql");
        setCleanupScript("q_test_cleanup.sql");

        setHiveConfDir("");
        setClusterType(MiniClusterType.none);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class NegativeMinimrCli extends AbstractCliConfig {
    public NegativeMinimrCli() {
      super(CoreNegativeCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientnegative");

        includesFrom(testConfigProps, "minimr.query.negative.files");

        setResultsDir("ql/src/test/results/clientnegative");
        setLogDir("itests/qtest/target/qfile-results/clientnegative");

        setInitScript("q_test_init_src.sql");
        setCleanupScript("q_test_cleanup_src.sql");

        setHiveConfDir("");
        setClusterType(MiniClusterType.mr);
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

        setHiveConfDir("");
        setClusterType(MiniClusterType.none);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class DummyConfig extends AbstractCliConfig {
    public DummyConfig() {
      super(CoreDummy.class);
      try {
        setQueryDir("ql/src/test/queries/clientcompare");

        setResultsDir("ql/src/test/results/clientcompare");
        setLogDir("itests/qtest/target/qfile-results/clientcompare");

        setInitScript("q_test_init_compare.sql");
        setCleanupScript("q_test_cleanup_compare.sql");

        setHiveConfDir("");
        setClusterType(MiniClusterType.none);
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

        setHiveConfDir("");
        setClusterType(MiniClusterType.none);
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

        setHiveConfDir("");
        setClusterType(MiniClusterType.none);
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

        setHiveConfDir("");
        setClusterType(MiniClusterType.none);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class SparkCliConfig extends AbstractCliConfig {
    public SparkCliConfig() {
      super(CoreCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientpositive");

        includesFrom(testConfigProps, "spark.query.files");
        includesFrom(testConfigProps, "spark.only.query.files");

        setResultsDir("ql/src/test/results/clientpositive/spark");
        setLogDir("itests/qtest-spark/target/qfile-results/clientpositive/spark");

        setInitScript("q_test_init.sql");
        setCleanupScript("q_test_cleanup.sql");

        setHiveConfDir("data/conf/spark/standalone");
        setClusterType(MiniClusterType.spark);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class LocalSparkCliConfig extends AbstractCliConfig {
    public LocalSparkCliConfig() {
      super(CoreCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientpositive");

        includesFrom(testConfigProps, "localSpark.only.query.files");

        setResultsDir("ql/src/test/results/clientpositive/spark");
        setLogDir("itests/qtest-spark/target/qfile-results/clientpositive/spark");

        setInitScript("q_test_init.sql");
        setCleanupScript("q_test_cleanup.sql");

        setHiveConfDir("data/conf/spark/local");
        setClusterType(MiniClusterType.spark);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class SparkOnYarnCliConfig extends AbstractCliConfig {
    public SparkOnYarnCliConfig() {
      super(CoreCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientpositive");

        includesFrom(testConfigProps, "miniSparkOnYarn.query.files");
        includesFrom(testConfigProps, "miniSparkOnYarn.only.query.files");

        setResultsDir("ql/src/test/results/clientpositive/spark");
        setLogDir("itests/qtest-spark/target/qfile-results/clientpositive/spark");

        setInitScript("q_test_init.sql");
        setCleanupScript("q_test_cleanup.sql");

        setHiveConfDir("data/conf/spark/yarn-client");
        setClusterType(MiniClusterType.miniSparkOnYarn);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class SparkNegativeCliConfig extends AbstractCliConfig {
    public SparkNegativeCliConfig() {
      super(CoreNegativeCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientnegative");

        includesFrom(testConfigProps, "spark.query.negative.files");

        setResultsDir("ql/src/test/results/clientnegative/spark");
        setLogDir("itests/qtest-spark/target/qfile-results/clientnegative/spark");

        setInitScript("q_test_init.sql");
        setCleanupScript("q_test_cleanup.sql");

        setHiveConfDir("data/conf/spark/standalone");
        setClusterType(MiniClusterType.spark);
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
        setClusterType(MiniClusterType.none);
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
        setClusterType(MiniClusterType.none);
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

        setInitScript("q_test_init.sql");
        setCleanupScript("q_test_cleanup.sql");

        setClusterType(MiniClusterType.mr);
        setFsType(QTestUtil.FsType.erasure_coded_hdfs);
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
      case tez:
        setHiveConfDir("data/conf/tez");
        break;
      case spark:
        setHiveConfDir("data/conf/spark/standalone");
        break;
      case miniSparkOnYarn:
        setHiveConfDir("data/conf/spark/yarn-cluster");
        break;
      default:
        setHiveConfDir("data/conf");
        break;
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

        setHiveConfDir("");
        setClusterType(MiniClusterType.none);
        setMetastoreType(MetastoreType.sql);
        setFsType(QTestUtil.FsType.local);
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

        setHiveConfDir("");
        setClusterType(MiniClusterType.none);
        setMetastoreType(MetastoreType.sql);
        setFsType(QTestUtil.FsType.local);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }
}
