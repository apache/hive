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

        excludesFrom(testConfigProps, "minillap.query.files");
        excludesFrom(testConfigProps, "minillaplocal.query.files");
        excludesFrom(testConfigProps, "minimr.query.files");
        excludesFrom(testConfigProps, "minitez.query.files");
        excludesFrom(testConfigProps, "encrypted.query.files");
        excludesFrom(testConfigProps, "spark.only.query.files");
        excludesFrom(testConfigProps, "miniSparkOnYarn.only.query.files");
        excludesFrom(testConfigProps, "disabled.query.files");
        excludesFrom(testConfigProps, "localSpark.only.query.files");
        excludesFrom(testConfigProps, "druid.query.files");
        excludesFrom(testConfigProps, "druid.kafka.query.files");

        excludeQuery("fouter_join_ppr.q"); // Disabled in HIVE-19509

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

        setInitScript("q_test_init.sql");
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
        excludesFrom(testConfigProps, "minillap.query.files");
        excludesFrom(testConfigProps, "minillap.shared.query.files");

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
        includesFrom(testConfigProps, "minillap.shared.query.files");

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
        excludeQuery("druid_timestamptz.q"); // Disabled in HIVE-20322
        excludeQuery("druidmini_joins.q"); // Disabled in HIVE-20322
        excludeQuery("druidmini_masking.q"); // Disabled in HIVE-20322
        //excludeQuery("druidmini_test1.q"); // Disabled in HIVE-20322

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

  public static class MiniLlapLocalCliConfig extends AbstractCliConfig {

    public MiniLlapLocalCliConfig() {
      super(CoreCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientpositive");

        includesFrom(testConfigProps, "minillaplocal.query.files");
        includesFrom(testConfigProps, "minillaplocal.shared.query.files");
        excludeQuery("bucket_map_join_tez1.q"); // Disabled in HIVE-19509
        excludeQuery("special_character_in_tabnames_1.q"); // Disabled in HIVE-19509
        excludeQuery("sysdb.q"); // Disabled in HIVE-19509
        excludeQuery("tez_smb_1.q"); // Disabled in HIVE-19509
        excludeQuery("union_fast_stats.q"); // Disabled in HIVE-19509
        excludeQuery("schema_evol_orc_acidvec_part.q"); // Disabled in HIVE-19509
        excludeQuery("schema_evol_orc_vec_part_llap_io.q"); // Disabled in HIVE-19509

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

  public static class TezPerfCliConfig extends AbstractCliConfig {
    public TezPerfCliConfig() {
      super(CorePerfCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientpositive/perf");

        excludesFrom(testConfigProps, "minimr.query.files");
        excludesFrom(testConfigProps, "minitez.query.files");
        excludesFrom(testConfigProps, "encrypted.query.files");

        setResultsDir("ql/src/test/results/clientpositive/perf/tez");
        setLogDir("itests/qtest/target/qfile-results/clientpositive/tez");

        setInitScript("q_perf_test_init.sql");
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
        excludeQuery("authorization_uri_import.q");
        excludeQuery("spark_job_max_tasks.q");
        excludeQuery("spark_stage_max_tasks.q");

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

  public static class AccumuloCliConfig extends AbstractCliConfig {
    public AccumuloCliConfig() {
      super(CoreAccumuloCliDriver.class);
      try {
        setQueryDir("accumulo-handler/src/test/queries/positive");

        setResultsDir("accumulo-handler/src/test/results/positive");
        setLogDir("itests/qtest/target/qfile-results/accumulo-handler/positive");

        setInitScript("q_test_init_src_with_stats.sql");
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
}
