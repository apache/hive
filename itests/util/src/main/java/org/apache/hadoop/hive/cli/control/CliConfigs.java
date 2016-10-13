/**
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
        excludesFrom(testConfigProps, "minimr.query.files");
        excludesFrom(testConfigProps, "minitez.query.files");
        excludesFrom(testConfigProps, "encrypted.query.files");
        excludesFrom(testConfigProps, "spark.only.query.files");
        excludesFrom(testConfigProps, "disabled.query.files");

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

        setInitScript("q_test_init.sql");
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

        setInitScript("q_test_init.sql");
        setCleanupScript("q_test_cleanup.sql");

        setHiveConfDir("data/conf/tez");
        setClusterType(MiniClusterType.tez);
        setMetastoreType(MetastoreType.hbase);
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

        setInitScript("q_test_init_for_encryption.sql");
        setCleanupScript("q_test_cleanup_for_encryption.sql");

        setHiveConfDir("data/conf");
        setClusterType(MiniClusterType.encrypted);
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

        setInitScript("q_test_init.sql");
        setCleanupScript("q_test_cleanup.sql");

        setHiveConfDir("");
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class PerfCliConfig extends AbstractCliConfig {
    public PerfCliConfig() {
      super(CorePerfCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientpositive/perf");

        excludesFrom(testConfigProps, "minimr.query.files");
        excludesFrom(testConfigProps, "minitez.query.files");
        excludesFrom(testConfigProps, "encrypted.query.files");

        setResultsDir("ql/src/test/results/clientpositive/perf/");
        setLogDir("itests/qtest/target/qfile-results/clientpositive/");

        setInitScript("q_perf_test_init.sql");
        setCleanupScript("q_perf_test_cleanup.sql");

        setHiveConfDir("data/conf/perf-reg/");
        setClusterType(MiniClusterType.tez);
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

        setInitScript("q_test_init.sql");
        setCleanupScript("q_test_cleanup.sql");

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

        setInitScript("q_test_init.sql");
        setCleanupScript("q_test_cleanup.sql");

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

        setInitScript("q_test_init.sql");
        setCleanupScript("q_test_cleanup.sql");

        setHiveConfDir("");
        setClusterType(MiniClusterType.none);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  public static class HBaseMinimrCliConfig extends AbstractCliConfig {
    public HBaseMinimrCliConfig() {
      super(CoreHBaseCliDriver.class);
      try {
        setQueryDir("hbase-handler/src/test/queries/positive");
        // XXX: i think this was non intentionally set to run only hbase_bulk.m???
        // includeQuery("hbase_bulk.m"); => will be filter out because not ends with .q
        // to keep existing behaviour i added this method
        overrideUserQueryFile("hbase_bulk.m");

        setResultsDir("hbase-handler/src/test/results/positive");
        setLogDir("itests/qtest/target/qfile-results/hbase-handler/minimrpositive");
        setInitScript("q_test_init.sql");
        setCleanupScript("q_test_cleanup.sql");
        setHiveConfDir("");
        setClusterType(MiniClusterType.mr);
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

        setInitScript("q_test_init.sql");
        setCleanupScript("q_test_cleanup.sql");

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

        setInitScript("q_test_init.sql");
        setCleanupScript("q_test_cleanup.sql");

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
      // FIXME: beeline is disabled...
      super(null);
      // super(CoreBeeLineDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientpositive");

        excludesFrom(testConfigProps, "beeline.positive.exclude");

        setResultsDir("ql/src/test/results/clientpositive");
        setLogDir("itests/qtest/target/qfile-results/beelinepositive");

        setInitScript("q_test_init.sql");
        setCleanupScript("q_test_cleanup.sql");

        setHiveConfDir("");
        setClusterType(MiniClusterType.none);
      } catch (Exception e) {
        throw new RuntimeException("can't construct cliconfig", e);
      }
    }
  }

  // XXX: pending merge of ACC ; and upgrade of executor
  public static class AccumuloCliConfig extends AbstractCliConfig {
    public AccumuloCliConfig() {
      super(null);
      // super(CoreAccumuloCliDriver.class);
      try {
        setQueryDir("accumulo-handler/src/test/queries/positive");

        excludesFrom(testConfigProps, "beeline.positive.exclude");

        setResultsDir("accumulo-handler/src/test/results/positive");
        setLogDir("itests/qtest/target/qfile-results/accumulo-handler/positive");

        setInitScript("q_test_init.sql");
        setCleanupScript("q_test_cleanup.sql");

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

  public static class SparkOnYarnCliConfig extends AbstractCliConfig {
    public SparkOnYarnCliConfig() {
      super(CoreCliDriver.class);
      try {
        setQueryDir("ql/src/test/queries/clientpositive");

        includesFrom(testConfigProps, "miniSparkOnYarn.query.files");

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

        setResultsDir("ql/src/test/results/clientnegative");
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
