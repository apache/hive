package org.apache.hadoop.hive.cli.control;
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



import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.hive.ql.QTestUtil;
import org.apache.hadoop.hive.ql.QTestUtil.MiniClusterType;
import org.junit.After;
import org.junit.AfterClass;
/**
 This is the TestPerformance Cli Driver for integrating performance regression tests
 as part of the Hive Unit tests.
 Currently this includes support for :
 1. Running explain plans for TPCDS workload (non-partitioned dataset)  on 30TB scaleset.
 TODO :
 1. Support for partitioned data set
 2. Use HBase Metastore instead of Derby

This suite differs from TestCliDriver w.r.t the fact that we modify the underlying metastore
database to reflect the dataset before running the queries.
*/
public class CorePerfCliDriver extends CliAdapter{

  private static QTestUtil qt;

  public CorePerfCliDriver(AbstractCliConfig testCliConfig) {
    super(testCliConfig);
  }

  @Override
  public void beforeClass() {
    System.setProperty("datanucleus.schema.autoCreateAll", "true");
    System.setProperty("hive.metastore.schema.verification", "false");
    MiniClusterType miniMR = cliConfig.getClusterType();
    String hiveConfDir = cliConfig.getHiveConfDir();
    String initScript = cliConfig.getInitScript();
    String cleanupScript = cliConfig.getCleanupScript();
    try {
      String hadoopVer = cliConfig.getHadoopVersion();
      qt = new QTestUtil(cliConfig.getResultsDir(), cliConfig.getLogDir(), miniMR, hiveConfDir,
          hadoopVer, initScript,
          cleanupScript, false, false);

      // do a one time initialization
      qt.cleanUp();
      qt.createSources();
      // Manually modify the underlying metastore db to reflect statistics corresponding to
      // the 30TB TPCDS scale set. This way the optimizer will generate plans for a 30 TB set.
      QTestUtil.setupMetaStoreTableColumnStatsFor30TBTPCDSWorkload(qt.getConf());
    } catch (Exception e) {
      System.err.println("Exception: " + e.getMessage());
      e.printStackTrace();
      System.err.flush();
      throw new RuntimeException("Unexpected exception in static initialization: " + e.getMessage(),
          e);
    }
  }

  @Override
  @AfterClass
  public void shutdown() throws Exception {
    qt.shutdown();
  }

  @Override
  public void setUp() {
  }

  @Override
  @After
  public void tearDown() {
    try {
      qt.clearPostTestEffects();
    } catch (Exception e) {
      System.err.println("Exception: " + e.getMessage());
      e.printStackTrace();
      System.err.flush();
      fail("Unexpected exception in tearDown");
    }
  }

  static String debugHint =
      "\nSee ./ql/target/tmp/log/hive.log or ./itests/qtest/target/tmp/log/hive.log, "
          + "or check ./ql/target/surefire-reports or ./itests/qtest/target/surefire-reports/ for specific test cases logs.";


  @Override
  public void runTest(String name, String fname, String fpath) throws Exception {
    long startTime = System.currentTimeMillis();
    try {
      System.err.println("Begin query: " + fname);

      qt.addFile(fpath);

      if (qt.shouldBeSkipped(fname)) {
        return;
      }

      qt.cliInit(fname, false);

      int ecode = qt.executeClient(fname);
      if (ecode != 0) {
        qt.failed(ecode, fname, debugHint);
      }
      ecode = qt.checkCliDriverResults(fname);
      if (ecode != 0) {
        qt.failedDiff(ecode, fname, debugHint);
      }
    } catch (Throwable e) {
      qt.failed(e, fname, debugHint);
    }

    long elapsedTime = System.currentTimeMillis() - startTime;
    System.err.println("Done query: " + fname + " elapsedTime=" + elapsedTime / 1000 + "s");
    assertTrue("Test passed", true);
  }


}
