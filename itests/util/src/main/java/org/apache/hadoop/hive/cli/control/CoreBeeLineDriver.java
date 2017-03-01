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

import static org.junit.Assert.fail;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.beeline.util.QFileClient;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.HashMap;

public class CoreBeeLineDriver extends CliAdapter {
  private final String hiveRootDirectory = AbstractCliConfig.HIVE_ROOT;
  private final String queryDirectory;
  private final String logDirectory;
  private final String resultsDirectory;
  private final String initScript;
  private final String cleanupScript;
  private boolean overwrite = false;
  private MiniHS2 miniHS2;
//  private static QTestUtil.QTestSetup miniZKCluster = null;

  public CoreBeeLineDriver(AbstractCliConfig testCliConfig) {
    super(testCliConfig);
    queryDirectory = testCliConfig.getQueryDirectory();
    logDirectory = testCliConfig.getLogDir();
    resultsDirectory = testCliConfig.getResultsDir();
    initScript = testCliConfig.getInitScript();
    cleanupScript = testCliConfig.getCleanupScript();
  }

  @Override
  @BeforeClass
  public void beforeClass() throws Exception {
    String testOutputOverwrite = System.getProperty("test.output.overwrite");
    if (testOutputOverwrite != null && "true".equalsIgnoreCase(testOutputOverwrite)) {
      overwrite = true;
    }

    String disableserver = System.getProperty("test.service.disable.server");
    if (null != disableserver && disableserver.equalsIgnoreCase("true")) {
      System.err.println("test.service.disable.server=true Skipping HiveServer2 initialization!");
      return;
    }

    HiveConf hiveConf = new HiveConf();
    // We do not need Zookeeper at the moment
    hiveConf.set(HiveConf.ConfVars.HIVE_LOCK_MANAGER.varname,
        "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");

    // But if we need later we can enable it with this, or create one ourself
//    miniZKCluster = new QTestUtil.QTestSetup();
//    miniZKCluster.preTest(hiveConf);

    hiveConf.logVars(System.err);
    System.err.flush();

    miniHS2 = new MiniHS2.Builder().withConf(hiveConf).cleanupLocalDirOnStartup(true).build();

    miniHS2.start(new HashMap<String, String>());
  }


  @Override
  @AfterClass
  public void shutdown() throws Exception {
    if (miniHS2 != null) {
      miniHS2.stop();
    }
//    if (miniZKCluster != null) {
//      miniZKCluster.tearDown();
//    }
  }

  public void runTest(String qFileName) throws Exception {
    QFileClient qClient = new QFileClient(miniHS2.getHiveConf(), hiveRootDirectory,
        queryDirectory, logDirectory, resultsDirectory, initScript, cleanupScript)
    .setQFileName(qFileName)
    .setUsername("user")
    .setPassword("password")
    .setJdbcUrl(miniHS2.getJdbcURL())
    .setJdbcDriver("org.apache.hive.jdbc.HiveDriver")
    .setTestDataDirectory(hiveRootDirectory + "/data/files")
    .setTestScriptDirectory(hiveRootDirectory + "/data/scripts");

    long startTime = System.currentTimeMillis();
    System.err.println(">>> STARTED " + qFileName
        + " (Thread " + Thread.currentThread().getName() + ")");
    try {
      qClient.run();
    } catch (Exception e) {
      System.err.println(">>> FAILED " + qFileName + " with exception:");
      e.printStackTrace();
      throw e;
    }
    long elapsedTime = (System.currentTimeMillis() - startTime)/1000;
    String time = "(" + elapsedTime + "s)";

    if (qClient.compareResults()) {
      System.err.println(">>> PASSED " + qFileName + " " + time);
    } else {
      if (qClient.hasErrors()) {
        System.err.println(">>> FAILED " + qFileName + " (ERROR) " + time);
        fail();
      }
      if (overwrite) {
        System.err.println(">>> PASSED " + qFileName + " (OVERWRITE) " + time);
        qClient.overwriteResults();
      } else {
        System.err.println(">>> FAILED " + qFileName + " (DIFF) " + time);
        fail();
      }
    }
  }

  @Override
  public void setUp() {
  }

  @Override
  public void tearDown() {
  }

  @Override
  public void runTest(String name, String name2, String absolutePath) throws Exception {
    runTest(name2);
  }
}
