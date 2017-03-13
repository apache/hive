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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.PreExecutePrinter;
import org.apache.hive.beeline.qfile.QFile;
import org.apache.hive.beeline.qfile.QFile.QFileBuilder;
import org.apache.hive.beeline.qfile.QFileBeeLineClient;
import org.apache.hive.beeline.qfile.QFileBeeLineClient.QFileClientBuilder;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

public class CoreBeeLineDriver extends CliAdapter {
  private final File hiveRootDirectory = new File(AbstractCliConfig.HIVE_ROOT);
  private final File queryDirectory;
  private final File logDirectory;
  private final File resultsDirectory;
  private final File initScript;
  private final File cleanupScript;
  private final File testDataDirectory;
  private final File testScriptDirectory;
  private boolean overwrite = false;
  private MiniHS2 miniHS2;
  private QFileClientBuilder clientBuilder;
  private QFileBuilder fileBuilder;

//  private static QTestUtil.QTestSetup miniZKCluster = null;

  public CoreBeeLineDriver(AbstractCliConfig testCliConfig) {
    super(testCliConfig);
    queryDirectory = new File(testCliConfig.getQueryDirectory());
    logDirectory = new File(testCliConfig.getLogDir());
    resultsDirectory = new File(testCliConfig.getResultsDir());
    testDataDirectory = new File(hiveRootDirectory, "data" + File.separator + "files");
    testScriptDirectory = new File(hiveRootDirectory, "data" + File.separator + "scripts");
    initScript = new File(testScriptDirectory, testCliConfig.getInitScript());
    cleanupScript = new File(testScriptDirectory, testCliConfig.getCleanupScript());
  }

  @Override
  @BeforeClass
  public void beforeClass() throws Exception {
    String testOutputOverwrite = System.getProperty("test.output.overwrite");
    if (testOutputOverwrite != null && "true".equalsIgnoreCase(testOutputOverwrite)) {
      overwrite = true;
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

    clientBuilder = new QFileClientBuilder()
        .setJdbcDriver("org.apache.hive.jdbc.HiveDriver")
        .setJdbcUrl(miniHS2.getJdbcURL())
        .setUsername("user")
        .setPassword("password");

    fileBuilder = new QFileBuilder()
        .setHiveRootDirectory(hiveRootDirectory)
        .setLogDirectory(logDirectory)
        .setQueryDirectory(queryDirectory)
        .setResultsDirectory(resultsDirectory)
        .setScratchDirectoryString(hiveConf.getVar(HiveConf.ConfVars.SCRATCHDIR))
        .setWarehouseDirectoryString(hiveConf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE));

    runInfraScript(initScript, new File(logDirectory, "init.beeline"),
        new File(logDirectory, "init.raw"));
  }

  protected void runInfraScript(File script, File beeLineOutput, File log)
      throws IOException {
    try (QFileBeeLineClient beeLineClient = clientBuilder.getClient(beeLineOutput)) {
      beeLineClient.execute(
          new String[]{
            "set hive.exec.pre.hooks=" + PreExecutePrinter.class.getName() + ";",
            "set test.data.dir=" + testDataDirectory + ";",
            "set test.script.dir=" + testScriptDirectory + ";",
            "!run " + script,
          },
          log);
    }
  }

  @Override
  @AfterClass
  public void shutdown() throws Exception {
    runInfraScript(cleanupScript, new File(logDirectory, "cleanup.beeline"),
        new File(logDirectory, "cleanup.raw"));
    if (miniHS2 != null) {
      miniHS2.stop();
    }
    //    if (miniZKCluster != null) {
    //      miniZKCluster.tearDown();
    //    }
  }

  public void runTest(QFile qFile) throws Exception {
    try (QFileBeeLineClient beeLineClient = clientBuilder.getClient(qFile.getLogFile())) {
      long startTime = System.currentTimeMillis();
      System.err.println(">>> STARTED " + qFile.getName());
      assertTrue("QFile execution failed, see logs for details", beeLineClient.execute(qFile));

      long endTime = System.currentTimeMillis();
      System.err.println(">>> EXECUTED " + qFile.getName() + ":" + (endTime - startTime) / 1000
          + "s");

      qFile.filterOutput();
      long filterEndTime = System.currentTimeMillis();
      System.err.println(">>> FILTERED " + qFile.getName() + ":" + (filterEndTime - endTime) / 1000
          + "s");

      if (!overwrite) {
        if (qFile.compareResults()) {
          System.err.println(">>> PASSED " + qFile.getName());
        } else {
          System.err.println(">>> FAILED " + qFile.getName());
          fail("Failed diff");
        }
      } else {
        qFile.overwriteResults();
        System.err.println(">>> PASSED " + qFile.getName());
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
    QFile qFile = fileBuilder.getQFile(name);
    runTest(qFile);
  }
}
