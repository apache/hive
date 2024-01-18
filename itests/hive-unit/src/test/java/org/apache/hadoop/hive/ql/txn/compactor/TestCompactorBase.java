/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.common.util.Retry;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class TestCompactorBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestCompactorBase.class);

  private static final AtomicInteger salt = new AtomicInteger(new Random().nextInt());
  private final String TEST_DATA_DIR = HCatUtil.makePathASafeFileName(System.getProperty("java.io.tmpdir") +
      File.separator + TestCompactor.class.getCanonicalName() + "-" + System.currentTimeMillis() + "_" +
      salt.getAndIncrement());
  private final String BASIC_FILE_NAME = TEST_DATA_DIR + "/basic.input.data";
  private final String TEST_WAREHOUSE_DIR = TEST_DATA_DIR + "/warehouse";

  @Rule
  public TemporaryFolder stagingFolder = new TemporaryFolder();

  @Rule
  public Retry retry = new Retry(2);

  protected HiveConf conf;
  IMetaStoreClient msClient;
  protected IDriver driver;

  protected StatsUpdater statsUpdater = new StatsUpdater();

  @Before
  public void setup() throws Exception {

    File f = new File(TEST_WAREHOUSE_DIR);
    if (f.exists()) {
      FileUtil.fullyDelete(f);
    }
    if (!(new File(TEST_WAREHOUSE_DIR).mkdirs())) {
      throw new RuntimeException("Could not create " + TEST_WAREHOUSE_DIR);
    }

    HiveConf hiveConf = new HiveConf(this.getClass());
    hiveConf.setVar(HiveConf.ConfVars.PRE_EXEC_HOOKS, "");
    hiveConf.setVar(HiveConf.ConfVars.POST_EXEC_HOOKS, "");
    hiveConf.setVar(HiveConf.ConfVars.METASTORE_WAREHOUSE, TEST_WAREHOUSE_DIR);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_INPUT_FORMAT, HiveInputFormat.class.getName());
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER, false);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_OPTIMIZE_METADATA_QUERIES, false);
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_INITIATOR_ON, true);
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_CLEANER_ON, true);
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_CLEAN_ABORTS_USING_CLEANER, true);

    TestTxnDbUtil.setConfValues(hiveConf);
    TestTxnDbUtil.cleanDb(hiveConf);
    TestTxnDbUtil.prepDb(hiveConf);

    conf = hiveConf;
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_MM_ALLOW_ORIGINALS, true);
    HiveConf.setTimeVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_TIME_THRESHOLD, 0, TimeUnit.MILLISECONDS);
    msClient = new HiveMetaStoreClient(conf);
    driver = DriverFactory.newDriver(hiveConf);
    SessionState.start(new CliSessionState(hiveConf));


    int LOOP_SIZE = 3;
    String[] input = new String[LOOP_SIZE * LOOP_SIZE];
    int k = 0;
    for (int i = 1; i <= LOOP_SIZE; i++) {
      String si = i + "";
      for (int j = 1; j <= LOOP_SIZE; j++) {
        String sj = "S" + j + "S";
        input[k] = si + "\t" + sj;
        k++;
      }
    }
    createTestDataFile(BASIC_FILE_NAME, input);
  }

  @After
  public void tearDown() {
    conf = null;
    if (msClient != null) {
      msClient.close();
    }
    if (driver != null) {
      driver.close();
    }
  }

  /**
   * convenience method to execute a select stmt and dump results to log file
   */
  static List<String> execSelectAndDumpData(String selectStmt, IDriver driver, String msg)
      throws Exception {
    executeStatementOnDriver(selectStmt, driver);
    ArrayList<String> valuesReadFromHiveDriver = new ArrayList<>();
    driver.getResults(valuesReadFromHiveDriver);
    int rowIdx = 0;
    LOG.debug(msg);
    for (String row : valuesReadFromHiveDriver) {
      LOG.debug(" rowIdx=" + rowIdx++ + ":" + row);
    }
    return valuesReadFromHiveDriver;
  }

  /**
   * Execute Hive CLI statement and ignore any exception thrown.
   *
   * @param cmd arbitrary statement to execute
   */
  static void executeStatementOnDriverSilently(String cmd, IDriver driver) {
    try {
      executeStatementOnDriver(cmd, driver);
    }
    catch (Exception ex) {
      LOG.warn("Error while executing query: " + cmd, ex);
    }
  }

  /**
   * Execute Hive CLI statement
   *
   * @param cmd arbitrary statement to execute
   */
  static void executeStatementOnDriver(String cmd, IDriver driver) throws Exception {
    LOG.debug("Executing: " + cmd);
    driver.run(cmd);
  }

  private void createTestDataFile(String filename, String[] lines) throws IOException {
    FileWriter writer = null;
    try {
      File file = new File(filename);
      file.deleteOnExit();
      writer = new FileWriter(file);
      for (String line : lines) {
        writer.write(line + "\n");
      }
    } finally {
      if (writer != null) {
        writer.close();
      }
    }
  }
}
