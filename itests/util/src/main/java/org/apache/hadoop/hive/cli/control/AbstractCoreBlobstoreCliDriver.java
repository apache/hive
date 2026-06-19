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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveVariableSource;
import org.apache.hadoop.hive.conf.VariableSubstitution;
import org.apache.hadoop.hive.ql.QTestArguments;
import org.apache.hadoop.hive.ql.QTestProcessExecResult;
import org.apache.hadoop.hive.ql.QTestUtil;
import org.apache.hadoop.hive.ql.QTestMiniClusters.MiniClusterType;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hive.testutils.HiveTestEnvSetup;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.google.common.base.Strings;

public abstract class AbstractCoreBlobstoreCliDriver extends CliAdapter {

  protected static QTestUtil qt;
  private static final String HCONF_TEST_BLOBSTORE_PATH = "test.blobstore.path";
  private static final String HCONF_TEST_BLOBSTORE_PATH_UNIQUE = HCONF_TEST_BLOBSTORE_PATH + ".unique";
  private static String testBlobstorePathUnique;

  public AbstractCoreBlobstoreCliDriver(AbstractCliConfig testCliConfig) {
    super(testCliConfig);
  }

  @Override
  @BeforeClass
  public void beforeClass() throws Exception {
    MiniClusterType miniMR = cliConfig.getClusterType();
    String hiveConfDir = cliConfig.getHiveConfDir();
    String initScript = cliConfig.getInitScript();
    String cleanupScript = cliConfig.getCleanupScript();

    qt = new QTestUtil(
        QTestArguments.QTestArgumentsBuilder.instance()
          .withOutDir(cliConfig.getResultsDir())
          .withLogDir(cliConfig.getLogDir())
          .withClusterType(miniMR)
          .withConfDir(hiveConfDir)
          .withInitScript(initScript)
          .withCleanupScript(cleanupScript)
          .withLlapIo(true)
          .build());

    if (Strings.isNullOrEmpty(qt.getConf().get(HCONF_TEST_BLOBSTORE_PATH))) {
      fail(String.format("%s must be set. Try setting in blobstore-conf.xml", HCONF_TEST_BLOBSTORE_PATH));
    }

    // do a one time initialization
    setupUniqueTestPath();
  }

  @Override
  @Before
  public void setUp() throws Exception {
    qt.newSession();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    qt.clearTestSideEffects();
    qt.clearPostTestEffects();
  }

  @Override
  @AfterClass
  public void shutdown() throws Exception {
    qt.shutdown();
    if (System.getenv(QTestUtil.QTEST_LEAVE_FILES) == null) {
      qt.executeAdHocCommand("dfs -rmdir " + testBlobstorePathUnique);
    }
  }

  @Override
  protected QTestUtil getQt() {
    return qt;
  }

  private static String debugHint = "\nSee ./itests/hive-blobstore/target/tmp/log/hive.log, "
      + "or check ./itests/hive-blobstore/target/surefire-reports/ for specific test cases logs.";

  protected void runTestHelper(String tname, String fname, String fpath, boolean expectSuccess) {
    long startTime = System.currentTimeMillis();
    qt.getConf().set(HCONF_TEST_BLOBSTORE_PATH_UNIQUE, testBlobstorePathUnique);
    try {
      System.err.println("Begin query: " + fname);

      qt.setInputFile(fpath);
      qt.cliInit();

      try {
        qt.executeClient();
        if (!expectSuccess) {
          qt.failedQuery(null, 0, fname, debugHint);
        }
      } catch (CommandProcessorException e) {
        if (expectSuccess) {
          qt.failedQuery(e.getCause(), e.getResponseCode(), fname, debugHint);
        }
      }

      QTestProcessExecResult result = qt.checkCliDriverResults();
      if (result.getReturnCode() != 0) {
        String message = Strings.isNullOrEmpty(result.getCapturedOutput()) ?
            debugHint : "\r\n" + result.getCapturedOutput();
        qt.failedDiff(result.getReturnCode(), fname, message);
      }
    }
    catch (Exception e) {
      qt.failedWithException(e, fname, debugHint);
    }

    long elapsedTime = System.currentTimeMillis() - startTime;
    System.err.println("Done query: " + fname + " elapsedTime=" + elapsedTime/1000 + "s");
    assertTrue("Test passed", true);
  }

  /**
   * Generates a unique test path for this particular CliDriver in the following form:
   *   ${test.blobstore.path}/CoreBlobstore[Negative]CliDriver/20160101.053046.332-{random number 000-999}
   * 20160101.053046.332 represents the current datetime:
   *   {year}{month}{day}.{hour}{minute}{second}.{millisecond}
   * Random integer 000-999 included to avoid collisions when two test runs are started at the same millisecond with
   *  the same ${test.blobstore.path} (possible if test runs are controlled by an automated system)
   */
  private void setupUniqueTestPath() {
    String testBlobstorePath = new VariableSubstitution(new HiveVariableSource() {
      @Override
      public Map<String, String> getHiveVariable() {
        return null;
      }
    }).substitute(new HiveConf(), qt.getConf().get(HCONF_TEST_BLOBSTORE_PATH));

    testBlobstorePath = HiveTestEnvSetup.ensurePathEndsInSlash(testBlobstorePath);
    testBlobstorePath += HiveTestEnvSetup.ensurePathEndsInSlash(this.getClass().getSimpleName()); // name of child class
    String uid = new SimpleDateFormat("yyyyMMdd.HHmmss.SSS").format(Calendar.getInstance().getTime())
        + "-" + String.format("%03d", (int)(Math.random() * 999));
    testBlobstorePathUnique = testBlobstorePath + uid;

    qt.getQOutProcessor().addPatternWithMaskComment(testBlobstorePathUnique,
        String.format("### %s ###", HCONF_TEST_BLOBSTORE_PATH));
  }
}
