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

import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import org.apache.hadoop.hive.cli.control.AbstractCliConfig.MetastoreType;
import org.apache.hadoop.hive.ql.QTestProcessExecResult;
import org.apache.hadoop.hive.ql.QTestUtil;
import org.apache.hadoop.hive.ql.QTestUtil.MiniClusterType;
import org.apache.hadoop.hive.util.ElapsedTimeLoggingWrapper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoreCliDriver extends CliAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(CoreCliDriver.class);
  private static QTestUtil qt;

  public CoreCliDriver(AbstractCliConfig testCliConfig) {
    super(testCliConfig);
  }

  @Override
  @BeforeClass
  public void beforeClass() {
    String message = "Starting " + CoreCliDriver.class.getName() + " run at " + System.currentTimeMillis();
    LOG.info(message);
    System.err.println(message);
    final MiniClusterType miniMR =cliConfig.getClusterType();
    final String hiveConfDir = cliConfig.getHiveConfDir();
    final String initScript = cliConfig.getInitScript();
    final String cleanupScript = cliConfig.getCleanupScript();
    try {
      final String hadoopVer = cliConfig.getHadoopVersion();

      qt = new ElapsedTimeLoggingWrapper<QTestUtil>() {
        @Override
        public QTestUtil invokeInternal() throws Exception {
          return new QTestUtil((cliConfig.getResultsDir()), (cliConfig.getLogDir()), miniMR,
              hiveConfDir, hadoopVer, initScript, cleanupScript, true, cliConfig.getFsType());
        }
      }.invoke("QtestUtil instance created", LOG, true);

      // do a one time initialization
      new ElapsedTimeLoggingWrapper<Void>() {
        @Override
        public Void invokeInternal() throws Exception {
          qt.cleanUp();
          return null;
        }
      }.invoke("Initialization cleanup done.", LOG, true);

      new ElapsedTimeLoggingWrapper<Void>() {
        @Override
        public Void invokeInternal() throws Exception {
          qt.createSources();
          return null;
        }
      }.invoke("Initialization createSources done.", LOG, true);

    } catch (Exception e) {
      System.err.println("Exception: " + e.getMessage());
      e.printStackTrace();
      System.err.flush();
      throw new RuntimeException("Unexpected exception in static initialization",e);
    }
  }

  @Override
  @Before
  public void setUp() {
    try {
      new ElapsedTimeLoggingWrapper<Void>() {
        @Override
        public Void invokeInternal() throws Exception {
          qt.clearTestSideEffects();
          return null;
        }
      }.invoke("PerTestSetup done.", LOG, false);
    } catch (Exception e) {
      System.err.println("Exception: " + e.getMessage());
      e.printStackTrace();
      System.err.flush();
      fail("Unexpected exception in setup");
    }
  }

  @Override
  @After
  public void tearDown() {
    try {
      new ElapsedTimeLoggingWrapper<Void>() {
        @Override
        public Void invokeInternal() throws Exception {
          qt.clearPostTestEffects();
          return null;
        }
      }.invoke("PerTestTearDown done.", LOG, false);
    } catch (Exception e) {
      System.err.println("Exception: " + e.getMessage());
      e.printStackTrace();
      System.err.flush();
      fail("Unexpected exception in tearDown");
    }
  }

  @Override
  @AfterClass
  public void shutdown() throws Exception {
    try {
      new ElapsedTimeLoggingWrapper<Void>() {
        @Override
        public Void invokeInternal() throws Exception {
          qt.shutdown();
          return null;
        }
      }.invoke("Teardown done.", LOG, false);
    } catch (Exception e) {
      System.err.println("Exception: " + e.getMessage());
      e.printStackTrace();
      System.err.flush();
      fail("Unexpected exception in shutdown");
    }
  }

  static String debugHint = "\nSee ./ql/target/tmp/log/hive.log or ./itests/qtest/target/tmp/log/hive.log, "
     + "or check ./ql/target/surefire-reports or ./itests/qtest/target/surefire-reports/ for specific test cases logs.";

  @Override
  public void runTest(String tname, String fname, String fpath) throws Exception {
    Stopwatch sw = Stopwatch.createStarted();
    boolean skipped = false;
    boolean failed = false;
    try {
      LOG.info("Begin query: " + fname);
      System.err.println("Begin query: " + fname);

      qt.addFile(fpath);

      if (qt.shouldBeSkipped(fname)) {
        LOG.info("Test " + fname + " skipped");
        System.err.println("Test " + fname + " skipped");
        skipped = true;
        return;
      }

      qt.cliInit(fname, false);
      int ecode = qt.executeClient(fname);
      if (ecode != 0) {
        failed = true;
        qt.failed(ecode, fname, debugHint);
      }
      QTestProcessExecResult result = qt.checkCliDriverResults(fname);
      if (result.getReturnCode() != 0) {
        failed = true;
        String message = Strings.isNullOrEmpty(result.getCapturedOutput()) ?
            debugHint : "\r\n" + result.getCapturedOutput();
        qt.failedDiff(result.getReturnCode(), fname, message);
      }
    }
    catch (Exception e) {
      failed = true;
      qt.failed(e, fname, debugHint);
    } finally {
      String message = "Done query" + fname + ". succeeded=" + !failed + ", skipped=" + skipped +
          ". ElapsedTime(ms)=" + sw.stop().elapsed(TimeUnit.MILLISECONDS);
      LOG.info(message);
      System.err.println(message);
    }
    assertTrue("Test passed", true);
  }
}
