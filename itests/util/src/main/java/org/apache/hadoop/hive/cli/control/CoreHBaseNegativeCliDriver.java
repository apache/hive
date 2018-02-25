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

import org.apache.hadoop.hive.hbase.HBaseQTestUtil;
import org.apache.hadoop.hive.hbase.HBaseTestSetup;
import org.apache.hadoop.hive.ql.QTestProcessExecResult;
import org.apache.hadoop.hive.ql.QTestUtil.MiniClusterType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;

public class CoreHBaseNegativeCliDriver extends CliAdapter {

  private HBaseQTestUtil qt;
  private static HBaseTestSetup setup = new HBaseTestSetup();

  public CoreHBaseNegativeCliDriver(AbstractCliConfig testCliConfig) {
    super(testCliConfig);
  }

  @Override
  public void beforeClass() throws Exception {
  }

  // hmm..this looks a bit wierd...setup boots qtestutil...this part used to be in beforeclass
  @Override
  @Before
  public void setUp() {

    MiniClusterType miniMR = cliConfig.getClusterType();
    String initScript = cliConfig.getInitScript();
    String cleanupScript = cliConfig.getCleanupScript();

    try {
      qt = new HBaseQTestUtil(cliConfig.getResultsDir(), cliConfig.getLogDir(), miniMR,
      setup, initScript, cleanupScript);
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
      qt.shutdown();
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
    // closeHBaseConnections
    setup.tearDown();
  }

  @Override
  public void runTest(String tname, String fname, String fpath) throws Exception {
    long startTime = System.currentTimeMillis();
    try {
      System.err.println("Begin query: " + fname);

      qt.addFile(fpath);

      if (qt.shouldBeSkipped(fname)) {
        System.err.println("Test " + fname + " skipped");
        return;
      }

      qt.cliInit(fname);
      qt.clearTestSideEffects();
      int ecode = qt.executeClient(fname);
      if (ecode == 0) {
        qt.failed(fname, null);
      }

      QTestProcessExecResult result = qt.checkCliDriverResults(fname);
      if (result.getReturnCode() != 0) {
        qt.failedDiff(result.getReturnCode(), fname, result.getCapturedOutput());
      }
      qt.clearPostTestEffects();

    } catch (Exception e) {
      qt.failed(e, fname, null);
    }

    long elapsedTime = System.currentTimeMillis() - startTime;
    System.err.println("Done query: " + fname + " elapsedTime=" + elapsedTime/1000 + "s");
    assertTrue("Test passed", true);
  }


}

