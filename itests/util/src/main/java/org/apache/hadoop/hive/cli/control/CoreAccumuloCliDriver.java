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

import java.io.File;

import org.apache.hadoop.hive.accumulo.AccumuloQTestUtil;
import org.apache.hadoop.hive.accumulo.AccumuloTestSetup;
import org.apache.hadoop.hive.ql.QTestProcessExecResult;
import org.apache.hadoop.hive.ql.QTestUtil.MiniClusterType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class CoreAccumuloCliDriver extends CliAdapter {

  private AccumuloQTestUtil qt;
  private static AccumuloTestSetup setup;

  public CoreAccumuloCliDriver(AbstractCliConfig cliConfig) {
    super(cliConfig);
  }

  @Override
  @BeforeClass
  public void beforeClass() {
    setup = new AccumuloTestSetup();

    MiniClusterType miniMR = cliConfig.getClusterType();
    String initScript = cliConfig.getInitScript();
    String cleanupScript = cliConfig.getCleanupScript();

    try {
      qt = new AccumuloQTestUtil(cliConfig.getResultsDir(), cliConfig.getLogDir(), miniMR,
          setup, initScript, cleanupScript);

      // do a one time initialization
      qt.newSession();
      qt.cleanUp();
      qt.createSources();
    } catch (Exception e) {
      throw new RuntimeException("Unexpected exception in setUp",e);
    }
  }

  @Override
  @AfterClass
  public void shutdown() throws Exception {
    setup.tearDown();

    try {
      qt.shutdown();
    }
    catch (Exception e) {
      throw new RuntimeException("Unexpected exception in tearDown",e);
    }
  }

  @Override
  @Before
  public void setUp() {
    try {
      qt.newSession();
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
      qt.clearPostTestEffects();
      qt.clearTestSideEffects();
    } catch (Exception e) {
      System.err.println("Exception: " + e.getMessage());
      e.printStackTrace();
      System.err.flush();
      fail("Unexpected exception in tearDown");
    }
  }
  @Override
  public void runTest(String tname, String fname, String fpath) throws Exception {
    long startTime = System.currentTimeMillis();
    try {
      System.err.println("Begin query: " + fname);

      qt.addFile(fpath);

      qt.cliInit(new File(fpath));
      int ecode = qt.executeClient(fname);
      if (ecode != 0) {
        qt.failed(ecode, fname, null);
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
