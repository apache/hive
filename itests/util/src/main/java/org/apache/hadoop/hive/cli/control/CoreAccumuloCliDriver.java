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

import org.apache.hadoop.hive.accumulo.AccumuloQTestUtil;
import org.apache.hadoop.hive.accumulo.AccumuloTestSetup;
import org.apache.hadoop.hive.ql.QTestProcessExecResult;
import org.apache.hadoop.hive.ql.QTestUtil;
import org.apache.hadoop.hive.ql.QTestMiniClusters.MiniClusterType;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.internal.AssumptionViolatedException;

public class CoreAccumuloCliDriver extends CliAdapter {

  private AccumuloQTestUtil qt;

  public CoreAccumuloCliDriver(AbstractCliConfig cliConfig) {
    super(cliConfig);
  }

  @Override
  @BeforeClass
  public void beforeClass() throws Exception {
    MiniClusterType miniMR = cliConfig.getClusterType();
    String initScript = cliConfig.getInitScript();
    String cleanupScript = cliConfig.getCleanupScript();

    qt = new AccumuloQTestUtil(cliConfig.getResultsDir(), cliConfig.getLogDir(), miniMR, new AccumuloTestSetup(),
        initScript, cleanupScript);
  }

  @Override
  @AfterClass
  public void shutdown() throws Exception {
    qt.shutdown();
  }

  @Override
  @Before
  public void setUp() throws Exception {
    qt.newSession();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    qt.clearPostTestEffects();
    qt.clearTestSideEffects();
  }

  @Override
  protected QTestUtil getQt() {
    return qt;
  }

  @Override
  public void runTest(String tname, String fname, String fpath) {
    long startTime = System.currentTimeMillis();
    try {
      System.err.println("Begin query: " + fname);

      qt.setInputFile(fpath);
      qt.cliInit();

      try {
        qt.executeClient();
      } catch (CommandProcessorException e) {
        qt.failedQuery(e.getCause(), e.getResponseCode(), fname, null);
      }

      QTestProcessExecResult result = qt.checkCliDriverResults();
      if (result.getReturnCode() != 0) {
        qt.failedDiff(result.getReturnCode(), fname, result.getCapturedOutput());
      }
      qt.clearPostTestEffects();

    } catch (AssumptionViolatedException e) {
      throw e;
    } catch (Exception e) {
      qt.failedWithException(e, fname, null);
    }

    long elapsedTime = System.currentTimeMillis() - startTime;
    System.err.println("Done query: " + fname + " elapsedTime=" + elapsedTime/1000 + "s");
    assertTrue("Test passed", true);
  }
}

