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

import org.apache.hadoop.hive.ql.QTestArguments;
import org.apache.hadoop.hive.ql.QTestProcessExecResult;
import org.apache.hadoop.hive.ql.QTestUtil;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.QTestMiniClusters.MiniClusterType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.internal.AssumptionViolatedException;

import com.google.common.base.Strings;

public class CoreNegativeCliDriver extends CliAdapter{

  private QTestUtil qt;

  public CoreNegativeCliDriver(AbstractCliConfig testCliConfig) {
    super(testCliConfig);
  }

  @Override
  public void beforeClass(){
    MiniClusterType miniMR = cliConfig.getClusterType();
    String hiveConfDir = cliConfig.getHiveConfDir();
    String initScript = cliConfig.getInitScript();
    String cleanupScript = cliConfig.getCleanupScript();

    try {
      qt = new QTestUtil(
          QTestArguments.QTestArgumentsBuilder.instance()
            .withOutDir(cliConfig.getResultsDir())
            .withLogDir(cliConfig.getLogDir())
            .withClusterType(miniMR)
            .withConfDir(hiveConfDir)
            .withInitScript(initScript)
            .withCleanupScript(cleanupScript)
            .withLlapIo(false)
            .build());
    } catch (Exception e) {
      System.err.println("Exception: " + e.getMessage());
      e.printStackTrace();
      System.err.flush();
      throw new RuntimeException("Unexpected exception in static initialization", e);
    }
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
  }

  @Override
  protected QTestUtil getQt() {
    return qt;
  }

  @Override
  public void runTest(String tname, String fname, String fpath) throws Exception {
    long startTime = System.currentTimeMillis();
    try {
      System.err.println("Begin query: " + fname);

      qt.setInputFile(fpath);
      qt.cliInit();

      try {
        qt.executeClient();
        qt.failed(fname, QTestUtil.DEBUG_HINT);
      } catch (CommandProcessorException e) {
        // this is the expected outcome
      }

      QTestProcessExecResult result = qt.checkCliDriverResults();
      if (result.getReturnCode() != 0) {
        String message = Strings.isNullOrEmpty(result.getCapturedOutput()) ? QTestUtil.DEBUG_HINT
          : "\r\n" + result.getCapturedOutput();
        qt.failedDiff(result.getReturnCode(), fname, message);
      }
    } catch (AssumptionViolatedException e) {
      throw e;
    } catch (Error error) {
      QTestProcessExecResult qTestProcessExecResult = qt.checkNegativeResults(fname, error);
      if (qTestProcessExecResult.getReturnCode() != 0) {
        String message = Strings.isNullOrEmpty(qTestProcessExecResult.getCapturedOutput())
          ? QTestUtil.DEBUG_HINT : "\r\n" + qTestProcessExecResult.getCapturedOutput();
        qt.failedDiff(qTestProcessExecResult.getReturnCode(), fname, message);
      }
    } catch (Exception e) {
      qt.failedWithException(e, fname, QTestUtil.DEBUG_HINT);
    }

    long elapsedTime = System.currentTimeMillis() - startTime;
    System.err.println("Done query: " + fname + " elapsedTime=" + elapsedTime/1000 + "s");
    assertTrue("Test passed", true);
  }
}
