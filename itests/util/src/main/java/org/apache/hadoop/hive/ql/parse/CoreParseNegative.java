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
package org.apache.hadoop.hive.ql.parse;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.hive.cli.control.AbstractCliConfig;
import org.apache.hadoop.hive.cli.control.CliAdapter;
import org.apache.hadoop.hive.cli.control.CliConfigs;
import org.apache.hadoop.hive.ql.QTestArguments;
import org.apache.hadoop.hive.ql.QTestProcessExecResult;
import org.apache.hadoop.hive.ql.QTestUtil;
import org.apache.hadoop.hive.ql.QTestMiniClusters.MiniClusterType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.google.common.base.Strings;

public class CoreParseNegative extends CliAdapter{

  private static QTestUtil qt;

  private static CliConfigs.ParseNegativeConfig cliConfig = new CliConfigs.ParseNegativeConfig();

  public CoreParseNegative(AbstractCliConfig testCliConfig) {
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
          .withLlapIo(false)
          .build());
  }

  @Override
  public void setUp() {
  }

  @Override
  @After
  public void tearDown() {
  }

  @Override
  @AfterClass
  public void shutdown() throws Exception {
    qt.clearPostTestEffects();
    qt.shutdown();
  }

  protected boolean shouldRunCreateScriptBeforeEveryTest() {
    return true;
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

      ASTNode tree = qt.parseQuery();
      qt.analyzeAST(tree);
      fail("Unexpected success for query: " + fname + QTestUtil.DEBUG_HINT);
    } catch (ParseException pe) {
      QTestProcessExecResult result = qt.checkNegativeResults(fname, pe);
      if (result.getReturnCode() != 0) {
        qt.failedQuery(null, result.getReturnCode(), fname, result.getCapturedOutput() + "\r\n" + QTestUtil.DEBUG_HINT);
      }
    } catch (SemanticException se) {
      QTestProcessExecResult result = qt.checkNegativeResults(fname, se);
      if (result.getReturnCode() != 0) {
        String message = Strings.isNullOrEmpty(result.getCapturedOutput()) ? QTestUtil.DEBUG_HINT
          : "\r\n" + result.getCapturedOutput();
        qt.failedDiff(result.getReturnCode(), fname, message);
      }
    } catch (Exception e) {
      qt.failedWithException(e, fname, QTestUtil.DEBUG_HINT);
    }

    long elapsedTime = System.currentTimeMillis() - startTime;
    System.err.println("Done query: " + fname + " elapsedTime=" + elapsedTime/1000 + "s");
    assertTrue("Test passed", true);
  }

}
