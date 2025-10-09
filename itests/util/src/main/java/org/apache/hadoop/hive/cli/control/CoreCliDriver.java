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

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.QTestArguments;
import org.apache.hadoop.hive.ql.QTestProcessExecResult;
import org.apache.hadoop.hive.ql.QTestUtil;
import org.apache.hadoop.hive.ql.QTestMiniClusters.MiniClusterType;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.internal.AssumptionViolatedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;

public class CoreCliDriver extends CliAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(CoreCliDriver.class);
  private static QTestUtil qt;

  public CoreCliDriver(AbstractCliConfig testCliConfig) {
    super(testCliConfig);
  }

  @Override
  @BeforeClass
  public void beforeClass() throws Exception {
    String message = "Starting " + CoreCliDriver.class.getName() + " run at " + System.currentTimeMillis();
    LOG.info(message);
    System.err.println(message);

    MiniClusterType miniMR = cliConfig.getClusterType();
    String hiveConfDir = cliConfig.getHiveConfDir();
    String initScript = cliConfig.getInitScript();
    String cleanupScript = cliConfig.getCleanupScript();

    qt = new QTestUtil(QTestArguments.QTestArgumentsBuilder.instance()
            .withOutDir(cliConfig.getResultsDir())
            .withLogDir(cliConfig.getLogDir())
            .withClusterType(miniMR)
            .withConfDir(hiveConfDir)
            .withInitScript(initScript)
            .withCleanupScript(cleanupScript)
            .withLlapIo(true)
            .withFsType(cliConfig.getFsType())
            .withCustomConfigValueMap(this.cliConfig.getCustomConfigValueMap())
            .build());
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
  @AfterClass
  public void shutdown() throws Exception {
    qt.shutdown();
  }

  @Override
  protected QTestUtil getQt() {
    return qt;
  }

  @Override
  public void runTest(String testName, String fname, String fpath) {
    Stopwatch sw = Stopwatch.createStarted();
    boolean skipped = false;
    boolean failed = false;
    try {
      LOG.info("Begin query: " + fname);
      System.err.println("Begin query: " + fname);

      qt.setInputFile(fpath);
      qt.cliInit();

      try {
        qt.executeClient();
      } catch (CommandProcessorException e) {
        failed = true;
        qt.failedQuery(e.getCause(), e.getResponseCode(), fname, QTestUtil.DEBUG_HINT);
      }

      setupAdditionalPartialMasks();
      QTestProcessExecResult result = qt.checkCliDriverResults();
      resetAdditionalPartialMasks();
      if (result.getReturnCode() != 0) {
        failed = true;
        String message = Strings.isNullOrEmpty(result.getCapturedOutput()) ? QTestUtil.DEBUG_HINT
            : "\r\n" + result.getCapturedOutput();
        qt.failedDiff(result.getReturnCode(), fname, message);
      }
    } catch (AssumptionViolatedException e) {
      skipped = true;
      throw e;
    } catch (Exception e) {
      failed = true;
      qt.failedWithException(e, fname, QTestUtil.DEBUG_HINT);
    } finally {
      String message = "Done query " + fname + ". succeeded=" + !failed + ", skipped=" + skipped +
          ". ElapsedTime(ms)=" + sw.stop().elapsed(TimeUnit.MILLISECONDS);
      LOG.info(message);
      System.err.println(message);
    }
    assertTrue("Test passed", true);
  }

  private void setupAdditionalPartialMasks() {
    String patternStr = HiveConf.getVar(qt.getConf(), ConfVars.HIVE_ADDITIONAL_PARTIAL_MASKS_PATTERN);
    String replacementStr = HiveConf.getVar(qt.getConf(), ConfVars.HIVE_ADDITIONAL_PARTIAL_MASKS_REPLACEMENT_TEXT);
    if (patternStr != null  && replacementStr != null && !replacementStr.isEmpty() && !patternStr.isEmpty()) {
      String[] patterns = patternStr.split(",");
      String[] replacements = replacementStr.split(",");
      if (patterns.length != replacements.length) {
        throw new RuntimeException("Count mismatch for additional partial masks and their replacements");
      }
      for (int i = 0; i < patterns.length; i++) {
        qt.getQOutProcessor().addPatternWithMaskComment(patterns[i],
            String.format("### %s ###", replacements[i]));
      }
    }
  }

  private void resetAdditionalPartialMasks() {
    qt.getQOutProcessor().resetPatternwithMaskComments();
  }
}
