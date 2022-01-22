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

import org.apache.hadoop.hive.ql.QTestArguments;
import org.apache.hadoop.hive.ql.QTestProcessExecResult;
import org.apache.hadoop.hive.ql.QTestUtil;
import org.apache.hadoop.hive.ql.QTestMiniClusters.MiniClusterType;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.internal.AssumptionViolatedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

/**
 * CliDriver for integrating performance regression tests as part of the Hive Unit tests.
 *
 * Typically it is meant to be combined with configurations providing an initialised metastore and read-only queries.
 */
public class CorePerfCliDriver extends CliAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(CorePerfCliDriver.class);
  private static QTestUtil qt;

  public CorePerfCliDriver(AbstractCliConfig testCliConfig) {
    super(testCliConfig);
  }

  @Override
  public void beforeClass() throws Exception {
    MiniClusterType miniMR = cliConfig.getClusterType();
    String hiveConfDir = cliConfig.getHiveConfDir();
    String initScript = cliConfig.getInitScript();
    String cleanupScript = cliConfig.getCleanupScript();

    qt = new QTestUtil(QTestArguments.QTestArgumentsBuilder.instance().withOutDir(cliConfig.getResultsDir())
        .withLogDir(cliConfig.getLogDir()).withClusterType(miniMR).withConfDir(hiveConfDir).withInitScript(initScript)
        .withCleanupScript(cleanupScript).withLlapIo(false).build());
  }

  @Override
  public void shutdown() throws Exception {
    qt.shutdown();
  }

  @Override
  public void setUp() throws Exception {
    qt.newSession();
  }

  @Override
  public void tearDown() throws Exception {
    qt.clearPostTestEffects();
  }

  @Override
  protected QTestUtil getQt() {
    return qt;
  }

  @Override
  public void runTest(String name, String fname, String fpath) {
    long startTime = System.currentTimeMillis();
    try {
      LOG.info("Begin query: " + fname);

      qt.setInputFile(fpath);
      qt.cliInit();

      try {
        qt.executeClient();
      } catch (CommandProcessorException e) {
        qt.failedQuery(e.getCause(), e.getResponseCode(), fname, QTestUtil.DEBUG_HINT);
      }

      QTestProcessExecResult result = qt.checkCliDriverResults();
      if (result.getReturnCode() != 0) {
        String message = Strings.isNullOrEmpty(result.getCapturedOutput()) ? QTestUtil.DEBUG_HINT :
            "\r\n" + result.getCapturedOutput();
        qt.failedDiff(result.getReturnCode(), fname, message);
      }
    } catch (AssumptionViolatedException e) {
      throw e;
    } catch (Exception e) {
      qt.failedWithException(e, fname, QTestUtil.DEBUG_HINT);
    }

    long elapsedTime = System.currentTimeMillis() - startTime;
    LOG.info("Done query: " + fname + " elapsedTime=" + elapsedTime / 1000 + "s");
  }

}
