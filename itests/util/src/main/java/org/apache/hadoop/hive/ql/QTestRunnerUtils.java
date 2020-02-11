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

package org.apache.hadoop.hive.ql;

import java.io.File;

import org.apache.hadoop.hive.ql.QTestMiniClusters.MiniClusterType;
import org.apache.logging.log4j.util.Strings;

public class QTestRunnerUtils {
  public static final String DEFAULT_INIT_SCRIPT = "q_test_init.sql";
  public static final String DEFAULT_CLEANUP_SCRIPT = "q_test_cleanup.sql";

  /**
   * QTRunner: Runnable class for running a single query file.
   **/
  public static class QTRunner implements Runnable {
    private final QTestUtil qt;
    private final File file;

    public QTRunner(QTestUtil qt, File file) {
      this.qt = qt;
      this.file = file;
    }

    @Override
    public void run() {
      try {
        qt.startSessionState(false);
        // assumption is that environment has already been cleaned once globally
        // hence each thread does not call cleanUp() and createSources() again
        qt.cliInit(file);
        qt.executeClient(file.getName());
      } catch (Throwable e) {
        System.err
            .println("Query file " + file.getName() + " failed with exception " + e.getMessage());
        e.printStackTrace();
        outputTestFailureHelpMessage();
      }
    }
  }

  /**
   * Setup to execute a set of query files. Uses QTestUtil to do so.
   *
   * @param qfiles array of input query files containing arbitrary number of hive queries
   * @param resDir output directory
   * @param logDir log directory
   * @return one QTestUtil for each query file
   */
  public static QTestUtil[] queryListRunnerSetup(File[] qfiles, String resDir, String logDir,
      String initScript, String cleanupScript) throws Exception {
    QTestUtil[] qt = new QTestUtil[qfiles.length];
    for (int i = 0; i < qfiles.length; i++) {

      qt[i] = new QTestUtil(QTestArguments.QTestArgumentsBuilder.instance().withOutDir(resDir)
          .withLogDir(logDir).withClusterType(MiniClusterType.NONE).withConfDir(null)
          .withInitScript(initScript == null ? DEFAULT_INIT_SCRIPT : initScript)
          .withCleanupScript(cleanupScript == null ? DEFAULT_CLEANUP_SCRIPT : cleanupScript)
          .withLlapIo(false).build());

      qt[i].addFile(qfiles[i], false);
      qt[i].clearTestSideEffects();
    }

    return qt;
  }

  /**
   * Executes a set of query files in sequence.
   *
   * @param qfiles array of input query files containing arbitrary number of hive queries
   * @param qt array of QTestUtils, one per qfile
   * @return true if all queries passed, false otw
   */
  public static boolean queryListRunnerSingleThreaded(File[] qfiles, QTestUtil[] qt)
      throws Exception {
    boolean failed = false;
    qt[0].cleanUp();
    qt[0].createSources();
    for (int i = 0; i < qfiles.length && !failed; i++) {
      qt[i].clearTestSideEffects();
      qt[i].cliInit(qfiles[i]);
      qt[i].executeClient(qfiles[i].getName());
      QTestProcessExecResult result = qt[i].checkCliDriverResults(qfiles[i].getName());
      if (result.getReturnCode() != 0) {
        failed = true;
        StringBuilder builder = new StringBuilder();
        builder.append("Test ").append(qfiles[i].getName())
            .append(" results check failed with error code ").append(result.getReturnCode());
        if (Strings.isNotEmpty(result.getCapturedOutput())) {
          builder.append(" and diff value ").append(result.getCapturedOutput());
        }
        System.err.println(builder.toString());
        outputTestFailureHelpMessage();
      }
      qt[i].clearPostTestEffects();
    }
    return (!failed);
  }

  /**
   * Executes a set of query files parallel.
   * <p>
   * Each query file is run in a separate thread. The caller has to arrange that different query
   * files do not collide (in terms of destination tables)
   *
   * @param qfiles array of input query files containing arbitrary number of hive queries
   * @param qt array of QTestUtils, one per qfile
   * @return true if all queries passed, false otw
   */
  public static boolean queryListRunnerMultiThreaded(File[] qfiles, QTestUtil[] qt)
      throws Exception {
    boolean failed = false;

    // in multithreaded mode - do cleanup/initialization just once

    qt[0].cleanUp();
    qt[0].createSources();
    qt[0].clearTestSideEffects();

    QTRunner[] qtRunners = new QTRunner[qfiles.length];
    Thread[] qtThread = new Thread[qfiles.length];

    for (int i = 0; i < qfiles.length; i++) {
      qtRunners[i] = new QTRunner(qt[i], qfiles[i]);
      qtThread[i] = new Thread(qtRunners[i]);
    }

    for (int i = 0; i < qfiles.length; i++) {
      qtThread[i].start();
    }

    for (int i = 0; i < qfiles.length; i++) {
      qtThread[i].join();
      QTestProcessExecResult result = qt[i].checkCliDriverResults(qfiles[i].getName());
      if (result.getReturnCode() != 0) {
        failed = true;
        StringBuilder builder = new StringBuilder();
        builder.append("Test ").append(qfiles[i].getName())
            .append(" results check failed with error code ").append(result.getReturnCode());
        if (Strings.isNotEmpty(result.getCapturedOutput())) {
          builder.append(" and diff value ").append(result.getCapturedOutput());
        }
        System.err.println(builder.toString());
        outputTestFailureHelpMessage();
      }
    }
    return (!failed);
  }

  public static void outputTestFailureHelpMessage() {
    System.err.println(QTestUtil.DEBUG_HINT);
    System.err.flush();
  }
}
