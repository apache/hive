/**
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

import java.io.Serializable;
import java.util.List;
import org.apache.hadoop.hive.cli.control.AbstractCliConfig;
import org.apache.hadoop.hive.cli.control.CliAdapter;
import org.apache.hadoop.hive.cli.control.CliConfigs;
import org.apache.hadoop.hive.ql.QTestUtil;
import org.apache.hadoop.hive.ql.QTestUtil.MiniClusterType;
import org.apache.hadoop.hive.ql.exec.Task;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class CoreParseNegative extends CliAdapter{

  private static QTestUtil qt;

  static CliConfigs.ParseNegativeConfig cliConfig = new CliConfigs.ParseNegativeConfig();
  static boolean firstRun;
  public CoreParseNegative(AbstractCliConfig testCliConfig) {
    super(testCliConfig);
  }

  @Override
  @BeforeClass
  public void beforeClass() {
    MiniClusterType miniMR = cliConfig.getClusterType();
    String initScript = cliConfig.getInitScript();
    String cleanupScript = cliConfig.getCleanupScript();
    firstRun = true;
    try {
      String hadoopVer = cliConfig.getHadoopVersion();
      qt = new QTestUtil((cliConfig.getResultsDir()), (cliConfig.getLogDir()), miniMR, null,
          hadoopVer,
       initScript, cleanupScript, false, false);
    } catch (Exception e) {
      System.err.println("Exception: " + e.getMessage());
      e.printStackTrace();
      System.err.flush();
      throw new RuntimeException("Unexpected exception in static initialization",e);
    }
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
    String reason = "clear post test effects";
    try {
      qt.clearPostTestEffects();
      reason = "shutdown";
      qt.shutdown();
    } catch (Exception e) {
      System.err.println("Exception: " + e.getMessage());
      e.printStackTrace();
      System.err.flush();
      throw new RuntimeException("Unexpected exception in " + reason,e);
    }
  }

  static String debugHint = "\nSee ./ql/target/tmp/log/hive.log or ./itests/qtest/target/tmp/log/hive.log, "
     + "or check ./ql/target/surefire-reports or ./itests/qtest/target/surefire-reports/ for specific test cases logs.";


  @Override
  public void runTest(String tname, String fname, String fpath) throws Exception {
    long startTime = System.currentTimeMillis();
    try {
      System.err.println("Begin query: " + fname);

      qt.addFile(fpath);
      if (firstRun) {
        qt.init(fname);
        firstRun = false;
      }
      ASTNode tree = qt.parseQuery(fname);
      List<Task<? extends Serializable>> tasks = qt.analyzeAST(tree);
      fail("Unexpected success for query: " + fname + debugHint);
    }
    catch (ParseException pe) {
      int ecode = qt.checkNegativeResults(fname, pe);
      if (ecode != 0) {
        qt.failed(ecode, fname, debugHint);
      }
    }
    catch (SemanticException se) {
      int ecode = qt.checkNegativeResults(fname, se);
      if (ecode != 0) {
        qt.failedDiff(ecode, fname, debugHint);
      }
    }
    catch (Throwable e) {
      qt.failed(e, fname, debugHint);
    }

    long elapsedTime = System.currentTimeMillis() - startTime;
    System.err.println("Done query: " + fname + " elapsedTime=" + elapsedTime/1000 + "s");
    assertTrue("Test passed", true);
  }

}
