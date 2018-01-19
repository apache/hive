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

import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Strings;
import org.apache.hadoop.hive.ql.QTestProcessExecResult;
import org.apache.hadoop.hive.ql.QTestUtil;
import org.apache.hadoop.hive.ql.QTestUtil.MiniClusterType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
public class CoreCompareCliDriver extends CliAdapter{

  private static QTestUtil qt;
  public CoreCompareCliDriver(AbstractCliConfig testCliConfig) {
    super(testCliConfig);
  }


  @Override
  @BeforeClass
  public void beforeClass() {

    MiniClusterType miniMR = cliConfig.getClusterType();
    String hiveConfDir = cliConfig.getHiveConfDir();
    String initScript = cliConfig.getInitScript();
    String cleanupScript = cliConfig.getCleanupScript();
    try {
      String hadoopVer = cliConfig.getHadoopVersion();
      qt = new QTestUtil(cliConfig.getResultsDir(), cliConfig.getLogDir(), miniMR,
      hiveConfDir, hadoopVer, initScript, cleanupScript, false);

      // do a one time initialization
      qt.cleanUp();
      qt.createSources();

    } catch (Exception e) {
      System.err.println("Exception: " + e.getMessage());
      e.printStackTrace();
      System.err.flush();
      fail("Unexpected exception in static initialization");
    }
  }

  @Override
  @Before
  public void setUp() {
    try {
      qt.clearTestSideEffects();
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
      qt.shutdown();
    } catch (Exception e) {
      System.err.println("Exception: " + e.getMessage());
      e.printStackTrace();
      System.err.flush();
      fail("Unexpected exception in shutdown");
    }
  }

  private Map<String, List<String>> versionFiles = new HashMap<>();

  static String debugHint = "\nSee ./ql/target/tmp/log/hive.log or ./itests/qtest/target/tmp/log/hive.log, "
     + "or check ./ql/target/surefire-reports or ./itests/qtest/target/surefire-reports/ for specific test cases logs.";

  @Override
  public void runTest(String tname, String fname, String fpath) throws Exception {
    final String queryDirectory = cliConfig.getQueryDirectory();

    long startTime = System.currentTimeMillis();
    try {
      System.err.println("Begin query: " + fname);
      // TODO: versions could also be picked at build time.
      List<String> versionFiles = QTestUtil.getVersionFiles(queryDirectory, tname);
      if (versionFiles.size() < 2) {
        fail("Cannot run " + tname + " with only " + versionFiles.size() + " versions");
      }

      qt.addFile(fpath);
      for (String versionFile : versionFiles) {
        qt.addFile(new File(queryDirectory, versionFile), true);
      }

      if (qt.shouldBeSkipped(fname)) {
        return;
      }

      int ecode = 0;
      List<String> outputs = new ArrayList<>(versionFiles.size());
      for (String versionFile : versionFiles) {
        // 1 for "_" after tname; 3 for ".qv" at the end. Version is in between.
        String versionStr = versionFile.substring(tname.length() + 1, versionFile.length() - 3);
        outputs.add(qt.cliInit(tname + "." + versionStr, false));
        // TODO: will this work?
        ecode = qt.executeClient(versionFile, fname);
        if (ecode != 0) {
          qt.failed(ecode, fname, debugHint);
        }
      }

      QTestProcessExecResult result = qt.checkCompareCliDriverResults(fname, outputs);
      if (result.getReturnCode() != 0) {
        String message = Strings.isNullOrEmpty(result.getCapturedOutput()) ?
            debugHint : "\r\n" + result.getCapturedOutput();
        qt.failedDiff(result.getReturnCode(), fname, message);
      }
    }
    catch (Exception e) {
      qt.failed(e, fname, debugHint);
    }

    long elapsedTime = System.currentTimeMillis() - startTime;
    System.err.println("Done query: " + fname + " elapsedTime=" + elapsedTime/1000 + "s");
  }
}
