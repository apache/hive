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
package org.apache.hadoop.hive.conf;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * TestHiveLogging
 *
 * Test cases for HiveLogging, which is initialized in HiveConf.
 * Loads configuration files located in common/src/test/resources.
 */
public class TestHiveLogging extends TestCase {
  private Runtime runTime;
  private Process process;

  public TestHiveLogging() {
    super();
    runTime = Runtime.getRuntime();
    process = null;
  }

  private void configLog(String hiveLog4jTest, String hiveExecLog4jTest) {
    System.setProperty(ConfVars.HIVE_LOG4J_FILE.varname,
      System.getProperty("test.build.resources") + "/" + hiveLog4jTest);
    System.setProperty(ConfVars.HIVE_EXEC_LOG4J_FILE.varname,
      System.getProperty("test.build.resources") + "/" + hiveExecLog4jTest);

    String expectedLog4jPath = System.getProperty("test.build.resources")
      + "/" + hiveLog4jTest;
    String expectedLog4jExecPath = System.getProperty("test.build.resources")
      + "/" + hiveExecLog4jTest;

    try {
      LogUtils.initHiveLog4j();
    } catch (LogInitializationException e) {
    }

    HiveConf conf = new HiveConf();
    assertEquals(expectedLog4jPath, conf.getVar(ConfVars.HIVE_LOG4J_FILE));
    assertEquals(expectedLog4jExecPath, conf.getVar(ConfVars.HIVE_EXEC_LOG4J_FILE));
  }

  private void runCmd(String cmd) {
    try {
      process = runTime.exec(cmd);
    } catch (IOException e) {
      e.printStackTrace();
    }
    try {
      process.waitFor();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void getCmdOutput(String logFile) {
    boolean logCreated = false;
    BufferedReader buf = new BufferedReader(
      new InputStreamReader(process.getInputStream()));
    String line = "";
    try {
      while((line = buf.readLine()) != null) {
        if (line.equals(logFile))
          logCreated = true;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    assertEquals(true, logCreated);
  }

  private void RunTest(String cleanCmd, String findCmd, String logFile,
    String hiveLog4jProperty, String hiveExecLog4jProperty) throws Exception {
    // clean test space
    runCmd(cleanCmd);

    // config log4j with customized files
    // check whether HiveConf initialize log4j correctly
    configLog(hiveLog4jProperty, hiveExecLog4jProperty);

    // check whether log file is created on test running
    runCmd(findCmd);
    getCmdOutput(logFile);

    // clean test space
    runCmd(cleanCmd);
  }

  public void testHiveLogging() throws Exception {
    // customized log4j config log file to be: /tmp/hiveLog4jTest.log
    String customLogPath = "/tmp/";
    String customLogName = "hiveLog4jTest.log";
    String customLogFile = customLogPath + customLogName;
    String customCleanCmd = "rm -rf " + customLogFile;
    String customFindCmd = "find /tmp -name " + customLogName;
    RunTest(customCleanCmd, customFindCmd, customLogFile,
      "hive-log4j-test.properties", "hive-exec-log4j-test.properties");
  }
}
