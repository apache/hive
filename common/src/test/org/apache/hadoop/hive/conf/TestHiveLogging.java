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
package org.apache.hadoop.hive.conf;

import java.io.File;

import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.common.util.HiveTestUtils;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * TestHiveLogging
 *
 * Test cases for HiveLogging, which is initialized in HiveConf.
 * Loads configuration files located in common/src/test/resources.
 */
public class TestHiveLogging {
  public TestHiveLogging() {
    super();
  }

  private void configLog(String hiveLog4jTest, String hiveExecLog4jTest)
  throws Exception {
    String expectedLog4jTestPath = HiveTestUtils.getFileFromClasspath(hiveLog4jTest);
    String expectedLog4jExecPath = HiveTestUtils.getFileFromClasspath(hiveExecLog4jTest);
    System.setProperty(ConfVars.HIVE_LOG4J_FILE.varname, expectedLog4jTestPath);
    System.setProperty(ConfVars.HIVE_EXEC_LOG4J_FILE.varname, expectedLog4jExecPath);

    LogUtils.initHiveLog4j();

    HiveConf conf = new HiveConf();
    assertEquals(expectedLog4jTestPath, conf.getVar(ConfVars.HIVE_LOG4J_FILE));
    assertEquals(expectedLog4jExecPath, conf.getVar(ConfVars.HIVE_EXEC_LOG4J_FILE));
  }

  public void cleanLog(File logFile) {
    if (logFile.exists()) {
      logFile.delete();
    }
    File logFileDir = logFile.getParentFile();
    if (logFileDir.exists()) {
      logFileDir.delete();
    }
  }

  private void RunTest(File logFile,
    String hiveLog4jProperty, String hiveExecLog4jProperty) throws Exception {
    // clean test space
    cleanLog(logFile);
    assertFalse(logFile + " should not exist", logFile.exists());

    // config log4j with customized files
    // check whether HiveConf initialize log4j correctly
    configLog(hiveLog4jProperty, hiveExecLog4jProperty);

    // check whether log file is created on test running
    assertTrue(logFile + " should exist", logFile.exists());
  }

  @Test
  public void testHiveLogging() throws Exception {
    // customized log4j config log file to be: /${test.tmp.dir}/TestHiveLogging/hiveLog4jTest.log
    File customLogPath = new File(new File(System.getProperty("test.tmp.dir")),
        System.getProperty("user.name") + "-TestHiveLogging/");
    String customLogName = "hiveLog4j2Test.log";
    File customLogFile = new File(customLogPath, customLogName);
    RunTest(customLogFile,
      "hive-log4j2-test.properties", "hive-exec-log4j2-test.properties");
  }
}
