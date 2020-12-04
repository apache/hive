/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.hive.beeline;

import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHplSqlViaBeeLine {
  private static MiniHS2 miniHS2;
  private static final String userName = System.getProperty("user.name");

  /**
   * Start up a local Hive Server 2 for these tests
   */
  @BeforeClass
  public static void preTests() throws Exception {
    HiveConf hiveConf = new HiveConf();
    hiveConf.setVar(HiveConf.ConfVars.HIVE_LOCK_MANAGER,
            "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_DEFAULT_FETCH_SIZE, 10);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVEOPTIMIZEMETADATAQUERIES, false);
    hiveConf.set(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LEVEL.varname, "verbose");
    miniHS2 = new MiniHS2(hiveConf, MiniHS2.MiniClusterType.TEZ);
    Map<String, String> confOverlay = new HashMap<>();
    miniHS2.start(confOverlay);
  }

  @AfterClass
  public static void postTests() {
    if (miniHS2.isStarted()) {
      miniHS2.stop();
    }
  }

  @Test
  public void testHplSqlMode() throws Throwable {
    String SCRIPT_TEXT =
      "DROP TABLE IF EXISTS numbers;\n" +
      "CREATE TABLE numbers (n INT);\n" +
      "FOR i IN 1..10\n" +
      "LOOP\n" +
      "  EXEC 'INSERT INTO numbers values(' || (i * 10) || ')';\n" +
      "END LOOP;\n" +
      "DECLARE\n" +
      "  total INT = 0;\n" +
      "BEGIN\n" +
      "  FOR each IN (SELECT n FROM numbers)\n" +
      "  LOOP\n" +
      "    total = total + each.n;\n" +
      "  END LOOP;\n" +
      "  DROP TABLE IF EXISTS result;\n" +
      "  CREATE TABLE result (total INT);\n" +
      "  EXEC 'INSERT INTO result VALUES (' || total || ')';\n" +
      "  SELECT * FROM result;\n" +
      "END;\n" +
      "/\n";
    testScriptFile(SCRIPT_TEXT, args(), "550");
  }

  @Test
  public void testHplSqlProcedure() throws Throwable {
    String SCRIPT_TEXT =
      "DROP TABLE IF EXISTS result;\n" +
      "CREATE TABLE result (s string);\n" +
      "CREATE PROCEDURE p1(s STRING)\n" +
      "BEGIN\n" +
        "INSERT INTO result VALUES('Hello world');\n" +
      "END;\n" +
      "p1();\n" +
      "SELECT * FROM result;\n" +
      "/\n";
    testScriptFile(SCRIPT_TEXT, args(), "Hello world");
  }

  @Test
  public void testHplSqlMultipleStatementsWithDiv() throws Throwable {
    String SCRIPT_TEXT =
      "DROP TABLE IF EXISTS result;\n" +
      "CREATE TABLE result (n int); /\n" +
      "DECLARE c INT = 1000 / 2;\n" +
      "EXEC 'INSERT INTO result VALUES(' || c || ')';\n" +
      "SELECT * from result; /\n";
    testScriptFile(SCRIPT_TEXT, args(), "500");
  }

  @Test
  public void testCursor() throws Throwable {
    String SCRIPT_TEXT =
            "DROP TABLE IF EXISTS numbers;\n" +
            "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE numbers (n int);\n" +
            "CREATE TABLE result (n int);\n" +
            "FOR i IN 1000..1043 LOOP INSERT INTO numbers values(i) END LOOP;\n" +
            "CREATE PROCEDURE pc(cur OUT SYS_REFCURSOR)\n" +
            "BEGIN\n" +
            "  OPEN cur FOR SELECT n FROM NUMBERS;\n" +
            "END;\n" +
            "CREATE PROCEDURE uc()\n" +
            "BEGIN\n" +
            " DECLARE curs SYS_REFCURSOR;\n" +
            " DECLARE n INT = 0;\n" +
            " DECLARE sum INT = 0;\n" +
            " CALL pc(curs);\n" +
            " FETCH curs INTO n;\n" +
            " WHILE (SQLCODE = 0) DO\n" +
            "    sum = sum + n;\n" +
            "    FETCH curs INTO n;\n" +
            " END WHILE;\n" +
            " CLOSE curs;\n" +
            " INSERT INTO result VALUES(sum);\n" +
            "END;\n" +
            "uc();\n" +
            "SELECT * FROM result;\n" +
            "/\n";
    testScriptFile(SCRIPT_TEXT, args(), "44946");
  }

  @Test
  public void testDbChange() throws Throwable {
    String SCRIPT_TEXT =
        "DROP TABLE IF EXISTS result;\n" +
        "CREATE TABLE result (n int);\n" +
        "create database test_db1;\n" +
        "create database test_db2;\n" +
        "use test_db1; CREATE PROCEDURE f() BEGIN INSERT INTO default.result VALUES(42); END;\n" +
        "use test_db2; CREATE PROCEDURE f() BEGIN INSERT INTO default.result VALUES(43); END;\n" +
        "use test_db1; f();/\n" +
        "use test_db2; f();/\n" +
        "SELECT sum(n) FROM default.result; /\n";
    testScriptFile(SCRIPT_TEXT, args(), "85");
  }

  private static List<String> args() {
    return Arrays.asList("-d", BeeLine.BEELINE_DEFAULT_JDBC_DRIVER,
            "-u", miniHS2.getBaseJdbcURL() + ";mode=hplsql", "-n", userName);
  }

  private static String testCommandLineScript(List<String> argList, InputStream inputStream)
          throws Throwable {
    BeeLine beeLine = new BeeLine();
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PrintStream beelineOutputStream = new PrintStream(os);
    beeLine.setOutputStream(beelineOutputStream);
    String[] args = argList.toArray(new String[argList.size()]);
    beeLine.begin(args, inputStream);
    beeLine.close();
    beelineOutputStream.close();
    String output = os.toString("UTF8");
    return output;
  }

  private void testScriptFile(String scriptText, List<String> argList, String expectedPattern)
          throws Throwable {
    File scriptFile = File.createTempFile(this.getClass().getSimpleName(), "temp");
    scriptFile.deleteOnExit();
    try (PrintStream os = new PrintStream(new FileOutputStream(scriptFile))) {
      os.print(scriptText);
    }
    List<String> copy = new ArrayList<>(argList);
    copy.add("-f");
    copy.add(scriptFile.getAbsolutePath());
    String output = testCommandLineScript(copy, null);
    if (!Pattern.compile(".*" + expectedPattern + ".*", Pattern.DOTALL).matcher(output).matches()) {
      fail("Output: '" + output + "' should match " + expectedPattern);
    }
    scriptFile.delete();
  }
}
