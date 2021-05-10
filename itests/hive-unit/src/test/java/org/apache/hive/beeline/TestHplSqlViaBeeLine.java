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
  public void testPackage() throws Throwable {
    String SCRIPT_TEXT =
      "DROP TABLE IF EXISTS result;\n" +
      "CREATE TABLE result (n int);\n" +
      "CREATE PACKAGE Counter AS\n" +
      "  count INT := 0;\n" +
      "  FUNCTION current() RETURNS INT;\n" +
      "  PROCEDURE inc(i INT);\n" +
      "END;\n" +
      "CREATE PACKAGE BODY Counter AS\n" +
      "  FUNCTION current() RETURNS INT IS BEGIN RETURN count; END;\n" +
      "  PROCEDURE inc(i INT) IS BEGIN count := count + i; END;\n" +
      "END;\n" +
      "Counter.inc(6172);\n" +
      "Counter.inc(6173);\n" +
      "INSERT INTO result VALUES(Counter.current());\n" +
      "SELECT * FROM result;\n";
    testScriptFile(SCRIPT_TEXT, args(), "12345");
  }
  
  @Test
  public void testUdf() throws Throwable {
    String SCRIPT_TEXT =
            "DROP TABLE IF EXISTS result;\n" +
                    "CREATE TABLE result (s string);\n" +
                    "INSERT INTO result VALUES('alice');\n" +
                    "INSERT INTO result VALUES('bob');\n" +
                    "CREATE FUNCTION hello(p STRING) RETURNS STRING BEGIN RETURN 'hello ' || p; END;\n" +
                    "SELECT hello(s) FROM result;\n";
    testScriptFile(SCRIPT_TEXT, args(), "hello alice.*hello bob");
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

  @Test
  public void testTableSelect() throws Throwable {
    String SCRIPT_TEXT =
            "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (s string);\n" +
            "DROP TABLE IF EXISTS emp;\n" +
            "CREATE TABLE emp (name string, age int);\n" +
            "INSERT INTO emp VALUES('alice', 20);\n" +
            "TYPE t_emp IS TABLE OF emp%ROWTYPE INDEX BY BINARY_INTEGER;\n" +
            "TYPE t_names IS TABLE OF emp.name%TYPE INDEX BY BINARY_INTEGER;\n" +
            "TYPE t_ages IS TABLE OF emp.age%TYPE INDEX BY BINARY_INTEGER;\n" +
            "DECLARE rows t_emp;\n" +
            "DECLARE names t_names;\n" +
            "DECLARE ages t_ages;\n" +
            "SELECT * INTO rows(200) FROM emp WHERE name = 'alice';\n" +
            "SELECT name, age INTO names(1), ages(1) FROM emp WHERE name = 'alice';\n" +
            "INSERT INTO result VALUES(rows(200).name || ' is ' || rows(200).age || ' ' || names(1) || ' = ' || ages(1));\n" +
            "SELECT * FROM result;\n";
    testScriptFile(SCRIPT_TEXT, args(), " alice is 20 alice = 20");
  }

  @Test
  public void testTableTypeCustom() throws Throwable {
    String SCRIPT_TEXT =
            "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (s string);\n" +
            "DROP TABLE IF EXISTS emp;\n" +
            "CREATE TABLE emp (name string);\n" +
            "INSERT INTO emp VALUES('bob');\n" +
            "TYPE my_type1 IS TABLE OF STRING INDEX BY BINARY_INTEGER;\n" +
            "DECLARE my_table1 my_type1;\n" +
            "DECLARE my_table2 my_type1;\n" +
            "SELECT name INTO my_table1(100) FROM emp;\n" +
            "my_table2(101) := 'alice';\n" +
            "INSERT INTO result VALUES(my_table1(100) || ' ' ||  my_table2(101));\n" +
            "SELECT * FROM result;\n";
    testScriptFile(SCRIPT_TEXT, args(), "bob alice");
  }

  @Test
  public void testTableIteration() throws Throwable {
    String SCRIPT_TEXT =
            "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (s string);\n" +
            "DROP TABLE IF EXISTS emp;\n" +
            "CREATE TABLE emp (name string);\n" +
            "INSERT INTO emp VALUES('alice');\n" +
            "INSERT INTO emp VALUES('bob');\n" +
            "TYPE tbl_type IS TABLE OF emp%ROWTYPE INDEX BY BINARY_INTEGER;\n" +
            "DECLARE tbl tbl_type;\n" +
            "SELECT * INTO tbl(1) FROM emp WHERE name = 'bob';\n" +
            "SELECT * INTO tbl(2) FROM emp WHERE name = 'alice';\n" +
            "DECLARE idx INT = tbl.FIRST;\n" +
            "DECLARE s STRING = '';\n" +
            "WHILE idx IS NOT NULL LOOP\n" +
            "   DECLARE r ROW = tbl(idx);\n" +
            "   s = s || r.name || ' ';\n" +
            "   idx = tbl.NEXT(idx);\n" +
            "END LOOP;\n" +
            "INSERT INTO result VALUES(s);\n" +
            "SELECT * FROM result;\n";
    testScriptFile(SCRIPT_TEXT, args(), "bob alice ");
  }

  @Test
  public void testTableRowAssignment() throws Throwable {
    String SCRIPT_TEXT =
            "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (s string);\n" +
            "DROP TABLE IF EXISTS emp;\n" +
            "CREATE TABLE emp (name string, age int);\n" +
            "INSERT INTO emp VALUES('alice', 16);\n" +
            "INSERT INTO emp VALUES('bob', 18);\n" +
            "TYPE tbl_type IS TABLE OF emp%ROWTYPE INDEX BY BINARY_INTEGER;\n" +
            "DECLARE tbl tbl_type;\n" +
            "DECLARE tmp tbl_type;\n" +
            "SELECT * INTO tbl(1) FROM emp WHERE name = 'alice';\n" +
            "SELECT * INTO tmp(1) FROM emp WHERE name = 'bob';\n" +
            "tbl(2) := tmp(1);" +
            "INSERT INTO result VALUES( tbl(1).name || ' ' || tbl(1).age || ' ' || tbl(2).name || ' ' || tbl(2).age );\n" +
            "SELECT * FROM result;\n";
    testScriptFile(SCRIPT_TEXT, args(), "alice 16 bob 18");
  }

  @Test
  public void testBulkCollectColumns() throws Throwable {
    String SCRIPT_TEXT =
            "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (s string);\n" +
            "DROP TABLE IF EXISTS emp;\n" +
            "CREATE TABLE emp (name string, age int);\n" +
            "INSERT INTO emp VALUES('alice', 20);\n" +
            "INSERT INTO emp VALUES('bob', 30);\n" +
            "TYPE t_names IS TABLE OF emp.name%TYPE INDEX BY BINARY_INTEGER;\n" +
            "TYPE t_ages IS TABLE OF emp.age%TYPE INDEX BY BINARY_INTEGER;\n" +
            "DECLARE names t_names;\n" +
            "DECLARE ages  t_ages;\n" +
            "SELECT name, age BULK COLLECT INTO names, ages FROM emp;\n" +
            "INSERT INTO result VALUES(names(1) || ' = ' || ages(1) || ' ' || names(2) || ' = ' || ages(2));\n" +
            "SELECT * FROM result;\n";
    testScriptFile(SCRIPT_TEXT, args(), " alice = 20 bob = 30");
  }

  @Test
  public void testBulkCollectRows() throws Throwable {
    String SCRIPT_TEXT =
            "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (s string);\n" +
            "DROP TABLE IF EXISTS emp;\n" +
            "CREATE TABLE emp (name string, age int);\n" +
            "INSERT INTO emp VALUES('alice', 20);\n" +
            "INSERT INTO emp VALUES('bob', 30);\n" +
            "TYPE t_emp IS TABLE OF emp%ROWTYPE INDEX BY BINARY_INTEGER;\n" +
            "DECLARE tbl t_emp;\n" +
            "SELECT * BULK COLLECT INTO tbl FROM emp;\n" +
            "INSERT INTO result VALUES(tbl(1).name || ' = ' || tbl(1).age || ' ' || tbl(2).name || ' = ' || tbl(2).age);\n" +
            "SELECT * FROM result;\n";
    testScriptFile(SCRIPT_TEXT, args(), " alice = 20 bob = 30");
  }

  @Test
  public void testBulkCollectFetch() throws Throwable {
    String SCRIPT_TEXT =
            "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (s string);\n" +
            "DROP TABLE IF EXISTS emp;\n" +
            "CREATE TABLE emp (name string, age int);\n" +
            "INSERT INTO emp VALUES('alice', 20);\n" +
            "INSERT INTO emp VALUES('bob', 30);\n" +
            "TYPE t_rows IS TABLE OF emp%ROWTYPE INDEX BY BINARY_INTEGER;\n" +
            "DECLARE rows t_rows;\n" +
            "DECLARE cur SYS_REFCURSOR;\n" +
            "OPEN cur FOR SELECT * FROM emp;\n" +
            "FETCH cur BULK COLLECT INTO rows;\n" +
            "CLOSE cur;\n" +
            "INSERT INTO result VALUES(rows(1).name || ' = ' || rows(1).age || ' ' || rows(2).name || ' = ' || rows(2).age);\n" +
            "SELECT * FROM result;\n";
    testScriptFile(SCRIPT_TEXT, args(), " alice = 20 bob = 30");
  }

  @Test
  public void testBulkCollectFetchLoop() throws Throwable {
    String SCRIPT_TEXT =
            "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (s string);\n" +
            "DROP TABLE IF EXISTS emp;\n" +
            "CREATE TABLE emp (name string, age int);\n" +
            "INSERT INTO emp VALUES('e1', 1);\n" +
            "INSERT INTO emp VALUES('e2', 2);\n" +
            "INSERT INTO emp VALUES('e3', 3);\n" +
            "INSERT INTO emp VALUES('e4', 4);\n" +
            "INSERT INTO emp VALUES('e5', 5);\n" +
            "INSERT INTO emp VALUES('e6', 6);\n" +
            "TYPE t_rows IS TABLE OF emp%ROWTYPE INDEX BY BINARY_INTEGER;\n" +
            "DECLARE batch t_rows;\n" +
            "DECLARE cur SYS_REFCURSOR;\n" +
            "DECLARE s STRING = '';\n" +
            "OPEN cur FOR SELECT * FROM emp;\n" +
            "LOOP\n" +
            "   FETCH cur BULK COLLECT INTO batch LIMIT 2;\n" +
            "   EXIT WHEN batch.COUNT = 0;\n" +
            "   s = s || batch(1).name || '=' || batch(1).age || ' ' || batch(2).name || '=' || batch(2).age || ' ';\n" +
            "END LOOP;\n" +
            "CLOSE cur;\n" +
            "INSERT INTO result VALUES(s);\n" +
            "SELECT * FROM result;\n";
    testScriptFile(SCRIPT_TEXT, args(), "e1=1 e2=2 e3=3 e4=4 e5=5 e6=6");
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
