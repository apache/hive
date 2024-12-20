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

import static org.apache.hive.beeline.TestBeeLineWithArgs.OutStream;
import static org.apache.hive.beeline.TestBeeLineWithArgs.testCommandLineScript;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.UtilsForTest;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHplSqlViaBeeLine {
  private static MiniHS2 miniHS2;
  private static final String USER_NAME = System.getProperty("user.name");

  /**
   * Start up a local Hive Server 2 for these tests
   */
  @BeforeClass
  public static void preTests() throws Exception {
    HiveConf hiveConf = UtilsForTest.getHiveOnTezConfFromDir("../../data/conf/tez/");
    hiveConf.setVar(HiveConf.ConfVars.HIVE_LOCK_MANAGER,
            "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_DEFAULT_FETCH_SIZE, 10);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_OPTIMIZE_METADATA_QUERIES, false);
    hiveConf.set(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LEVEL.varname, "verbose");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED, true);
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
    String scriptText =
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
    testScriptFile(scriptText, args(), "550");
  }

  @Test
  public void testHplSqlProcedure() throws Throwable {
    String scriptText =
      "DROP TABLE IF EXISTS result;\n" +
      "CREATE TABLE result (s string);\n" +
      "CREATE PROCEDURE p1(s STRING)\n" +
      "BEGIN\n" +
        "INSERT INTO result VALUES('Hello world');\n" +
      "END;\n" +
      "p1();\n" +
      "SELECT * FROM result;\n" +
      "/\n";
    testScriptFile(scriptText, args(), "wrong number of arguments in call to 'p1'. Expected 1 got 0.", OutStream.ERR);
  }

  @Test
  public void testHplSqlMultipleStatementsWithDiv() throws Throwable {
    String scriptText =
      "DROP TABLE IF EXISTS result;\n" +
      "CREATE TABLE result (n int); /\n" +
      "DECLARE c INT = 1000 / 2;\n" +
      "EXEC 'INSERT INTO result VALUES(' || c || ')';\n" +
      "SELECT * from result; /\n";
    testScriptFile(scriptText, args(), "500");
  }

  @Test
  public void testCursor() throws Throwable {
    String scriptText =
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
    testScriptFile(scriptText, args(), "44946");
  }

  @Test
  public void testPackage() throws Throwable {
    String scriptText =
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
    testScriptFile(scriptText, args(), "12345");
  }

  @Test
  public void testUdfBoolean() throws Throwable {
    String scriptText =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (col_b boolean);\n" +
            "INSERT INTO result VALUES(true);\n" +
            "INSERT INTO result VALUES(false);\n" +
            "CREATE FUNCTION check(b boolean)\n" +
            "   RETURNS STRING\n" +
            "BEGIN\n" +
            "   RETURN 'This is ' || b;\n" +
            "END;\n" +
            "SELECT check(col_b) FROM result ORDER BY col_b ASC;\n";
    testScriptFile(scriptText, args(), "This is false.*This is true");
  }

  @Test
  public void testUdfSmallInt() throws Throwable {
    String scriptText =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (col_s smallint);\n" +
            "INSERT INTO result VALUES(123);\n" +
            "INSERT INTO result VALUES(321);\n" +
            "CREATE FUNCTION dbl(s smallint)\n" +
            "   RETURNS smallint\n" +
            "BEGIN\n" +
            "   RETURN s + s;\n" +
            "END;\n" +
            "SELECT dbl(col_s) FROM result;\n";
    testScriptFile(scriptText, args(), "246.*642");
  }

  @Test
  public void testUdfInt() throws Throwable {
    String scriptText =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (col_i int);\n" +
            "INSERT INTO result VALUES(12345);\n" +
            "INSERT INTO result VALUES(54321);\n" +
            "CREATE FUNCTION dbl(i int)\n" +
            "   RETURNS int\n" +
            "BEGIN\n" +
            "   RETURN i * 2;\n" +
            "END;\n" +
            "SELECT dbl(col_i) FROM result;\n";
    testScriptFile(scriptText, args(), "24690.*108642");
  }

  @Test
  public void testUdfBigInt() throws Throwable {
    String scriptText =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (col_b bigint);\n" +
            "INSERT INTO result VALUES(123456789);\n" +
            "INSERT INTO result VALUES(987654321);\n" +
            "CREATE FUNCTION dbl(b bigint)\n" +
            "   RETURNS int8\n" +
            "BEGIN\n" +
            "   RETURN b * 2;\n" +
            "END;\n" +
            "SELECT dbl(col_b) FROM result;\n";
    testScriptFile(scriptText, args(), "246913578.*1975308642");
  }

  @Test
  public void testUdfFloat() throws Throwable {
    String scriptText =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (col_f float);\n" +
            "INSERT INTO result VALUES(12345.6789);\n" +
            "INSERT INTO result VALUES(98765.4321);\n" +
            "CREATE FUNCTION dbl(f float)\n" +
            "   RETURNS float\n" +
            "BEGIN\n" +
            "   RETURN f * 2;\n" +
            "END;\n" +
            "SELECT dbl(col_f) FROM result;\n";
    testScriptFile(scriptText, args(), "24691.357421875.*197530.859375");
  }

  @Test
  public void testUdfDouble() throws Throwable {
    String scriptText =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (col_d double);\n" +
            "INSERT INTO result VALUES(123456789.12);\n" +
            "INSERT INTO result VALUES(987654321.98);\n" +
            "CREATE FUNCTION dbl(d float)\n" +
            "   RETURNS double\n" +
            "BEGIN\n" +
            "   RETURN d * 2;\n" +
            "END;\n" +
            "SELECT dbl(col_d) FROM result;\n";
    testScriptFile(scriptText, args(), "2.4691357824E8.*1.97530864396E9");
  }

  @Test
  public void testUdfString() throws Throwable {
    String scriptText =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (col_s string);\n" +
            "INSERT INTO result VALUES('Alice');\n" +
            "INSERT INTO result VALUES('Smith');\n" +
            "CREATE FUNCTION hello(s string)\n" +
            "   RETURNS string\n" +
            "BEGIN\n" +
            "   RETURN 'Hello ' || s || '!';\n" +
            "END;\n" +
            "SELECT hello(col_s) FROM result ORDER BY col_s ASC;\n";
    testScriptFile(scriptText, args(), "Hello Alice!.*Hello Smith!");
  }

  @Test
  public void testUdfDate() throws Throwable {
    String scriptText =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (col_d date);\n" +
            "INSERT INTO result VALUES('2022-11-24');\n" +
            "INSERT INTO result VALUES('2022-12-25');\n" +
            "CREATE FUNCTION date_today(d date)\n" +
            "   RETURNS date\n" +
            "BEGIN\n" +
            "   RETURN d;\n" +
            "END;\n" +
            "SELECT date_today(col_d) FROM result;\n";
    testScriptFile(scriptText, args(), "2022-11-24.*2022-12-25");
  }

  @Test
  public void testUdfTimestamp() throws Throwable {
    String scriptText =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (col_t timestamp);\n" +
            "INSERT INTO result VALUES('2022-11-24 10:20:30');\n" +
            "INSERT INTO result VALUES('2022-12-25 06:30:30');\n" +
            "CREATE FUNCTION time_today(t timestamp)\n" +
            "   RETURNS timestamp\n" +
            "BEGIN\n" +
            "   RETURN t;\n" +
            "END;\n" +
            "SELECT time_today(col_t) FROM result;\n";
    testScriptFile(scriptText, args(), "2022-11-24 10:20:30.*2022-12-25 06:30:30");
  }

  @Test
  public void testUdfDecimal() throws Throwable {
    String scriptText =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (col_d decimal(15,2));\n" +
            "INSERT INTO result VALUES(123456789.98);\n" +
            "INSERT INTO result VALUES(987654321.12);\n" +
            "CREATE FUNCTION triple(d decimal(15,2))\n" +
            "   RETURNS decimal(15,2)\n" +
            "BEGIN\n" +
            "   RETURN d * 3;\n" +
            "END;\n" +
            "SELECT triple(col_d) FROM result;\n";
    testScriptFile(scriptText, args(), "370370369.94.*2962962963.36");
  }

  @Test
  public void testUdfVarchar() throws Throwable {
    String scriptText =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (col_v varchar(20));\n" +
            "INSERT INTO result VALUES('Smith');\n" +
            "INSERT INTO result VALUES('Sachin');\n" +
            "CREATE FUNCTION hello(v varchar(20))\n" +
            "   RETURNS varchar(20)\n" +
            "BEGIN\n" +
            "   RETURN 'Hello ' || v || '!';\n" +
            "END;\n" +
            "SELECT hello(col_v) FROM result ORDER BY col_v ASC;\n";
    testScriptFile(scriptText, args(), "Hello Sachin!.*Hello Smith!");
  }

  @Test
  public void testUdfChar() throws Throwable {
    String scriptText =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (col_c char(10));\n" +
            "INSERT INTO result VALUES('Daya');\n" +
            "INSERT INTO result VALUES('Alice');\n" +
            "CREATE FUNCTION hello(c char(10))\n" +
            "   RETURNS char(10)\n" +
            "BEGIN\n" +
            "   RETURN 'Hello ' || c || '!';\n" +
            "END;\n" +
            "SELECT hello(col_c) FROM result ORDER BY col_c ASC;\n";
    testScriptFile(scriptText, args(), "Hello Alice!.*Hello Daya!");
  }

  @Test
  public void testUdfWhenUdfParamerAndActualParamDifferent() throws Throwable {
    String scriptText =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (col_d decimal(10,2));\n" +
            "INSERT INTO result VALUES(12345.67);\n" +
            "INSERT INTO result VALUES(98765.43);\n" +
            "CREATE FUNCTION hello(s String)\n" +
            "   RETURNS String\n" +
            "BEGIN\n" +
            "   RETURN 'Hello ' || s || '!';\n" +
            "END;\n" +
            "SELECT hello(col_d) FROM result;\n";
    testScriptFile(scriptText, args(), "Hello 12345.67!.*Hello 98765.43!");
  }

  @Test
  public void testBuiltInUdf() throws Throwable {
    String scriptText = "SELECT abs(-2);\n";
    testScriptFile(scriptText, args(), "2");
  }

  @Test
  public void testNestedUdfAndProcedure() throws Throwable {
    String scriptText =
        "CREATE FUNCTION dbl(d int)\n" +
        "   RETURNS int\n" +
        "BEGIN\n" +
        "   RETURN d * 2;\n" +
        "END;\n" +
        "SELECT dbl(abs(-2)), abs(dbl(-2)), dbl(dbl(20));\n";
    testScriptFile(scriptText, args(), "4.*4\\.0.*80");
  }

  @Test
    public void testDbChange() throws Throwable {
    String scriptText =
        "DROP TABLE IF EXISTS result;\n" +
        "CREATE TABLE result (n int);\n" +
        "create database test_db1;\n" +
        "create database test_db2;\n" +
        "use test_db1; CREATE PROCEDURE f() BEGIN INSERT INTO default.result VALUES(42); END;\n" +
        "use test_db2; CREATE PROCEDURE f() BEGIN INSERT INTO default.result VALUES(43); END;\n" +
        "use test_db1; f();/\n" +
        "use test_db2; f();/\n" +
        "SELECT sum(n) FROM default.result; /\n";
    testScriptFile(scriptText, args(), "85");
  }

  @Test
  public void testTableSelect() throws Throwable {
    String scriptText =
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
    testScriptFile(scriptText, args(), " alice is 20 alice = 20");
  }

  @Test
  public void testTableTypeCustom() throws Throwable {
    String scriptText =
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
    testScriptFile(scriptText, args(), "bob alice");
  }

  @Test
  public void testTableIteration() throws Throwable {
    String scriptText =
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
    testScriptFile(scriptText, args(), "bob alice ");
  }

  @Test
  public void testTableRowAssignment() throws Throwable {
    String scriptText =
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
    testScriptFile(scriptText, args(), "alice 16 bob 18");
  }

  @Test
  public void testBulkCollectColumns() throws Throwable {
    String scriptText =
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
    testScriptFile(scriptText, args(), " alice = 20 bob = 30");
  }

  @Test
  public void testBulkCollectRows() throws Throwable {
    String scriptText =
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
    testScriptFile(scriptText, args(), " alice = 20 bob = 30");
  }

  @Test
  public void testBulkCollectFetch() throws Throwable {
    String scriptText =
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
    testScriptFile(scriptText, args(), " alice = 20 bob = 30");
  }

  @Test
  public void testBulkCollectFetchLoop() throws Throwable {
    String scriptText =
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
    testScriptFile(scriptText, args(), "e1=1 e2=2 e3=3 e4=4 e5=5 e6=6");
  }

  @Test
  public void testDecimalCast() throws Throwable {
    String scriptText =
        "DECLARE\n" +
        "a DECIMAL(10,2);\n" +
        "BEGIN\n" +
        "SELECT CAST('10.5' AS DECIMAL(10,2)) as t INTO a;\n" +
        "print (a);\n" +
        "END;\n" +
        "/";
    testScriptFile(scriptText, args(), "10.50", OutStream.ERR);
  }

  @Test
  public void testNullCast() throws Throwable {
    String scriptText =
        "BEGIN\n" +
        "DECLARE a BIGINT;\n" +
        "print('started');\n" +
        "SELECT cast (null as BIGINT) as t INTO a\n" +
        "print (a);\n" +
        "print ('here');\n" +
        "end;\n" +
        "/";
    // Inverted match, output should not have NPE
    testScriptFile(scriptText, args(), "^(.(?!(NullPointerException)))*$", OutStream.ERR);
  }

  @Test
  public void testACTIVITY_COUNTHplSqlFunction1() throws Throwable {
    String scriptText =
        "DROP TABLE IF EXISTS result;\n" +
        "CREATE TABLE result (col1 string);\n" +
        "INSERT INTO result VALUES('Alice');\n" +
        "INSERT INTO result VALUES('Bob');\n" +
        "SELECT * FROM result;\n" +
        "SELECT ACTIVITY_COUNT;";
    testScriptFile(scriptText, args(), "2");
  }

  @Test
  public void testACTIVITY_COUNTHplSqlFunction2() throws Throwable {
    String SCRIPT_TEXT =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (col1 string);\n" +
            "INSERT INTO result VALUES('Alice');\n" +
            "INSERT INTO result VALUES('Bob');\n" +
            "DECLARE var INT;\n" +
            "SELECT count(*) INTO var FROM result;\n" +
            "IF ACTIVITY_COUNT = 1 THEN\n" +
            " PRINT 'id = ' || var;\n" +
            "END IF;";
    testScriptFile(SCRIPT_TEXT, args(), "id = 2", OutStream.ERR);
  }

  @Test
  public void testCASTHplSqlFunction1() throws Throwable {
    String scriptText = "SELECT CAST('Abc' AS CHAR(1));";
    testScriptFile(scriptText, args(), "A");
  }

  @Test
  public void testCASTHplSqlFunction2() throws Throwable {
    String scriptText = "SELECT CAST(TIMESTAMP '2015-03-12 10:58:34.111' AS CHAR(10));";
    testScriptFile(scriptText, args(), "2015-03-12");
  }

  @Test
  public void testCASTHplSqlFunction3() throws Throwable {
    String SCRIPT_TEXT = "SELECT CAST(DATE '2023-05-29' AS CHAR(4));";
    testScriptFile(SCRIPT_TEXT, args(), "2023");
  }

  @Test
  public void testCHARHplSqlFunction() throws Throwable {
    String scriptText = "select CHAR(2023)";
    testScriptFile(scriptText, args(), "2023");
  }

  @Test
  public void testCOALESCEHplSQLFunction() throws Throwable {
    String scriptText = "select COALESCE(null,123,2023)";
    testScriptFile(scriptText, args(), "123");
  }

  @Test
  public void testCONCATHplSQLFunction() throws Throwable {
    String scriptText = "select CONCAT('a', 'b', NULL, 'c')";
    testScriptFile(scriptText, args(), "abc");
  }

  @Test
  public void testCURRENTHplSQLFunction1() throws Throwable {
    String scriptText = "SELECT CURRENT DATE;";
    testCurrentDate(scriptText);
  }

  private void testCurrentDate(String scriptText) throws Throwable {
    Date today = new Date(System.currentTimeMillis());
    testScriptFile(scriptText, args(), today.toString());
  }

  @Test
  public void testCURRENTHplSQLFunction2() throws Throwable {
    String scriptText = "SELECT CURRENT TIMESTAMP;";
    testCurrentTimestamp(scriptText);
  }

  private void testCurrentTimestamp(String scriptText) throws Throwable {
    Timestamp today = new Timestamp(System.currentTimeMillis());
    String timestamp = today.toString();
    testScriptFile(scriptText, args(), timestamp.substring(0, timestamp.length() - 9));
  }

  @Test
  public void testCURRENTHplSQLFunction3() throws Throwable {
    String scriptText = "SELECT CURRENT USER;";
    testScriptFile(scriptText, args(), System.getProperty("user.name"));
  }

  @Test
  public void testCURRENT_DATEHplSQLFunction() throws Throwable {
    String scriptText = "SELECT CURRENT_DATE;";
    testCurrentDate(scriptText);
  }

  @Test
  public void testCURRENT_TIME_MILLISHplSQLFunction() throws Throwable {
    String scriptText = "SELECT CURRENT_TIME_MILLIS();";
    testScriptFile(scriptText, args(), String.valueOf(System.currentTimeMillis() / 100000));
  }

  @Test
  public void testCURRENT_TIMESTAMPHplSQLFunction() throws Throwable {
    String scriptText = "SELECT CURRENT_TIMESTAMP;";
    testCurrentTimestamp(scriptText);
  }

  @Test
  public void testCURRENT_USERHplSQLFunction() throws Throwable {
    String scriptText = "SELECT CURRENT_USER;";
    testScriptFile(scriptText, args(), System.getProperty("user.name"));
  }

  @Test
  public void testDATEHplSQLFunction() throws Throwable {
    String scriptText = "SELECT DATE('2015-03-12');";
    testScriptFile(scriptText, args(), "2015-03-12");
  }

  @Test
  public void testDECODEHplSQLFunction() throws Throwable {
    String scriptText = "DECLARE var1 INT DEFAULT 3;\n" + "SELECT DECODE (var1, 1, 'A', 2, 'B', 3, 'C');";
    testScriptFile(scriptText, args(), "C");
  }

  @Test
  public void testFROM_UNIXTIMEHplSQLFunction() throws Throwable {
    String scriptText = "SELECT from_unixtime(1447141681, 'yyyy-MM-dd');";
    testScriptFile(scriptText, args(), "2015-11-");
  }

  @Test
  public void testINSTRHplSQLFunction1() throws Throwable {
    String scriptText = "SELECT INSTR('abc', 'b');";
    testScriptFile(scriptText, args(), "2");
  }

  @Test
  public void testINSTRHplSQLFunction2() throws Throwable {
    String scriptText = "SELECT INSTR('abcabcabc', 'b', 3, 2);";
    testScriptFile(scriptText, args(), "8");
  }

  @Test
  public void testLOWERHplSQLFunction() throws Throwable {
    String scriptText = "SELECT LOWER('ABC');";
    testScriptFile(scriptText, args(), "abc");
  }

  @Test
  public void testLENHplSQLFunction() throws Throwable {
    String scriptText = "SELECT LEN('Abc ');";
    testScriptFile(scriptText, args(), "3");
  }

  @Test
  public void testLENGTHHplSQLFunction() throws Throwable {
    String scriptText = "SELECT LENGTH('Abc ');";
    testScriptFile(scriptText, args(), "4");
  }

  @Test
  public void testMODHplSQLFunction() throws Throwable {
    String scriptText = "SELECT MOD(5,2);";
    testScriptFile(scriptText, args(), "1");
  }

  @Test
  public void testNOWHplSQLFunction() throws Throwable {
    String scriptText = "SELECT NOW();";
    testCurrentTimestamp(scriptText);
  }

  @Test
  public void testNVLHplSQLFunction() throws Throwable {
    String scriptText = "SELECT NVL(NULL, 100);";
    testScriptFile(scriptText, args(), "100");
  }

  @Test
  public void testNVL2HplSQLFunction() throws Throwable {
    String scriptText = "SELECT NVL2(NULL, 100, 200);";
    testScriptFile(scriptText, args(), "200");
  }

  @Test
  public void testREPLACEHplSQLFunction() throws Throwable {
    String scriptText = "SELECT replace('2016-03-03', '-', '');";
    testScriptFile(scriptText, args(), "20160303");
  }

  @Test
  public void testSUBSTRHplSQLFunction1() throws Throwable {
    String scriptText = "SELECT SUBSTR('Remark', 3);";
    testScriptFile(scriptText, args(), "mark");
  }

  @Test
  public void testSUBSTRHplSQLFunction2() throws Throwable {
    String scriptText = "SELECT SUBSTR('Remark', 3, 3);";
    testScriptFile(scriptText, args(), "mar");
  }

  @Test
  public void testSUBSTRINGHplSQLFunction1() throws Throwable {
    String scriptText = "SELECT SUBSTRING('Remark', 3);";
    testScriptFile(scriptText, args(), "mark");
  }

  @Test
  public void testSUBSTRINGHplSQLFunction2() throws Throwable {
    String scriptText = "SELECT SUBSTRING('Remark', 3, 3);";
    testScriptFile(scriptText, args(), "mar");
  }

  @Test
  public void testSYSDATEHplSQLFunction() throws Throwable {
    String scriptText = "SELECT SYSDATE;";
    testCurrentTimestamp(scriptText);
  }

  @Test
  public void testTIMESTAMP_ISOHplSQLFunction() throws Throwable {
    String scriptText = "SELECT TIMESTAMP_ISO('2015-03-12');";
    testScriptFile(scriptText, args(), "2015-03-12 00:00:00");
  }

  @Test
  public void testTO_CHARHplSQLFunction() throws Throwable {
    String scriptText = "SELECT TO_CHAR(CURRENT_DATE);";
    testCurrentDate(scriptText);
  }

  @Test
  public void testTO_TIMESTAMPHplSQLFunction1() throws Throwable {
    String scriptText = "SELECT TO_TIMESTAMP('2015-04-02', 'YYYY-MM-DD');";
    testScriptFile(scriptText, args(), "2015-04-02 00:00:00.0");
  }

  @Test
  public void testTO_TIMESTAMPHplSQLFunction2() throws Throwable {
    String scriptText = "SELECT TO_TIMESTAMP('04/02/2015', 'mm/dd/yyyy');";
    testScriptFile(scriptText, args(), "2015-04-02 00:00:00.0");
  }

  @Test
  public void testTO_TIMESTAMPHplSQLFunction3() throws Throwable {
    String scriptText = "SELECT TO_TIMESTAMP('2015-04-02 13:51:31', 'YYYY-MM-DD HH24:MI:SS');";
    testScriptFile(scriptText, args(), "2015-04-02 13:51:31.0");
  }

  @Test
  public void testTRIMHplSQLFunction() throws Throwable {
    String scriptText = "SELECT '#' || TRIM(' Hello ') || '#';";
    testScriptFile(scriptText, args(), "#Hello#");
  }

  @Test
  public void testUNIX_TIMESTAMPHplSQLFunction() throws Throwable {
    String scriptText = "SELECT UNIX_TIMESTAMP()";
    testScriptFile(scriptText, args(), String.valueOf(System.currentTimeMillis()/1000));
  }

  @Test
  public void testUPPERHplSQLFunction() throws Throwable {
    String scriptText = "SELECT UPPER('abc');";
    testScriptFile(scriptText, args(), "ABC");
  }

  @Test
  public void testUSERHplSQLFunction() throws Throwable {
    String scriptText = "SELECT USER;";
    testScriptFile(scriptText, args(), System.getProperty("user.name"));
  }

  @Test
  public void testTableAliasInColumnName() throws Throwable {
    String scriptText =
        "DROP TABLE IF EXISTS input;\n" +
            "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE input (col1 string, col2 int);\n" +
            "CREATE TABLE result (res string);\n" +
            "INSERT INTO input VALUES('Hive', 2023);\n" +
            "CREATE PROCEDURE p1() AS\n" +
            "  BEGIN\n" +
            "    FOR rec IN (select tab.col1, tab.col2 num from input tab) LOOP\n" +
            "      INSERT INTO result VALUES(rec.num || ' = ' || rec.col1);\n" +
            "  END LOOP;\n" +
            "END;\n" +
            "p1();\n" +
            "SELECT * FROM result;\n";
    testScriptFile(scriptText, args(), "2023 = Hive");
  }

  @Test
  public void testHplSqlProcedureCallingWithAllDefaultValues() throws Throwable {
    String scriptText =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (s string);\n" +
            "CREATE PROCEDURE p1(s STRING DEFAULT 'default_val', num NUMBER DEFAULT 123)\n" +
            "BEGIN\n" +
            "INSERT INTO result VALUES(s || ' = ' || num);\n" +
            "END;\n" +
            "p1();\n" +
            "SELECT * FROM result;" ;
    testScriptFile(scriptText, args(), "default_val = 123");
  }

  @Test
  public void testHplSqlProcedureCallingWithSomeDefaultValues() throws Throwable {
    String scriptText =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (s string);\n" +
            "CREATE PROCEDURE p1(s STRING DEFAULT 'default_val', num NUMBER DEFAULT 123)\n" +
            "BEGIN\n" +
            "INSERT INTO result VALUES(s || ' = ' || num);\n" +
            "END;\n" +
            "p1('Pass_Value');\n" +
            "SELECT * FROM result;" ;
    testScriptFile(scriptText, args(), "Pass_Value = 123");
  }

  @Test
  public void testHplSqlProcedureWithDefaultValues() throws Throwable {
    String scriptText =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (s string);\n" +
            "CREATE PROCEDURE p1(s STRING DEFAULT 'default_val', num NUMBER)\n" +
            "BEGIN\n" +
            "INSERT INTO result VALUES(s || ' = ' || num);\n" +
            "END;\n" +
            "p1(111);\n" +
            "SELECT * FROM result;" ;
    testScriptFile(scriptText, args(), "wrong number of arguments in call to 'p1'. Expected 2 got 1.", OutStream.ERR);
  }

  @Test
  public void testHplSqlProcedureWithSomeDefaultValues() throws Throwable {
    String scriptText =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (s string);\n" +
            "CREATE PROCEDURE p1(s STRING, num NUMBER DEFAULT 123)\n" +
            "BEGIN\n" +
            "INSERT INTO result VALUES(s || ' = ' || num);\n" +
            "END;\n" +
            "p1('Passed_Val');\n" +
            "SELECT * FROM result;" ;
    testScriptFile(scriptText, args(), "Passed_Val = 123");
  }

  @Test
  public void testHplSqlProcedureWithDefaultParamCallingWithNamedParameterBinding() throws Throwable {
    String scriptText =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (s string);\n" +
            "CREATE PROCEDURE p1(s STRING DEFAULT 'default_val', num NUMBER)\n" +
            "BEGIN\n" +
            "INSERT INTO result VALUES(s || ' = ' || num);\n" +
            "END;\n" +
            "p1(num => 111);\n" +
            "SELECT * FROM result;" ;
    testScriptFile(scriptText, args(), "default_val = 111");
  }

  @Test
  public void testHplSqlProcedureWithAllDefaultParamsCallingWithNamedParameterBinding() throws Throwable {
    String scriptText =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (s string);\n" +
            "CREATE PROCEDURE p1(s1 STRING default 'Default S1', s2 string default 'Default S2')\n" +
            "BEGIN\n" +
            "INSERT INTO result VALUES(s1 || '=' || s2);\n" +
            "END;\n" +
            "p1(s2 => 'PassedValue S2');\n" +
            "SELECT * FROM result;" ;
    testScriptFile(scriptText, args(), "Default S1=PassedValue S2");
  }

  @Test
  public void testHplSqlProcedureWithoutParameters() throws Throwable {
    String scriptText =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (s string);\n" +
            "CREATE PROCEDURE p1()\n" +
            "BEGIN\n" +
            "INSERT INTO result VALUES('No param');\n" +
            "END;\n" +
            "p1('none');\n" +
            "SELECT * FROM result;" ;
    testScriptFile(scriptText, args(), "wrong number of arguments in call to 'p1'. Expected 0 got 1.", OutStream.ERR);
  }

  @Test
  public void testHiveVariableInHplsql() throws Throwable {
    String scriptText =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (s string);\n" +
            "CREATE PROCEDURE p1()\n" +
            "DECLARE hivedb_tbl string;\n" +
            "BEGIN\n" +
            "SELECT hivedb || '.' || hivetbl into hivedb_tbl;\n" +
            "INSERT INTO result VALUES(hivedb_tbl);\n" +
            "END;\n" +
            "p1();\n" +
            "SELECT * FROM result;" ;
    List<String> args = new ArrayList<>(args());
    args.add("--hivevar");
    args.add("hivedb=sys");
    args.add("--hivevar");
    args.add("hivetbl=tbls");
    testScriptFile(scriptText, args, "sys.tbls");
  }

  @Test
  public void testHplSqlContinueConditionHandler() throws Throwable {
    String scriptText =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (s string);\n" +
            "CREATE PROCEDURE p1()\n" +
            "BEGIN\n" +
            " INSERT INTO result VALUES('Continue CONDITION Handler invoked.');\n" +
            "END;\n" +
            "DECLARE cnt_condition CONDITION;\n" +
            "DECLARE CONTINUE HANDLER FOR cnt_condition\n" +
            " p1();\n" +
            "IF 1 <> 2 THEN\n" +
            " SIGNAL cnt_condition;\n" +
            "END IF;\n" +
            "SELECT * FROM result;";
    testScriptFile(scriptText, args(), "Continue CONDITION Handler invoked.");
  }

  @Test
  public void testHplSqlExitConditionHandler() throws Throwable {
    String scriptText =
        "CREATE PROCEDURE p1()\n" +
            "BEGIN\n" +
            " PRINT('Exit CONDITION Handler invoked.');\n" +
            "END;\n" +
            "DECLARE cnt_condition CONDITION;\n" +
            "DECLARE EXIT HANDLER FOR cnt_condition\n" +
            " p1();\n" +
            "IF 1 <> 2 THEN\n" +
            " SIGNAL cnt_condition;\n" +
            "END IF;";
    testScriptFile(scriptText, args(), "Exit CONDITION Handler invoked.", OutStream.ERR);
  }

  @Test
  public void testExecuteImmediateSelectCountStatement() throws Throwable {
    String scriptText =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (s string);\n" +
            "execute immediate 'SELECT count(*) from result';";
    // Inverted match, output should not have NPE
    testScriptFile(scriptText, args(), "^(.(?!(ClassCastException)))*$", OutStream.ERR);
  }

  @Test
  public void testExecuteSelectCountStatement() throws Throwable {
    String scriptText =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (s string);\n" +
            "execute 'SELECT count(*) from result';";
    // Inverted match, output should not have NPE
    testScriptFile(scriptText, args(), "^(.(?!(ClassCastException)))*$", OutStream.ERR);
  }

  @Test
  public void testSetHplsqlOnErrorStop() throws Throwable {
    String scriptText =
        "SET hplsql.onerror='stop';\n" +
            "insert into abc values('Tbl Not Exists');\n" +
            "SELECT CURRENT_USER;";
    testScriptFile(scriptText, args(), "^(.(?!(" + System.getProperty("user.name") + ")))*$");
  }

  @Test
  public void testSetHplsqlOnErrorSetError() throws Throwable {
    String scriptText =
        "SET hplsql.onerror='seterror';\n" +
            "insert into abc values('Tbl Not Exists');\n" +
            "if SQLCODE < 0\n" + " PRINT 'SQL Error...';";
    testScriptFile(scriptText, args(), "SessionState: SQL Error...", OutStream.ERR);
  }

  @Test
  public void testSetHplsqlOnErrorException() throws Throwable {
    String scriptText =
        "SET hplsql.onerror='exception';\n" +
            "insert into abc values('Tbl Not Exists');\n" +
            "SELECT CURRENT_USER;";
    testScriptFile(scriptText, args(), "^(.(?!(" + System.getProperty("user.name") + ")))*$");
  }

  @Test
  public void testHplSqlProcedureWithCast1Udf() throws Throwable {
    String SCRIPT_TEXT =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (id int, s string);\n" +
            "CREATE PROCEDURE p1(s1 STRING, s2 string)\n" +
            "BEGIN\n" +
            "INSERT INTO result VALUES(cast(s1 as int), s2);\n" +
            "END;\n" +
            "p1('786', 'Cloud');\n" +
            "SELECT * FROM result;" ;
    testScriptFile(SCRIPT_TEXT, args(), "786.*Cloud");
  }

  @Test
  public void testHplSqlProcedureWithCast2Udf() throws Throwable {
    String SCRIPT_TEXT =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (s1 string, s2 string);\n" +
            "CREATE PROCEDURE p1(s1 STRING, s2 string)\n" +
            "BEGIN\n" +
            "INSERT INTO result VALUES(cast(s1 as char(1)), s2);\n" +
            "END;\n" +
            "p1('Hive', 'Cloud');\n" +
            "SELECT * FROM result;" ;
    testScriptFile(SCRIPT_TEXT, args(), "H.*Cloud");
  }

  @Test
  public void testHplSqlProcedureWithCast3Udf() throws Throwable {
    String SCRIPT_TEXT =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (s1 string, s2 string);\n" +
            "CREATE PROCEDURE p1(s1 STRING, s2 string)\n" +
            "BEGIN\n" +
            "INSERT INTO result VALUES(CAST(TIMESTAMP '2015-03-12 10:58:34.111' AS CHAR(10)), s2);\n" +
            "END;\n" +
            "p1('Hive', 'Cloud');\n" +
            "SELECT * FROM result;" ;
    testScriptFile(SCRIPT_TEXT, args(), "2015-03-12.*Cloud");
  }

  @Test
  public void testHplSqlProcedureWithCOALESCEUdf() throws Throwable {
    String SCRIPT_TEXT =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (id int, s string);\n" +
            "CREATE PROCEDURE p1(s1 int, s2 string)\n" +
            "BEGIN\n" +
            "INSERT INTO result VALUES(COALESCE(NULL, s1, 2, 3), s2);\n" +
            "END;\n" +
            "p1(786, 'Cloud');\n" +
            "SELECT * FROM result;" ;
    testScriptFile(SCRIPT_TEXT, args(), "786.*Cloud");
  }

  @Test
  public void testHplSqlProcedureWithCONCATUdf() throws Throwable {
    String SCRIPT_TEXT =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (id int, s string);\n" +
            "CREATE PROCEDURE p1(s1 int, s2 string)\n" +
            "BEGIN\n" +
            "INSERT INTO result VALUES(s1, CONCAT('M', 'y ', NULL, s2));\n" +
            "END;\n" +
            "p1(786, 'Cloud');\n" +
            "SELECT * FROM result;" ;
    testScriptFile(SCRIPT_TEXT, args(), "786.*My Cloud");
  }

  @Test
  public void testHplSqlProcedureWithSYSDATEUdf() throws Throwable {
    String SCRIPT_TEXT =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (name String, join_date timestamp);\n" +
            "CREATE PROCEDURE p1(s1 string)\n" +
            "BEGIN\n" +
            "INSERT INTO result VALUES(s1, SYSDATE);\n" +
            "END;\n" +
            "p1('Bob');\n" +
            "SELECT * FROM result;" ;
    Timestamp today = new Timestamp(System.currentTimeMillis());
    String timestamp = today.toString();
    testScriptFile(SCRIPT_TEXT, args(), "Bob.*" + timestamp.substring(0, timestamp.length() - 9));
  }

  @Test
  public void testHplSqlProcedureWithCurrent_DateUdf() throws Throwable {
    String SCRIPT_TEXT =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (name String, join_date date);\n" +
            "CREATE PROCEDURE p1(s1 string)\n" +
            "BEGIN\n" +
            "INSERT INTO result VALUES(s1, current_date);\n" +
            "END;\n" +
            "p1('Bob');\n" +
            "SELECT * FROM result;" ;
    Date today = new Date(System.currentTimeMillis());
    testScriptFile(SCRIPT_TEXT, args(), "Bob.*" + today.toString());
  }

  @Test
  public void testHplSqlProcedureWithCurrent_TimestampUdf() throws Throwable {
    String SCRIPT_TEXT =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (name String, join_time timestamp);\n" +
            "CREATE PROCEDURE p1(s1 string)\n" +
            "BEGIN\n" +
            "INSERT INTO result VALUES(s1, current_timestamp);\n" +
            "END;\n" +
            "p1('Bob');\n" +
            "SELECT * FROM result;" ;
    Timestamp today = new Timestamp(System.currentTimeMillis());
    String timestamp = today.toString();
    testScriptFile(SCRIPT_TEXT, args(), "Bob.*" + timestamp.substring(0, timestamp.length() - 9));
  }

  @Test
  public void testHplSqlProcedureWithCurrent_UserUdf() throws Throwable {
    String SCRIPT_TEXT =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (name String);\n" +
            "CREATE PROCEDURE p1(s1 string)\n" +
            "BEGIN\n" +
            "INSERT INTO result VALUES(current_user);\n" +
            "END;\n" +
            "p1('Bob');\n" +
            "SELECT * FROM result;" ;
    testScriptFile(SCRIPT_TEXT, args(), System.getProperty("user.name"));
  }

  @Test
  public void testHplSqlProcedureWithFrom_UnixtimeUdf() throws Throwable {
    String SCRIPT_TEXT =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (name String);\n" +
            "CREATE PROCEDURE p1(jt int)\n" +
            "BEGIN\n" +
            "INSERT INTO result VALUES(from_unixtime(jt, 'yyyy-MM-dd'));\n" +
            "END;\n" +
            "p1(1447141681);\n" +
            "SELECT * FROM result;" ;
    testScriptFile(SCRIPT_TEXT, args(), "2015-11-");
  }

  @Test
  public void testHplSqlProcedureWithInstrUdf() throws Throwable {
    String SCRIPT_TEXT =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (name String);\n" +
            "CREATE PROCEDURE p1(s1 string)\n" +
            "BEGIN\n" +
            "INSERT INTO result VALUES(instr(s1, 'b', -3, 2));\n" +
            "END;\n" +
            "p1('abcabcabc');\n" +
            "SELECT * FROM result;" ;
    testScriptFile(SCRIPT_TEXT, args(), "2");
  }

  @Test
  public void testHplSqlProcedureWithLowerUdf() throws Throwable {
    String SCRIPT_TEXT =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (name String);\n" +
            "CREATE PROCEDURE p1(s1 string)\n" +
            "BEGIN\n" +
            "INSERT INTO result VALUES(lower(s1));\n" +
            "END;\n" +
            "p1('ABCD');\n" +
            "SELECT * FROM result;" ;
    testScriptFile(SCRIPT_TEXT, args(), "abcd");
  }

  @Test
  public void testHplSqlProcedureWithLengthUdf() throws Throwable {
    String SCRIPT_TEXT =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (name String);\n" +
            "CREATE PROCEDURE p1(s1 string)\n" +
            "BEGIN\n" +
            "INSERT INTO result VALUES(length(s1));\n" +
            "END;\n" +
            "p1('Cloud');\n" +
            "SELECT * FROM result;" ;
    testScriptFile(SCRIPT_TEXT, args(), "5");
  }

  @Test
  public void testHplSqlProcedureWithNvlUdf() throws Throwable {
    String SCRIPT_TEXT =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (name String);\n" +
            "CREATE PROCEDURE p1(s1 string)\n" +
            "BEGIN\n" +
            "INSERT INTO result VALUES(nvl(NULL,s1));\n" +
            "END;\n" +
            "p1('Cloud');\n" +
            "SELECT * FROM result;" ;
    testScriptFile(SCRIPT_TEXT, args(), "Cloud");
  }

  @Test
  public void testHplSqlProcedureWithReplaceUdf() throws Throwable {
    String SCRIPT_TEXT =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (name String);\n" +
            "CREATE PROCEDURE p1(s1 string)\n" +
            "BEGIN\n" +
            "INSERT INTO result VALUES(replace(s1, '-', ''));\n" +
            "END;\n" +
            "p1('2024-02-29');\n" +
            "SELECT * FROM result;" ;
    testScriptFile(SCRIPT_TEXT, args(), "20240229");
  }

  @Test
  public void testHplSqlProcedureWithTrimUdf() throws Throwable {
    String SCRIPT_TEXT =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (name String);\n" +
            "CREATE PROCEDURE p1(s1 string)\n" +
            "BEGIN\n" +
            "INSERT INTO result VALUES('#' || TRIM(s1) || '#');\n" +
            "END;\n" +
            "p1(' Cloud ');\n" +
            "SELECT * FROM result;" ;
    testScriptFile(SCRIPT_TEXT, args(), "#Cloud#");
  }

  @Test
  public void testHplSqlProcedureWithUnix_TimestampUdf() throws Throwable {
    String SCRIPT_TEXT =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (col_millis int);\n" +
            "CREATE PROCEDURE p1()\n" +
            "BEGIN\n" +
            "INSERT INTO result VALUES(UNIX_TIMESTAMP());\n" +
            "END;\n" +
            "p1();\n" +
            "SELECT * FROM result;" ;
    testScriptFile(SCRIPT_TEXT, args(), String.valueOf(System.currentTimeMillis()/100000));
  }

  @Test
  public void testHplSqlProcedureWithUpperUdf() throws Throwable {
    String SCRIPT_TEXT =
        "DROP TABLE IF EXISTS result;\n" +
            "CREATE TABLE result (name String);\n" +
            "CREATE PROCEDURE p1(s1 string)\n" +
            "BEGIN\n" +
            "INSERT INTO result VALUES(upper(s1));\n" +
            "END;\n" +
            "p1('abcd');\n" +
            "SELECT * FROM result;" ;
    testScriptFile(SCRIPT_TEXT, args(), "ABCD");
  }

  @Test
  public void testCastHplSqlFunction() throws Throwable {
    String SCRIPT_TEXT =
            "SELECT CAST('1' AS int);" ;
    testScriptFile(SCRIPT_TEXT, args(), "1");
  }

  private static List<String> args() {
    return Arrays.asList("-d", BeeLine.BEELINE_DEFAULT_JDBC_DRIVER,
            "-u", miniHS2.getBaseJdbcURL() + ";mode=hplsql", "-n", USER_NAME);
  }

  private void testScriptFile(String scriptText, List<String> argList, String expectedPattern)
          throws Throwable {
    testScriptFile(scriptText, argList, expectedPattern, OutStream.OUT);
  }

  private void testScriptFile(String scriptText, List<String> argList, String expectedPattern,
          TestBeeLineWithArgs.OutStream outStream) throws Throwable {
    File scriptFile = File.createTempFile(this.getClass().getSimpleName(), "temp");
    scriptFile.deleteOnExit();
    try (PrintStream os = new PrintStream(new FileOutputStream(scriptFile))) {
      os.print(scriptText);
    }
    List<String> copy = new ArrayList<>(argList);
    copy.add("-f");
    copy.add(scriptFile.getAbsolutePath());
    String output = testCommandLineScript(copy, null, outStream);
    if (scriptText.equals("SELECT UNIX_TIMESTAMP()")) {
      Pattern pattern = Pattern.compile("\\|\\s*(\\d+)\\s*\\|");
      Matcher matcher = pattern.matcher(output);
      long expected = Long.parseLong(expectedPattern), actual = 0;
      if (matcher.find()) {
        actual = Long.parseLong(matcher.group(1));
        if (Math.abs(actual - expected) > 5) {
          output = String.valueOf(actual);
        } else {
          output = expectedPattern;
        }
      }
    }
    if (!Pattern.compile(".*" + expectedPattern + ".*", Pattern.DOTALL).matcher(output).matches()) {
      fail("Output: '" + output + "' should match " + expectedPattern);
    }
    scriptFile.delete();
  }
}
