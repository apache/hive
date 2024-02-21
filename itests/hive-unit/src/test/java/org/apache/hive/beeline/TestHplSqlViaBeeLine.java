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
import java.util.regex.Pattern;

import org.apache.hadoop.hive.UtilsForTest;
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
    HiveConf hiveConf = UtilsForTest.getHiveOnTezConfFromDir("../../data/conf/tez/");
    hiveConf.setVar(HiveConf.ConfVars.HIVE_LOCK_MANAGER,
            "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_DEFAULT_FETCH_SIZE, 10);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_OPTIMIZE_METADATA_QUERIES, false);
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
  public void testUdfBoolean() throws Throwable {
    String SCRIPT_TEXT =
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
    testScriptFile(SCRIPT_TEXT, args(), "This is false.*This is true");
  }

  @Test
  public void testUdfSmallInt() throws Throwable {
    String SCRIPT_TEXT =
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
    testScriptFile(SCRIPT_TEXT, args(), "246.*642");
  }

  @Test
  public void testUdfInt() throws Throwable {
    String SCRIPT_TEXT =
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
    testScriptFile(SCRIPT_TEXT, args(), "24690.*108642");
  }

  @Test
  public void testUdfBigInt() throws Throwable {
    String SCRIPT_TEXT =
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
    testScriptFile(SCRIPT_TEXT, args(), "246913578.*1975308642");
  }

  @Test
  public void testUdfFloat() throws Throwable {
    String SCRIPT_TEXT =
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
    testScriptFile(SCRIPT_TEXT, args(), "24691.357421875.*197530.859375");
  }

  @Test
  public void testUdfDouble() throws Throwable {
    String SCRIPT_TEXT =
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
    testScriptFile(SCRIPT_TEXT, args(), "2.4691357824E8.*1.97530864396E9");
  }

  @Test
  public void testUdfString() throws Throwable {
    String SCRIPT_TEXT =
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
    testScriptFile(SCRIPT_TEXT, args(), "Hello Alice!.*Hello Smith!");
  }

  @Test
  public void testUdfDate() throws Throwable {
    String SCRIPT_TEXT =
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
    testScriptFile(SCRIPT_TEXT, args(), "2022-11-24.*2022-12-25");
  }

  @Test
  public void testUdfTimestamp() throws Throwable {
    String SCRIPT_TEXT =
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
    testScriptFile(SCRIPT_TEXT, args(), "2022-11-24 10:20:30.*2022-12-25 06:30:30");
  }

  @Test
  public void testUdfDecimal() throws Throwable {
    String SCRIPT_TEXT =
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
    testScriptFile(SCRIPT_TEXT, args(), "370370369.94.*2962962963.36");
  }

  @Test
  public void testUdfVarchar() throws Throwable {
    String SCRIPT_TEXT =
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
    testScriptFile(SCRIPT_TEXT, args(), "Hello Sachin!.*Hello Smith!");
  }

  @Test
  public void testUdfChar() throws Throwable {
    String SCRIPT_TEXT =
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
    testScriptFile(SCRIPT_TEXT, args(), "Hello Alice!.*Hello Daya!");
  }

  @Test
  public void testUdfWhenUdfParamerAndActualParamDifferent() throws Throwable {
    String SCRIPT_TEXT =
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
    testScriptFile(SCRIPT_TEXT, args(), "Hello 12345.67!.*Hello 98765.43!");
  }

  @Test
  public void testBuiltInUdf() throws Throwable {
    String SCRIPT_TEXT = "SELECT abs(-2);\n";
    testScriptFile(SCRIPT_TEXT, args(), "2");
  }

  @Test
  public void testNestedUdfAndProcedure() throws Throwable {
    String SCRIPT_TEXT =
        "CREATE FUNCTION dbl(d int)\n" +
        "   RETURNS int\n" +
        "BEGIN\n" +
        "   RETURN d * 2;\n" +
        "END;\n" +
        "SELECT dbl(abs(-2)), abs(dbl(-2)), dbl(dbl(20));\n";
    testScriptFile(SCRIPT_TEXT, args(), "4.*4\\.0.*80");
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

  @Test
  public void testDecimalCast() throws Throwable {
    String SCRIPT_TEXT =
        "DECLARE\n" +
        "a DECIMAL(10,2);\n" +
        "BEGIN\n" +
        "SELECT CAST('10.5' AS DECIMAL(10,2)) as t INTO a;\n" +
        "print (a);\n" +
        "END;\n" +
        "/";
    testScriptFile(SCRIPT_TEXT, args(), "10.50", OutStream.ERR);
  }

  @Test
  public void testNullCast() throws Throwable {
    String SCRIPT_TEXT =
        "BEGIN\n" +
        "DECLARE a BIGINT;\n" +
        "print('started');\n" +
        "SELECT cast (null as BIGINT) as t INTO a\n" +
        "print (a);\n" +
        "print ('here');\n" +
        "end;\n" +
        "/";
    // Inverted match, output should not have NPE
    testScriptFile(SCRIPT_TEXT, args(), "^(.(?!(NullPointerException)))*$", OutStream.ERR);
  }

  @Test
  public void testACTIVITY_COUNTHplSqlFunction() throws Throwable {
    String SCRIPT_TEXT =
        "DROP TABLE IF EXISTS result;\n" +
        "CREATE TABLE result (col1 string);\n" +
        "INSERT INTO result VALUES('Alice');\n" +
        "INSERT INTO result VALUES('Bob');\n" +
        "SELECT * FROM result;\n" +
        "SELECT ACTIVITY_COUNT;";
    testScriptFile(SCRIPT_TEXT, args(), "2");
  }

  @Test
  public void testCASTHplSqlFunction1() throws Throwable {
    String SCRIPT_TEXT = "SELECT CAST('Abc' AS CHAR(1));";
    testScriptFile(SCRIPT_TEXT, args(), "A");
  }

  @Test
  public void testCASTHplSqlFunction2() throws Throwable {
    String SCRIPT_TEXT = "SELECT CAST(TIMESTAMP '2015-03-12 10:58:34.111' AS CHAR(10));";
    testScriptFile(SCRIPT_TEXT, args(), "2015-03-12");
  }

  @Test
  public void testCHARHplSqlFunction() throws Throwable {
    String SCRIPT_TEXT = "select CHAR(2023)";
    testScriptFile(SCRIPT_TEXT, args(), "2023");
  }

  @Test
  public void testCOALESCEHplSQLFunction() throws Throwable {
    String SCRIPT_TEXT = "select COALESCE(null,123,2023)";
    testScriptFile(SCRIPT_TEXT, args(), "123");
  }

  @Test
  public void testCONCATHplSQLFunction() throws Throwable {
    String SCRIPT_TEXT = "select CONCAT('a', 'b', NULL, 'c')";
    testScriptFile(SCRIPT_TEXT, args(), "abc");
  }

  @Test
  public void testCURRENTHplSQLFunction1() throws Throwable {
    String SCRIPT_TEXT = "SELECT CURRENT DATE;";
    testCurrentDate(SCRIPT_TEXT);
  }

  private void testCurrentDate(String SCRIPT_TEXT) throws Throwable {
    Date today = new Date(System.currentTimeMillis());
    testScriptFile(SCRIPT_TEXT, args(), today.toString());
  }

  @Test
  public void testCURRENTHplSQLFunction2() throws Throwable {
    String SCRIPT_TEXT = "SELECT CURRENT TIMESTAMP;";
    testCurrentTimestamp(SCRIPT_TEXT);
  }

  private void testCurrentTimestamp(String SCRIPT_TEXT) throws Throwable {
    Timestamp today = new Timestamp(System.currentTimeMillis());
    String timestamp = today.toString();
    testScriptFile(SCRIPT_TEXT, args(), timestamp.substring(0, timestamp.length() - 9));
  }

  @Test
  public void testCURRENTHplSQLFunction3() throws Throwable {
    String SCRIPT_TEXT = "SELECT CURRENT USER;";
    testScriptFile(SCRIPT_TEXT, args(), System.getProperty("user.name"));
  }

  @Test
  public void testCURRENT_DATEHplSQLFunction() throws Throwable {
    String SCRIPT_TEXT = "SELECT CURRENT_DATE;";
    testCurrentDate(SCRIPT_TEXT);
  }

  @Test
  public void testCURRENT_TIME_MILLISHplSQLFunction() throws Throwable {
    String SCRIPT_TEXT = "SELECT CURRENT_TIME_MILLIS();";
    testScriptFile(SCRIPT_TEXT, args(), String.valueOf(System.currentTimeMillis() / 100000));
  }

  @Test
  public void testCURRENT_TIMESTAMPHplSQLFunction() throws Throwable {
    String SCRIPT_TEXT = "SELECT CURRENT_TIMESTAMP;";
    testCurrentTimestamp(SCRIPT_TEXT);
  }

  @Test
  public void testCURRENT_USERHplSQLFunction() throws Throwable {
    String SCRIPT_TEXT = "SELECT CURRENT_USER;";
    testScriptFile(SCRIPT_TEXT, args(), System.getProperty("user.name"));
  }

  @Test
  public void testDATEHplSQLFunction() throws Throwable {
    String SCRIPT_TEXT = "SELECT DATE('2015-03-12');";
    testScriptFile(SCRIPT_TEXT, args(), "2015-03-12");
  }

  @Test
  public void testDECODEHplSQLFunction() throws Throwable {
    String SCRIPT_TEXT = "DECLARE var1 INT DEFAULT 3;\n" + "SELECT DECODE (var1, 1, 'A', 2, 'B', 3, 'C');";
    testScriptFile(SCRIPT_TEXT, args(), "C");
  }

  @Test
  public void testFROM_UNIXTIMEHplSQLFunction() throws Throwable {
    String SCRIPT_TEXT = "SELECT from_unixtime(1447141681, 'yyyy-MM-dd');";
    testScriptFile(SCRIPT_TEXT, args(), "2015-11-");
  }

  @Test
  public void testINSTRHplSQLFunction1() throws Throwable {
    String SCRIPT_TEXT = "SELECT INSTR('abc', 'b');";
    testScriptFile(SCRIPT_TEXT, args(), "2");
  }

  @Test
  public void testINSTRHplSQLFunction2() throws Throwable {
    String SCRIPT_TEXT = "SELECT INSTR('abcabcabc', 'b', 3, 2);";
    testScriptFile(SCRIPT_TEXT, args(), "8");
  }

  @Test
  public void testLOWERHplSQLFunction() throws Throwable {
    String SCRIPT_TEXT = "SELECT LOWER('ABC');";
    testScriptFile(SCRIPT_TEXT, args(), "abc");
  }

  @Test
  public void testLENHplSQLFunction() throws Throwable {
    String SCRIPT_TEXT = "SELECT LEN('Abc ');";
    testScriptFile(SCRIPT_TEXT, args(), "3");
  }

  @Test
  public void testLENGTHHplSQLFunction() throws Throwable {
    String SCRIPT_TEXT = "SELECT LENGTH('Abc ');";
    testScriptFile(SCRIPT_TEXT, args(), "4");
  }

  @Test
  public void testMODHplSQLFunction() throws Throwable {
    String SCRIPT_TEXT = "SELECT MOD(5,2);";
    testScriptFile(SCRIPT_TEXT, args(), "1");
  }

  @Test
  public void testNOWHplSQLFunction() throws Throwable {
    String SCRIPT_TEXT = "SELECT NOW();";
    testCurrentTimestamp(SCRIPT_TEXT);
  }

  @Test
  public void testNVLHplSQLFunction() throws Throwable {
    String SCRIPT_TEXT = "SELECT NVL(NULL, 100);";
    testScriptFile(SCRIPT_TEXT, args(), "100");
  }

  @Test
  public void testNVL2HplSQLFunction() throws Throwable {
    String SCRIPT_TEXT = "SELECT NVL2(NULL, 100, 200);";
    testScriptFile(SCRIPT_TEXT, args(), "200");
  }

  @Test
  public void testREPLACEHplSQLFunction() throws Throwable {
    String SCRIPT_TEXT = "SELECT replace('2016-03-03', '-', '');";
    testScriptFile(SCRIPT_TEXT, args(), "20160303");
  }

  @Test
  public void testSUBSTRHplSQLFunction1() throws Throwable {
    String SCRIPT_TEXT = "SELECT SUBSTR('Remark', 3);";
    testScriptFile(SCRIPT_TEXT, args(), "mark");
  }

  @Test
  public void testSUBSTRHplSQLFunction2() throws Throwable {
    String SCRIPT_TEXT = "SELECT SUBSTR('Remark', 3, 3);";
    testScriptFile(SCRIPT_TEXT, args(), "mar");
  }

  @Test
  public void testSUBSTRINGHplSQLFunction1() throws Throwable {
    String SCRIPT_TEXT = "SELECT SUBSTRING('Remark', 3);";
    testScriptFile(SCRIPT_TEXT, args(), "mark");
  }

  @Test
  public void testSUBSTRINGHplSQLFunction2() throws Throwable {
    String SCRIPT_TEXT = "SELECT SUBSTRING('Remark', 3, 3);";
    testScriptFile(SCRIPT_TEXT, args(), "mar");
  }

  @Test
  public void testSYSDATEHplSQLFunction() throws Throwable {
    String SCRIPT_TEXT = "SELECT SYSDATE;";
    testCurrentTimestamp(SCRIPT_TEXT);
  }

  @Test
  public void testTIMESTAMP_ISOHplSQLFunction() throws Throwable {
    String SCRIPT_TEXT = "SELECT TIMESTAMP_ISO('2015-03-12');";
    testScriptFile(SCRIPT_TEXT, args(), "2015-03-12 00:00:00");
  }

  @Test
  public void testTO_CHARHplSQLFunction() throws Throwable {
    String SCRIPT_TEXT = "SELECT TO_CHAR(CURRENT_DATE);";
    testCurrentDate(SCRIPT_TEXT);
  }

  @Test
  public void testTO_TIMESTAMPHplSQLFunction1() throws Throwable {
    String SCRIPT_TEXT = "SELECT TO_TIMESTAMP('2015-04-02', 'YYYY-MM-DD');";
    testScriptFile(SCRIPT_TEXT, args(), "2015-04-02 00:00:00.0");
  }

  @Test
  public void testTO_TIMESTAMPHplSQLFunction2() throws Throwable {
    String SCRIPT_TEXT = "SELECT TO_TIMESTAMP('04/02/2015', 'mm/dd/yyyy');";
    testScriptFile(SCRIPT_TEXT, args(), "2015-04-02 00:00:00.0");
  }

  @Test
  public void testTO_TIMESTAMPHplSQLFunction3() throws Throwable {
    String SCRIPT_TEXT = "SELECT TO_TIMESTAMP('2015-04-02 13:51:31', 'YYYY-MM-DD HH24:MI:SS');";
    testScriptFile(SCRIPT_TEXT, args(), "2015-04-02 13:51:31.0");
  }

  @Test
  public void testTRIMHplSQLFunction() throws Throwable {
    String SCRIPT_TEXT = "SELECT '#' || TRIM(' Hello ') || '#';";
    testScriptFile(SCRIPT_TEXT, args(), "#Hello#");
  }

  @Test
  public void testUNIX_TIMESTAMPHplSQLFunction() throws Throwable {
    String SCRIPT_TEXT = "SELECT UNIX_TIMESTAMP()";
    testScriptFile(SCRIPT_TEXT, args(), String.valueOf(System.currentTimeMillis()/10000));
  }

  @Test
  public void testUPPERHplSQLFunction() throws Throwable {
    String SCRIPT_TEXT = "SELECT UPPER('abc');";
    testScriptFile(SCRIPT_TEXT, args(), "ABC");
  }

  @Test
  public void testUSERHplSQLFunction() throws Throwable {
    String SCRIPT_TEXT = "SELECT USER;";
    testScriptFile(SCRIPT_TEXT, args(), System.getProperty("user.name"));
  }

  @Test
  public void testTableAliasInColumnName() throws Throwable {
    String SCRIPT_TEXT =
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
    testScriptFile(SCRIPT_TEXT, args(), "2023 = Hive");
  }

  private static List<String> args() {
    return Arrays.asList("-d", BeeLine.BEELINE_DEFAULT_JDBC_DRIVER,
            "-u", miniHS2.getBaseJdbcURL() + ";mode=hplsql", "-n", userName);
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
    if (!Pattern.compile(".*" + expectedPattern + ".*", Pattern.DOTALL).matcher(output).matches()) {
      fail("Output: '" + output + "' should match " + expectedPattern);
    }
    scriptFile.delete();
  }
}
