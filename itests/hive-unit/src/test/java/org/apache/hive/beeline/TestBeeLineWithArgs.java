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

package org.apache.hive.beeline;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.server.HiveServer2;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * TestBeeLineWithArgs - executes tests of the command-line arguments to BeeLine
 *
 */
//public class TestBeeLineWithArgs extends TestCase {
public class TestBeeLineWithArgs {
  // Default location of HiveServer2
  final private static String JDBC_URL = BeeLine.BEELINE_DEFAULT_JDBC_URL + "localhost:10000";
  private static final String tableName = "TestBeelineTable1";
  private static final String tableComment = "Test table comment";


  private static HiveServer2 hiveServer2;

  private List<String> getBaseArgs(String jdbcUrl) {
    List<String> argList = new ArrayList<String>(8);
    argList.add("-d");
    argList.add(BeeLine.BEELINE_DEFAULT_JDBC_DRIVER);
    argList.add("-u");
    argList.add(jdbcUrl);
    return argList;
  }
  /**
   * Start up a local Hive Server 2 for these tests
   */
  @BeforeClass
  public static void preTests() throws Exception {
    HiveConf hiveConf = new HiveConf();
    //  hiveConf.logVars(System.err);
    // System.err.flush();

    hiveServer2 = new HiveServer2();
    hiveServer2.init(hiveConf);
    System.err.println("Starting HiveServer2...");
    hiveServer2.start();
    Thread.sleep(1000);
    createTable();

  }

  /**
   * Create table for use by tests
   * @throws ClassNotFoundException
   * @throws SQLException
   */
  private static void createTable() throws ClassNotFoundException, SQLException {
    Class.forName(BeeLine.BEELINE_DEFAULT_JDBC_DRIVER);
    Connection con = DriverManager.getConnection(JDBC_URL,"", "");

    assertNotNull("Connection is null", con);
    assertFalse("Connection should not be closed", con.isClosed());
    Statement stmt = con.createStatement();
    assertNotNull("Statement is null", stmt);

    stmt.execute("set hive.support.concurrency = false");

    HiveConf conf = new HiveConf();
    String dataFileDir = conf.get("test.data.files").replace('\\', '/')
        .replace("c:", "");
    Path dataFilePath = new Path(dataFileDir, "kv1.txt");
    // drop table. ignore error.
    try {
      stmt.execute("drop table " + tableName);
    } catch (Exception ex) {
      fail(ex.toString());
    }

    // create table
    stmt.execute("create table " + tableName
        + " (under_col int comment 'the under column', value string) comment '"
        + tableComment + "'");

    // load data
    stmt.execute("load data local inpath '"
        + dataFilePath.toString() + "' into table " + tableName);
  }

  /**
   * Shut down a local Hive Server 2 for these tests
   */
  @AfterClass
  public static void postTests() {
    try {
      if (hiveServer2 != null) {
        System.err.println("Stopping HiveServer2...");
        hiveServer2.stop();
      }
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  /**
   * Execute a script with "beeline -f"
   * @param scriptFileName The name of the script to execute
   * @throws Any exception while executing
   * @return The stderr and stdout from running the script
   */
  private String testCommandLineScript(List<String> argList) throws Throwable {
    BeeLine beeLine = new BeeLine();
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PrintStream beelineOutputStream = new PrintStream(os);
    beeLine.setOutputStream(beelineOutputStream);
    beeLine.setErrorStream(beelineOutputStream);
    String[] args = argList.toArray(new String[argList.size()]);
    beeLine.begin(args, null);
    String output = os.toString("UTF8");

    return output;
  }

  /**
   * Attempt to execute a simple script file with the -f option to BeeLine
   * Test for presence of an expected pattern
   * in the output (stdout or stderr), fail if not found
   * Print PASSED or FAILED
   * @paramm testName Name of test to print
   * @param expectedPattern Text to look for in command output/error
   * @param shouldMatch true if the pattern should be found, false if it should not
   * @throws Exception on command execution error
   */
  private void testScriptFile(String testName, String scriptText, String expectedPattern,
      boolean shouldMatch, List<String> argList) throws Throwable {

    long startTime = System.currentTimeMillis();
    System.out.println(">>> STARTED " + testName);

    // Put the script content in a temp file
    File scriptFile = File.createTempFile(testName, "temp");
    scriptFile.deleteOnExit();
    PrintStream os = new PrintStream(new FileOutputStream(scriptFile));
    os.print(scriptText);
    os.close();

    argList.add("-f");
    argList.add(scriptFile.getAbsolutePath());

    String output = testCommandLineScript(argList);
    boolean matches = output.contains(expectedPattern);
    if (shouldMatch != matches) {
      //failed
      fail(testName + ": Output" + output + " should" +  (shouldMatch ? "" : " not") +
          " contain " + expectedPattern);
    }
    scriptFile.delete();
  }

  /**
   * Attempt to execute a simple script file with the -f option to BeeLine
   * Test for presence of an expected pattern
   * in the output (stdout or stderr), fail if not found
   * Print PASSED or FAILED
   */
  @Test
  public void testPositiveScriptFile() throws Throwable {
    final String TEST_NAME = "testPositiveScriptFile";
    final String SCRIPT_TEXT = "show databases;\n";
    final String EXPECTED_PATTERN = " default ";
    List<String> argList = getBaseArgs(JDBC_URL);
    testScriptFile(TEST_NAME, SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
  }

  /**
   * Test Beeline -hivevar option. User can specify --hivevar name=value on Beeline command line.
   * In the script, user should be able to use it in the form of ${name}, which will be substituted with
   * the value.
   * @throws Throwable
   */
  @Test
  public void testBeelineHiveVariable() throws Throwable {
    List<String> argList = getBaseArgs(JDBC_URL);
    argList.add("--hivevar");
    argList.add("DUMMY_TBL=dummy");
    final String TEST_NAME = "testHiveCommandLineHiveVariable";
    final String SCRIPT_TEXT = "create table ${DUMMY_TBL} (d int);\nshow tables;\n";
    final String EXPECTED_PATTERN = "dummy";
    testScriptFile(TEST_NAME, SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
  }

  /**
   * Test Beeline -hivevar option. User can specify --hivevar name=value on Beeline command line.
   * This test defines multiple variables using repeated --hivevar flags.
   * @throws Throwable
   */
  @Test
  public void testBeelineMultiHiveVariable() throws Throwable {
    List<String> argList = getBaseArgs(JDBC_URL);
    argList.add("--hivevar");
    argList.add("TABLE_NAME=dummy2");
    argList.add("--hivevar");
    argList.add("COMMAND=create");
    argList.add("--hivevar");
    argList.add("OBJECT=table");

    final String TEST_NAME = "testHiveCommandLineHiveVariable";
    final String SCRIPT_TEXT = "${COMMAND} ${OBJECT} ${TABLE_NAME} (d int);\nshow tables;\n";
    final String EXPECTED_PATTERN = "dummy2";
    testScriptFile(TEST_NAME, SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
  }

  /**
   * Attempt to execute a simple script file with the -f option to BeeLine
   * The first command should fail and the second command should not execute
   * Print PASSED or FAILED
   */
  @Test
  public void testBreakOnErrorScriptFile() throws Throwable {
    List<String> argList = getBaseArgs(JDBC_URL);
    final String TEST_NAME = "testBreakOnErrorScriptFile";
    final String SCRIPT_TEXT = "select * from abcdefg01;\nshow databases;\n";
    final String EXPECTED_PATTERN = " default ";
    testScriptFile(TEST_NAME, SCRIPT_TEXT, EXPECTED_PATTERN, false, argList);
  }


  /**
   * Select null from table , check how null is printed
   * Print PASSED or FAILED
   */
  @Test
  public void testNullDefault() throws Throwable {
    final String TEST_NAME = "testNullDefault";
    final String SCRIPT_TEXT = "set hive.support.concurrency = false;\n" +
        "select null from " + tableName + " limit 1 ;\n";
    final String EXPECTED_PATTERN = "NULL";
    testScriptFile(TEST_NAME, SCRIPT_TEXT, EXPECTED_PATTERN, true, getBaseArgs(JDBC_URL));
  }

  /**
   * Select null from table , check if default null is printed differently
   * Print PASSED or FAILED
   */
  @Test
  public void testNullNonEmpty() throws Throwable {
    final String TEST_NAME = "testNullNonDefault";
    final String SCRIPT_TEXT = "set hive.support.concurrency = false;\n" +
        "!set nullemptystring false\n select null from " + tableName + " limit 1 ;\n";
    final String EXPECTED_PATTERN = "NULL";
    testScriptFile(TEST_NAME, SCRIPT_TEXT, EXPECTED_PATTERN, true, getBaseArgs(JDBC_URL));
  }

  @Test
  public void testGetVariableValue() throws Throwable {
    final String TEST_NAME = "testGetVariableValue";
    final String SCRIPT_TEXT = "set env:TERM;";
    final String EXPECTED_PATTERN = "env:TERM";
    testScriptFile(TEST_NAME, SCRIPT_TEXT, EXPECTED_PATTERN, true, getBaseArgs(JDBC_URL));
  }

  /**
   * Select null from table , check if setting null to empty string works.
   * Original beeline/sqlline used to print nulls as empty strings
   * Print PASSED or FAILED
   */
  @Test
  public void testNullEmpty() throws Throwable {
    final String TEST_NAME = "testNullNonDefault";
    final String SCRIPT_TEXT = "set hive.support.concurrency = false;\n" +
                "!set nullemptystring true\n select 'abc',null,'def' from " + tableName + " limit 1 ;\n";
    final String EXPECTED_PATTERN = "'abc','','def'";

    List<String> argList = getBaseArgs(JDBC_URL);
    argList.add("--outputformat=csv");

    testScriptFile(TEST_NAME, SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
  }

  /**
   * Select null from table , check if setting null to empty string works - Using beeling cmd line
   *  argument.
   * Original beeline/sqlline used to print nulls as empty strings
   * Print PASSED or FAILED
   */
  @Test
  public void testNullEmptyCmdArg() throws Throwable {
    final String TEST_NAME = "testNullNonDefault";
    final String SCRIPT_TEXT = "set hive.support.concurrency = false;\n" +
                "select 'abc',null,'def' from " + tableName + " limit 1 ;\n";
    //final String EXPECTED_PATTERN = "| abc  |      | def  |";
    final String EXPECTED_PATTERN = "'abc','','def'";

    List<String> argList = getBaseArgs(JDBC_URL);
    argList.add("--nullemptystring=true");
    argList.add("--outputformat=csv");

    testScriptFile(TEST_NAME, SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
  }

  /**
   * Attempt to execute a missing script file with the -f option to BeeLine
   * Print PASSED or FAILED
   */
  @Test
  public void testNegativeScriptFile() throws Throwable {
    final String TEST_NAME = "testNegativeScriptFile";
    final String EXPECTED_PATTERN = " default ";

    long startTime = System.currentTimeMillis();
    System.out.println(">>> STARTED " + TEST_NAME);

    // Create and delete a temp file
    File scriptFile = File.createTempFile("beelinenegative", "temp");
    scriptFile.delete();

    List<String> argList = getBaseArgs(JDBC_URL);
    argList.add("-f");
    argList.add(scriptFile.getAbsolutePath());

    try {
        String output = testCommandLineScript(argList);
      long elapsedTime = (System.currentTimeMillis() - startTime)/1000;
      String time = "(" + elapsedTime + "s)";
      if (output.contains(EXPECTED_PATTERN)) {
        System.err.println("Output: " + output);
        System.err.println(">>> FAILED " + TEST_NAME + " (ERROR) " + time);
        fail(TEST_NAME);
      } else {
        System.out.println(">>> PASSED " + TEST_NAME + " " + time);
      }
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  /**
   * HIVE-4566
   * @throws UnsupportedEncodingException
   */
  @Test
  public void testNPE() throws UnsupportedEncodingException {
    BeeLine beeLine = new BeeLine();

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PrintStream beelineOutputStream = new PrintStream(os);
    beeLine.setOutputStream(beelineOutputStream);
    beeLine.setErrorStream(beelineOutputStream);

    beeLine.runCommands( new String[] {"!typeinfo"} );
    String output = os.toString("UTF8");
    Assert.assertFalse( output.contains("java.lang.NullPointerException") );
    Assert.assertTrue( output.contains("No current connection") );

    beeLine.runCommands( new String[] {"!nativesql"} );
    output = os.toString("UTF8");
    Assert.assertFalse( output.contains("java.lang.NullPointerException") );
    Assert.assertTrue( output.contains("No current connection") );

    System.out.println(">>> PASSED " + "testNPE" );
  }

  @Test
  public void testHiveVarSubstitution() throws Throwable {
    List<String> argList = getBaseArgs(JDBC_URL + "#D_TBL=dummy_t");
    final String TEST_NAME = "testHiveVarSubstitution";
    final String SCRIPT_TEXT = "create table ${D_TBL} (d int);\nshow tables;\n";
    final String EXPECTED_PATTERN = "dummy_t";
    testScriptFile(TEST_NAME, SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
  }

  @Test
  public void testEmbeddedBeelineConnection() throws Throwable{
    String embeddedJdbcURL = BeeLine.BEELINE_DEFAULT_JDBC_URL+"/Default";
    List<String> argList = getBaseArgs(embeddedJdbcURL);
	  argList.add("--hivevar");
    argList.add("DUMMY_TBL=embedded_table");
    final String TEST_NAME = "testEmbeddedBeelineConnection";
    final String SCRIPT_TEXT = "create table ${DUMMY_TBL} (d int);\nshow tables;\n";
    final String EXPECTED_PATTERN = "embedded_table";
    testScriptFile(TEST_NAME, SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
  }
}
