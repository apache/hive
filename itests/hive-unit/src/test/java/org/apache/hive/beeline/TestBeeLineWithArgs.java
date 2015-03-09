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
import java.io.InputStream;
import java.io.PrintStream;
import java.io.StringBufferInputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * TestBeeLineWithArgs - executes tests of the command-line arguments to BeeLine
 *
 */
public class TestBeeLineWithArgs {
  // Default location of HiveServer2
  private static final String tableName = "TestBeelineTable1";
  private static final String tableComment = "Test table comment";

  private static MiniHS2 miniHS2;

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
    // Set to non-zk lock manager to prevent HS2 from trying to connect
    hiveConf.setVar(HiveConf.ConfVars.HIVE_LOCK_MANAGER, "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
    miniHS2 = new MiniHS2(hiveConf);
    miniHS2.start(new HashMap<String,  String>());
    createTable();
  }

  /**
   * Create table for use by tests
   * @throws ClassNotFoundException
   * @throws SQLException
   */
  private static void createTable() throws ClassNotFoundException, SQLException {
    Class.forName(BeeLine.BEELINE_DEFAULT_JDBC_DRIVER);
    Connection con = DriverManager.getConnection(miniHS2.getBaseJdbcURL(),"", "");

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
    if (miniHS2.isStarted()) {
      miniHS2.stop();
    }
  }

  /**
   * Execute a script with "beeline -f or -i"
   *
   * @return The stderr and stdout from running the script
   */
  private String testCommandLineScript(List<String> argList, InputStream inputStream)
      throws Throwable {
    BeeLine beeLine = new BeeLine();
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PrintStream beelineOutputStream = new PrintStream(os);
    beeLine.setOutputStream(beelineOutputStream);
    beeLine.setErrorStream(beelineOutputStream);
    String[] args = argList.toArray(new String[argList.size()]);
    beeLine.begin(args, inputStream);
    String output = os.toString("UTF8");

    beeLine.close();
    return output;
  }

  /**
   * Attempt to execute a simple script file with the -f option to BeeLine
   * Test for presence of an expected pattern
   * in the output (stdout or stderr), fail if not found
   * Print PASSED or FAILED
   * @param expectedPattern Text to look for in command output/error
   * @param shouldMatch true if the pattern should be found, false if it should not
   * @throws Exception on command execution error
   */
  private void testScriptFile(String scriptText, String expectedPattern,
      boolean shouldMatch, List<String> argList) throws Throwable {

    // Put the script content in a temp file
    File scriptFile = File.createTempFile(this.getClass().getSimpleName(), "temp");
    scriptFile.deleteOnExit();
    PrintStream os = new PrintStream(new FileOutputStream(scriptFile));
    os.print(scriptText);
    os.close();

    {
      List<String> copy = new ArrayList<String>(argList);
      copy.add("-f");
      copy.add(scriptFile.getAbsolutePath());

      String output = testCommandLineScript(copy, null);
      boolean matches = output.contains(expectedPattern);
      if (shouldMatch != matches) {
        //failed
        fail("Output" + output + " should" +  (shouldMatch ? "" : " not") +
            " contain " + expectedPattern);
      }
    }

    {
      List<String> copy = new ArrayList<String>(argList);
      copy.add("-i");
      copy.add(scriptFile.getAbsolutePath());

      String output = testCommandLineScript(copy, new StringBufferInputStream("!quit\n"));
      boolean matches = output.contains(expectedPattern);
      if (shouldMatch != matches) {
        //failed
        fail("Output" + output + " should" +  (shouldMatch ? "" : " not") +
            " contain " + expectedPattern);
      }
    }
    scriptFile.delete();
  }

  /**
   * Test that BeeLine will read comment lines that start with whitespace
   * @throws Throwable
   */
  @Test
  public void testWhitespaceBeforeCommentScriptFile() throws Throwable {
    final String SCRIPT_TEXT = " 	 	-- comment has spaces and tabs before it\n 	 	# comment has spaces and tabs before it\n";
    final String EXPECTED_PATTERN = "cannot recognize input near '<EOF>'";
    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    testScriptFile(SCRIPT_TEXT, EXPECTED_PATTERN, false, argList);
  }

  /**
   * Attempt to execute a simple script file with the -f option to BeeLine
   * Test for presence of an expected pattern
   * in the output (stdout or stderr), fail if not found
   * Print PASSED or FAILED
   */
  @Test
  public void testPositiveScriptFile() throws Throwable {
    final String SCRIPT_TEXT = "show databases;\n";
    final String EXPECTED_PATTERN = " default ";
    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    testScriptFile( SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
  }

  /**
   * Test Beeline -hivevar option. User can specify --hivevar name=value on Beeline command line.
   * In the script, user should be able to use it in the form of ${name}, which will be substituted with
   * the value.
   * @throws Throwable
   */
  @Test
  public void testBeelineHiveVariable() throws Throwable {
    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    argList.add("--hivevar");
    argList.add("DUMMY_TBL=dummy");
    final String SCRIPT_TEXT = "create table ${DUMMY_TBL} (d int);\nshow tables;\n";
    final String EXPECTED_PATTERN = "dummy";
    testScriptFile(SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
  }

  @Test
  public void testBeelineHiveConfVariable() throws Throwable {
    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    argList.add("--hiveconf");
    argList.add("test.hive.table.name=dummy");
    final String SCRIPT_TEXT = "create table ${hiveconf:test.hive.table.name} (d int);\nshow tables;\n";
    final String EXPECTED_PATTERN = "dummy";
    testScriptFile(SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
  }

  /**
   * Test Beeline -hivevar option. User can specify --hivevar name=value on Beeline command line.
   * This test defines multiple variables using repeated --hivevar or --hiveconf flags.
   * @throws Throwable
   */
  @Test
  public void testBeelineMultiHiveVariable() throws Throwable {
    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    argList.add("--hivevar");
    argList.add("TABLE_NAME=dummy2");

    argList.add("--hiveconf");
    argList.add("COLUMN_NAME=d");

    argList.add("--hivevar");
    argList.add("COMMAND=create");
    argList.add("--hivevar");
    argList.add("OBJECT=table");

    argList.add("--hiveconf");
    argList.add("COLUMN_TYPE=int");

    final String SCRIPT_TEXT = "${COMMAND} ${OBJECT} ${TABLE_NAME} (${hiveconf:COLUMN_NAME} ${hiveconf:COLUMN_TYPE});\nshow tables;\n";
    final String EXPECTED_PATTERN = "dummy2";
    testScriptFile(SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
  }

  /**
   * Attempt to execute a simple script file with the -f option to BeeLine
   * The first command should fail and the second command should not execute
   * Print PASSED or FAILED
   */
  @Test
  public void testBreakOnErrorScriptFile() throws Throwable {
    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    final String SCRIPT_TEXT = "select * from abcdefg01;\nshow databases;\n";
    final String EXPECTED_PATTERN = " default ";
    testScriptFile(SCRIPT_TEXT, EXPECTED_PATTERN, false, argList);
  }

  @Test
  public void testBeelineShellCommand() throws Throwable {
    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    final String SCRIPT_TEXT = "!sh echo \"hello world.\" > hw.txt\n!sh cat hw.txt\n!rm hw.txt";
    final String EXPECTED_PATTERN = "hello world";
    testScriptFile(SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
  }

  /**
   * Select null from table , check how null is printed
   * Print PASSED or FAILED
   */
  @Test
  public void testNullDefault() throws Throwable {
    final String SCRIPT_TEXT = "set hive.support.concurrency = false;\n" +
        "select null from " + tableName + " limit 1 ;\n";
    final String EXPECTED_PATTERN = "NULL";
    testScriptFile(SCRIPT_TEXT, EXPECTED_PATTERN, true, getBaseArgs(miniHS2.getBaseJdbcURL()));
  }

  /**
   * Select null from table , check if default null is printed differently
   * Print PASSED or FAILED
   */
  @Test
  public void testNullNonEmpty() throws Throwable {
    final String SCRIPT_TEXT = "set hive.support.concurrency = false;\n" +
        "!set nullemptystring false\n select null from " + tableName + " limit 1 ;\n";
    final String EXPECTED_PATTERN = "NULL";
    testScriptFile(SCRIPT_TEXT, EXPECTED_PATTERN, true, getBaseArgs(miniHS2.getBaseJdbcURL()));
  }

  @Test
  public void testGetVariableValue() throws Throwable {
    final String SCRIPT_TEXT = "set env:TERM;";
    final String EXPECTED_PATTERN = "env:TERM";
    testScriptFile(SCRIPT_TEXT, EXPECTED_PATTERN, true, getBaseArgs(miniHS2.getBaseJdbcURL()));
  }

  /**
   * Select null from table , check if setting null to empty string works.
   * Original beeline/sqlline used to print nulls as empty strings.
   * Also test csv2 output format
   * Print PASSED or FAILED
   */
  @Test
  public void testNullEmpty() throws Throwable {
    final String SCRIPT_TEXT = "set hive.support.concurrency = false;\n" +
                "!set nullemptystring true\n select 'abc',null,'def' from " + tableName + " limit 1 ;\n";
    final String EXPECTED_PATTERN = "abc,,def";

    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    argList.add("--outputformat=csv2");

    testScriptFile(SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
  }

  /**
   * Test writing output using DSV format, with custom delimiter ";"
   */
  @Test
  public void testDSVOutput() throws Throwable {
    String SCRIPT_TEXT = getFormatTestQuery();
    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    argList.add("--outputformat=dsv");
    argList.add("--delimiterForDSV=;");

    final String EXPECTED_PATTERN = "1;NULL;defg;ab\"c;1.0";
    testScriptFile(SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
  }

  /**
   * Test writing output using TSV (new) format
   */
  @Test
  public void testTSV2Output() throws Throwable {
    String SCRIPT_TEXT = getFormatTestQuery();
    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    argList.add("--outputformat=tsv2");

    final String EXPECTED_PATTERN = "1\tNULL\tdefg\tab\"c\t1.0";
    testScriptFile(SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
  }

  /**
   * Test writing output using TSV deprecated format
   */
  @Test
  public void testTSVOutput() throws Throwable {
    String SCRIPT_TEXT = getFormatTestQuery();
    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    argList.add("--outputformat=tsv");

    final String EXPECTED_PATTERN = "'1'\t'NULL'\t'defg'\t'ab\"c\'\t'1.0'";
    testScriptFile(SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
  }

  /**
   * Test writing output using new TSV format
   */
  @Test
  public void testTSV2OutputWithDoubleQuotes() throws Throwable {
    String SCRIPT_TEXT = getFormatTestQueryForEableQuotes();
    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    argList.add("--outputformat=tsv2");
    System.setProperty(SeparatedValuesOutputFormat.DISABLE_QUOTING_FOR_SV,"false");

    final String EXPECTED_PATTERN = "1\tNULL\tdefg\t\"ab\"\"c\"\t\"\"\"aa\"\"\"\t1.0";
    testScriptFile(SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
    System.setProperty(SeparatedValuesOutputFormat.DISABLE_QUOTING_FOR_SV, "true");
  }

  /**
   * Test writing output using TSV deprecated format
   */
  @Test
  public void testTSVOutputWithDoubleQuotes() throws Throwable {
    String SCRIPT_TEXT = getFormatTestQueryForEableQuotes();
    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    argList.add("--outputformat=tsv");
    System.setProperty(SeparatedValuesOutputFormat.DISABLE_QUOTING_FOR_SV, "false");

    final String EXPECTED_PATTERN = "'1'\t'NULL'\t'defg'\t'ab\"c'\t'\"aa\"'\t'1.0'";
    testScriptFile(SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
    System.setProperty(SeparatedValuesOutputFormat.DISABLE_QUOTING_FOR_SV, "true");
  }

  /**
   * Test writing output using new CSV format
   */
  @Test
  public void testCSV2OutputWithDoubleQuotes() throws Throwable {
    String SCRIPT_TEXT = getFormatTestQueryForEableQuotes();
    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    argList.add("--outputformat=csv2");
    System.setProperty(SeparatedValuesOutputFormat.DISABLE_QUOTING_FOR_SV, "false");

    final String EXPECTED_PATTERN = "1,NULL,defg,\"ab\"\"c\",\"\"\"aa\"\"\",1.0";
    testScriptFile(SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
    System.setProperty(SeparatedValuesOutputFormat.DISABLE_QUOTING_FOR_SV, "true");
  }

  /**
   * Test writing output using CSV deprecated format
   */
  @Test
  public void testCSVOutputWithDoubleQuotes() throws Throwable {
    String SCRIPT_TEXT = getFormatTestQueryForEableQuotes();
    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    argList.add("--outputformat=csv");
    System.setProperty(SeparatedValuesOutputFormat.DISABLE_QUOTING_FOR_SV, "false");

    final String EXPECTED_PATTERN = "'1','NULL','defg','ab\"c','\"aa\"','1.0'";
    testScriptFile(SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
    System.setProperty(SeparatedValuesOutputFormat.DISABLE_QUOTING_FOR_SV, "true");
  }

  /**
   * Test writing output using DSV format, with custom delimiter ";"
   */
  @Test
  public void testDSVOutputWithDoubleQuotes() throws Throwable {
    String SCRIPT_TEXT = getFormatTestQueryForEableQuotes();
    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    argList.add("--outputformat=dsv");
    argList.add("--delimiterForDSV=;");
    System.setProperty(SeparatedValuesOutputFormat.DISABLE_QUOTING_FOR_SV, "false");

    final String EXPECTED_PATTERN = "1;NULL;defg;\"ab\"\"c\";\"\"\"aa\"\"\";1.0";
    testScriptFile(SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
    System.setProperty(SeparatedValuesOutputFormat.DISABLE_QUOTING_FOR_SV, "true");
  }

  /**
   * Test writing output using TSV deprecated format
   * Check for deprecation message
   */
  @Test
  public void testTSVOutputDeprecation() throws Throwable {
    String SCRIPT_TEXT = getFormatTestQuery();
    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    argList.add("--outputformat=tsv");

    final String EXPECTED_PATTERN = "Format tsv is deprecated, please use tsv2";
    testScriptFile(SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
  }

  /**
   * Test writing output using CSV deprecated format
   * Check for deprecation message
   */
  @Test
  public void testCSVOutputDeprecation() throws Throwable {
    String SCRIPT_TEXT = getFormatTestQuery();
    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    argList.add("--outputformat=csv");

    final String EXPECTED_PATTERN = "Format csv is deprecated, please use csv2";
    testScriptFile(SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
  }

  /**
   * Test writing output using CSV deprecated format
   */
  @Test
  public void testCSVOutput() throws Throwable {
    String SCRIPT_TEXT = getFormatTestQuery();
    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    argList.add("--outputformat=csv");
    final String EXPECTED_PATTERN = "'1','NULL','defg','ab\"c\','1.0'";
    testScriptFile(SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
  }


  private String getFormatTestQuery() {
    return "set hive.support.concurrency = false;\n" +
        "select 1, null, 'defg', 'ab\"c', 1.0D from " + tableName + " limit 1 ;\n";
  }

  private String getFormatTestQueryForEableQuotes() {
    return "set hive.support.concurrency = false;\n" +
        "select 1, null, 'defg', 'ab\"c', '\"aa\"', 1.0D from " + tableName + " limit 1 ;\n";
  }

  /**
   * Select null from table , check if setting null to empty string works - Using beeling cmd line
   *  argument.
   * Original beeline/sqlline used to print nulls as empty strings
   * Print PASSED or FAILED
   */
  @Test
  public void testNullEmptyCmdArg() throws Throwable {
    final String SCRIPT_TEXT = "set hive.support.concurrency = false;\n" +
                "select 'abc',null,'def' from " + tableName + " limit 1 ;\n";
    final String EXPECTED_PATTERN = "'abc','','def'";

    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    argList.add("--nullemptystring=true");
    argList.add("--outputformat=csv");

    testScriptFile(SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
  }

  /**
   * Attempt to execute a missing script file with the -f option to BeeLine
   */
  @Test
  public void testNegativeScriptFile() throws Throwable {
    final String EXPECTED_PATTERN = " default ";

    // Create and delete a temp file
    File scriptFile = File.createTempFile("beelinenegative", "temp");
    scriptFile.delete();

    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    argList.add("-f");
    argList.add(scriptFile.getAbsolutePath());

    try {
      String output = testCommandLineScript(argList, null);
      if (output.contains(EXPECTED_PATTERN)) {
        fail("Output: " + output +  " Negative pattern: " + EXPECTED_PATTERN);
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
    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL() + "#D_TBL=dummy_t");
    final String SCRIPT_TEXT = "create table ${D_TBL} (d int);\nshow tables;\n";
    final String EXPECTED_PATTERN = "dummy_t";
    testScriptFile(SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
  }

  @Test
  public void testEmbeddedBeelineConnection() throws Throwable{
    String embeddedJdbcURL = BeeLine.BEELINE_DEFAULT_JDBC_URL+"/Default";
    List<String> argList = getBaseArgs(embeddedJdbcURL);
	  argList.add("--hivevar");
    argList.add("DUMMY_TBL=embedded_table");
    // Set to non-zk lock manager to avoid trying to connect to zookeeper
    final String SCRIPT_TEXT =
        "set hive.lock.manager=org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager;\n" +
        "create table ${DUMMY_TBL} (d int);\nshow tables;\n";
    final String EXPECTED_PATTERN = "embedded_table";
    testScriptFile(SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
  }

  /**
   * Test Beeline could show the query progress for time-consuming query.
   * @throws Throwable
   */
  @Test
  public void testQueryProgress() throws Throwable {
    final String SCRIPT_TEXT = "set hive.support.concurrency = false;\n" +
        "select count(*) from " + tableName + ";\n";
    final String EXPECTED_PATTERN = "number of splits";
    testScriptFile(SCRIPT_TEXT, EXPECTED_PATTERN, true, getBaseArgs(miniHS2.getBaseJdbcURL()));
  }

  /**
   * Test Beeline could show the query progress for time-consuming query when hive.exec.parallel
   * is true
   * @throws Throwable
   */
  @Test
  public void testQueryProgressParallel() throws Throwable {
    final String SCRIPT_TEXT = "set hive.support.concurrency = false;\n" +
        "set hive.exec.parallel = true;\n" +
        "select count(*) from " + tableName + ";\n";
    final String EXPECTED_PATTERN = "number of splits";
    testScriptFile(SCRIPT_TEXT, EXPECTED_PATTERN, true, getBaseArgs(miniHS2.getBaseJdbcURL()));
  }

  /**
   * Test Beeline will hide the query progress when silent option is set.
   * @throws Throwable
   */
  @Test
  public void testQueryProgressHidden() throws Throwable {
    final String SCRIPT_TEXT = "set hive.support.concurrency = false;\n" +
        "!set silent true\n" +
        "select count(*) from " + tableName + ";\n";
    final String EXPECTED_PATTERN = "Parsing command";
    testScriptFile(SCRIPT_TEXT, EXPECTED_PATTERN, false, getBaseArgs(miniHS2.getBaseJdbcURL()));
  }
}
