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
package org.apache.hive.beeline;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestBeelinePasswordOption {
  private static final Logger LOG = LoggerFactory.getLogger(TestBeelinePasswordOption.class);
  private static final String tableName = "TestBeelineTable1";
  private static final String tableComment = "Test table comment";
  private static MiniHS2 miniHS2;

  /**
   * Start up a local Hive Server 2 for these tests
   */
  @BeforeClass
  public static void preTests() throws Exception {
    HiveConf hiveConf = new HiveConf();
    // Set to non-zk lock manager to prevent HS2 from trying to connect
    hiveConf.setVar(HiveConf.ConfVars.HIVE_LOCK_MANAGER,
        "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
    miniHS2 = new MiniHS2(hiveConf);
    miniHS2.start(new HashMap<String, String>());
    createTable();
  }

  /**
   * Test if beeline prompts for a password when optional password option is at the beginning of
   * arguments
   */
  @Test
  public void testPromptPasswordOptionAsFirst() throws Throwable {
    List<String> argList = new ArrayList<>();
    argList.add("-p");
    argList.addAll(getBaseArgs(miniHS2.getBaseJdbcURL()));
    argList.add("-n");
    argList.add("hive");
    connectBeelineWithUserPrompt(argList, "hivepassword");
  }

  /**
   * Test if beeline prompts for a password when optional password option is at the end of arguments
   */
  @Test
  public void testPromptPasswordOptionLast() throws Exception {
    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    argList.add("-n");
    argList.add("hive");
    argList.add("-p");
    connectBeelineWithUserPrompt(argList, "hivepassword");
  }

  /**
   * Test if beeline prompts for a password when optional password option is at the middle of
   * arguments
   */
  @Test
  public void testPromptPasswordOptionMiddle() throws Exception {
    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    argList.add("-p");
    argList.add("-n");
    argList.add("hive");
    connectBeelineWithUserPrompt(argList, "hivepassword");
  }

  /**
   * Test if beeline prompts for a password when optional password option is used in conjunction
   * with additional commandLine options after -p
   */
  @Test
  public void testPromptPasswordOptionWithOtherOptions() throws Exception {
    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    argList.add("-p");
    argList.add("-n");
    argList.add("hive");
    argList.add("-e");
    argList.add("show tables;");
    String output = connectBeelineWithUserPrompt(argList, "hivepassword");
    Assert.assertTrue("Table name " + tableName + " not found in the output",
        output.contains(tableName.toLowerCase()));
  }

  /**
   * Test if beeline prompts for a password when optional password option is used in conjunction
   * with additional BeeLineOpts options after -p
   */
  @Test
  public void testPromptPasswordOptionWithBeelineOpts() throws Exception {
    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    argList.add("-n");
    argList.add("hive");
    argList.add("-p");
    argList.add("--force=true");
    argList.add("-e");
    argList.add("show tables;");
    String output = connectBeelineWithUserPrompt(argList, "hivepassword");
    Assert.assertTrue("Table name " + tableName + " not found in the output",
        output.contains(tableName.toLowerCase()));
  }

  /**
   * Test if beeline prompts for a password when optional password option is used in conjunction
   * with additional BeeLineOpts options after -p. Also, verifies the beelineOpt value is set as
   * expected
   */
  @Test
  public void testPromptPasswordVerifyBeelineOpts() throws Exception {
    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    argList.add("-n");
    argList.add("hive");
    argList.add("-p");
    argList.add("--maxColumnWidth=57");
    argList.add("-e");
    argList.add("show tables;");
    String output = connectWithPromptAndVerify(argList, "hivepassword", true, 57, null, null);
    Assert.assertTrue("Table name " + tableName + " not found in the output",
        output.contains(tableName.toLowerCase()));
  }

  /**
   * Tests if beeline prompts for a password and also confirms that --hiveconf
   * argument works when given immediately after -p with no password
   * @throws Exception
   */
  @Test
  public void testPromptPasswordWithHiveConf() throws Exception {
    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    argList.add("-n");
    argList.add("hive");
    argList.add("-p");
    argList.add("--hiveconf");
    argList.add("hive.cli.print.header=true");
    argList.add("-e");
    argList.add("show tables;");
    String output = connectWithPromptAndVerify(argList, "hivepassword", false, null,
        "hive.cli.print.header", "true");
    Assert.assertTrue("Table name " + tableName + " not found in the output",
        output.contains(tableName.toLowerCase()));
  }

  /**
   * Tests if beeline doesn't prompt for a password and connects with empty password
   * when no password option provided
   */
  @Test
  public void testNoPasswordPrompt() throws Exception {
    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    argList.add("-n");
    argList.add("hive");
    argList.add("--force=true");
    argList.add("-e");
    argList.add("show tables;");
    String output = connectBeelineWithUserPrompt(argList);
    Assert.assertTrue("Table name " + tableName + " not found in the output",
        output.contains(tableName.toLowerCase()));
  }

  /**
   * Tests if beeline doesn't prompt for a password and connects with no password/username option
   * provided
   */
  @Test
  public void testNoPasswordPrompt2() throws Exception {
    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    argList.add("--force=true");
    argList.add("-e");
    argList.add("show tables;");
    String output = connectBeelineWithUserPrompt(argList);
    Assert.assertTrue("Table name " + tableName + " not found in the output",
        output.contains(tableName.toLowerCase()));
  }

  /**
   * Tests if Beeline prompts for password when -p is the last argument and argList has CommandLine
   * options as well as BeelineOpts
   */
  @Test
  public void testPromptPassOptionLastWithBeelineOpts() throws Exception {
    List<String> argList = getBaseArgs(miniHS2.getBaseJdbcURL());
    argList.add("-n");
    argList.add("hive");
    argList.add("--force=true");
    argList.add("-e");
    argList.add("show tables;");
    argList.add("-p");
    String output = connectBeelineWithUserPrompt(argList, "hivepassword");
    Assert.assertTrue("Table name " + tableName + " not found in the output",
        output.contains(tableName.toLowerCase()));
  }

  /**
   * Connects to miniHS2 using beeline with the given string value for the prompt if the prompt is
   * null, uses beeline with null inputstream in which this method expects that the argList is
   * sufficient to make a successful Beeline connection with no prompt required from user
   *
   * @param argList - arguments list for the beeline
   * @param prompt - String value to be given to beeline prompt during connection
   * @param beelineOptName - Name of BeelineOpt to be verified
   * @param beelineOptValue - Expected value of value of BeeLineOpt
   * @param hiveConfKey - hive conf variable name to verify
   * @param expectedHiveConfValue - Expected value of hive conf variable
   * @return output of beeline from outputstream
   * @throws Exception
   */
  private String connectWithPromptAndVerify(List<String> argList, String prompt,
    boolean testMaxColumnWidthOption, Integer expectedMaxColumnWidth, String hiveConfKey,
    String expectedHiveConfValue) throws Exception {
    BeeLine beeLine = null;
    InputStream inputStream = null;
    try {
      beeLine = new BeeLine();
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      PrintStream beelineOutputStream = new PrintStream(os);
      beeLine.setOutputStream(beelineOutputStream);
      beeLine.setErrorStream(beelineOutputStream);
      String[] args = argList.toArray(new String[argList.size()]);
      if (prompt != null) {
        inputStream = new ByteArrayInputStream(prompt.getBytes());
      }
      Assert.assertTrue(beeLine.begin(args, inputStream) == 0);
      if (testMaxColumnWidthOption) {
        int maxColumnWidth = beeLine.getOpts().getMaxColumnWidth();
        Assert.assertTrue(
          "Expected max columnWidth to be " + expectedMaxColumnWidth + " found " + maxColumnWidth,
          maxColumnWidth == expectedMaxColumnWidth);
      }
      if (hiveConfKey != null) {
        String hiveConfValue = beeLine.getOpts().getHiveConfVariables().get(hiveConfKey);
        Assert.assertTrue(
          "Expected " + expectedHiveConfValue + " got " + hiveConfValue + " for " + hiveConfKey,
          expectedHiveConfValue.equalsIgnoreCase(hiveConfValue));
      }
      String output = os.toString("UTF-8");
      LOG.debug(output);
      return output;
    } finally {
      if (beeLine != null) {
        beeLine.close();
      }
      if(inputStream != null) {
        inputStream.close();
      }
    }
  }

  private String connectBeelineWithUserPrompt(List<String> argList) throws Exception {
    return connectBeelineWithUserPrompt(argList, null);
  }

  private String connectBeelineWithUserPrompt(List<String> argList, String prompt)
      throws Exception {
    return connectWithPromptAndVerify(argList, prompt, false, null, null, null);
  }

  /**
   * Create table for use by tests
   *
   * @throws ClassNotFoundException
   * @throws SQLException
   */
  private static void createTable() throws ClassNotFoundException, SQLException {
    Class.forName(BeeLine.BEELINE_DEFAULT_JDBC_DRIVER);
    Connection con = DriverManager.getConnection(miniHS2.getBaseJdbcURL(), "", "");

    assertNotNull("Connection is null", con);
    assertFalse("Connection should not be closed", con.isClosed());
    Statement stmt = con.createStatement();
    assertNotNull("Statement is null", stmt);

    stmt.execute("set hive.support.concurrency = false");
    try {
      stmt.execute("drop table if exists " + tableName);
    } catch (Exception ex) {
      LOG.error("Failed due to exception ", ex);
      fail("Unable to create setup table " + tableName + ex.toString());
    }
    // create table
    stmt.execute("create table " + tableName
        + " (under_col int comment 'the under column', value string) comment '" + tableComment
        + "'");
  }

  private List<String> getBaseArgs(String jdbcUrl) {
    List<String> argList = new ArrayList<String>(8);
    argList.add("-d");
    argList.add(BeeLine.BEELINE_DEFAULT_JDBC_DRIVER);
    argList.add("-u");
    argList.add(jdbcUrl);
    return argList;
  }
}
