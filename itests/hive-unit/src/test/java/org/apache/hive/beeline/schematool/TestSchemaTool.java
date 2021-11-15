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

package org.apache.hive.beeline.schematool;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper.NestedScriptParser;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper.PostgresCommandParser;
import org.junit.Assert;
import org.junit.Test;

public class TestSchemaTool {

  /**
   * Test script formatting
   */
  @Test
  public void testScripts() throws Exception {
    String testScript[] = {
        "-- this is a comment",
      "DROP TABLE IF EXISTS fooTab;",
      "/*!1234 this is comment code like mysql */;",
      "CREATE TABLE fooTab(id INTEGER);",
      "DROP TABLE footab;",
      "-- ending comment"
    };
    String resultScript[] = {
      "DROP TABLE IF EXISTS fooTab",
      "/*!1234 this is comment code like mysql */",
      "CREATE TABLE fooTab(id INTEGER)",
      "DROP TABLE footab",
    };
    String expectedSQL = StringUtils.join(resultScript, System.getProperty("line.separator")) +
        System.getProperty("line.separator");
    File testScriptFile = generateTestScript(testScript);
    String flattenedSql = HiveSchemaHelper.getDbCommandParser("derby", false)
        .buildCommand(testScriptFile.getParentFile().getPath(),
            testScriptFile.getName());

    Assert.assertEquals(expectedSQL, flattenedSql);
  }

  /**
   * Test nested script formatting
   */
  @Test
  public void testNestedScriptsForDerby() throws Exception {
    String childTab1 = "childTab1";
    String childTab2 = "childTab2";
    String parentTab = "fooTab";

    String childTestScript1[] = {
      "-- this is a comment ",
      "DROP TABLE IF EXISTS " + childTab1 + ";",
      "CREATE TABLE " + childTab1 + "(id INTEGER);",
      "DROP TABLE " + childTab1 + ";"
    };
    String childTestScript2[] = {
        "-- this is a comment",
        "DROP TABLE IF EXISTS " + childTab2 + ";",
        "CREATE TABLE " + childTab2 + "(id INTEGER);",
        "-- this is also a comment",
        "DROP TABLE " + childTab2 + ";"
    };

    String parentTestScript[] = {
        " -- this is a comment",
        "DROP TABLE IF EXISTS " + parentTab + ";",
        " -- this is another comment ",
        "CREATE TABLE " + parentTab + "(id INTEGER);",
        "RUN '" + generateTestScript(childTestScript1).getName() + "';",
        "DROP TABLE " + parentTab + ";",
        "RUN '" + generateTestScript(childTestScript2).getName() + "';",
        "--ending comment ",
      };

    File testScriptFile = generateTestScript(parentTestScript);
    String flattenedSql = HiveSchemaHelper.getDbCommandParser("derby", false)
        .buildCommand(testScriptFile.getParentFile().getPath(),
            testScriptFile.getName());
    Assert.assertFalse(flattenedSql.contains("RUN"));
    Assert.assertFalse(flattenedSql.contains("comment"));
    Assert.assertTrue(flattenedSql.contains(childTab1));
    Assert.assertTrue(flattenedSql.contains(childTab2));
    Assert.assertTrue(flattenedSql.contains(parentTab));
  }

  /**
   * Test nested script formatting
   */
  @Test
  public void testNestedScriptsForMySQL() throws Exception {
    String childTab1 = "childTab1";
    String childTab2 = "childTab2";
    String parentTab = "fooTab";

    String childTestScript1[] = {
      "/* this is a comment code */",
      "DROP TABLE IF EXISTS " + childTab1 + ";",
      "CREATE TABLE " + childTab1 + "(id INTEGER);",
      "DROP TABLE " + childTab1 + ";"
    };
    String childTestScript2[] = {
        "/* this is a special exec code */;",
        "DROP TABLE IF EXISTS " + childTab2 + ";",
        "CREATE TABLE " + childTab2 + "(id INTEGER);",
        "-- this is a comment",
        "DROP TABLE " + childTab2 + ";"
    };

    String parentTestScript[] = {
        " -- this is a comment",
        "DROP TABLE IF EXISTS " + parentTab + ";",
        " /* this is special exec code */;",
        "CREATE TABLE " + parentTab + "(id INTEGER);",
        "SOURCE " + generateTestScript(childTestScript1).getName() + ";",
        "DROP TABLE " + parentTab + ";",
        "SOURCE " + generateTestScript(childTestScript2).getName() + ";",
        "--ending comment ",
      };

    File testScriptFile = generateTestScript(parentTestScript);
    String flattenedSql = HiveSchemaHelper.getDbCommandParser("mysql", false)
        .buildCommand(testScriptFile.getParentFile().getPath(),
            testScriptFile.getName());
    Assert.assertFalse(flattenedSql.contains("RUN"));
    Assert.assertFalse(flattenedSql.contains("comment"));
    Assert.assertTrue(flattenedSql.contains(childTab1));
    Assert.assertTrue(flattenedSql.contains(childTab2));
    Assert.assertTrue(flattenedSql.contains(parentTab));
  }

  /**
   * Test script formatting
   */
  @Test
  public void testScriptWithDelimiter() throws Exception {
    String testScript[] = {
        "-- this is a comment",
      "DROP TABLE IF EXISTS fooTab;",
      "DELIMITER $$",
      "/*!1234 this is comment code like mysql */$$",
      "CREATE TABLE fooTab(id INTEGER)$$",
      "CREATE PROCEDURE fooProc()",
      "SELECT * FROM fooTab;",
      "CALL barProc();",
      "END PROCEDURE$$",
      "DELIMITER ;",
      "DROP TABLE footab;",
      "-- ending comment"
    };
    String resultScript[] = {
      "DROP TABLE IF EXISTS fooTab",
      "/*!1234 this is comment code like mysql */",
      "CREATE TABLE fooTab(id INTEGER)",
      "CREATE PROCEDURE fooProc()" + " " +
      "SELECT * FROM fooTab;" + " " +
      "CALL barProc();" + " " +
      "END PROCEDURE",
      "DROP TABLE footab",
    };
    String expectedSQL = StringUtils.join(resultScript, System.getProperty("line.separator")) +
        System.getProperty("line.separator");
    File testScriptFile = generateTestScript(testScript);
    NestedScriptParser testDbParser = HiveSchemaHelper.getDbCommandParser("mysql", false);
    String flattenedSql = testDbParser.buildCommand(testScriptFile.getParentFile().getPath(),
        testScriptFile.getName());

    Assert.assertEquals(expectedSQL, flattenedSql);
  }

  /**
   * Test script formatting
   */
  @Test
  public void testScriptMultiRowComment() throws Exception {
    String testScript[] = {
        "-- this is a comment",
      "DROP TABLE IF EXISTS fooTab;",
      "DELIMITER $$",
      "/*!1234 this is comment code like mysql */$$",
      "CREATE TABLE fooTab(id INTEGER)$$",
      "DELIMITER ;",
      "/* multiline comment started ",
      " * multiline comment continue",
      " * multiline comment ended */",
      "DROP TABLE footab;",
      "-- ending comment"
    };
    String parsedScript[] = {
      "DROP TABLE IF EXISTS fooTab",
      "/*!1234 this is comment code like mysql */",
      "CREATE TABLE fooTab(id INTEGER)",
      "DROP TABLE footab",
    };

    String expectedSQL = StringUtils.join(parsedScript, System.getProperty("line.separator")) +
        System.getProperty("line.separator");
    File testScriptFile = generateTestScript(testScript);
    NestedScriptParser testDbParser = HiveSchemaHelper.getDbCommandParser("mysql", false);
    String flattenedSql = testDbParser.buildCommand(testScriptFile.getParentFile().getPath(),
        testScriptFile.getName());

    Assert.assertEquals(expectedSQL, flattenedSql);
  }

  /**
   * Test nested script formatting
   */
  @Test
  public void testNestedScriptsForOracle() throws Exception {
    String childTab1 = "childTab1";
    String childTab2 = "childTab2";
    String parentTab = "fooTab";

    String childTestScript1[] = {
      "-- this is a comment ",
      "DROP TABLE IF EXISTS " + childTab1 + ";",
      "CREATE TABLE " + childTab1 + "(id INTEGER);",
      "DROP TABLE " + childTab1 + ";"
    };
    String childTestScript2[] = {
        "-- this is a comment",
        "DROP TABLE IF EXISTS " + childTab2 + ";",
        "CREATE TABLE " + childTab2 + "(id INTEGER);",
        "-- this is also a comment",
        "DROP TABLE " + childTab2 + ";"
    };

    String parentTestScript[] = {
        " -- this is a comment",
        "DROP TABLE IF EXISTS " + parentTab + ";",
        " -- this is another comment ",
        "CREATE TABLE " + parentTab + "(id INTEGER);",
        "@" + generateTestScript(childTestScript1).getName() + ";",
        "DROP TABLE " + parentTab + ";",
        "@" + generateTestScript(childTestScript2).getName() + ";",
        "--ending comment ",
      };

    File testScriptFile = generateTestScript(parentTestScript);
    String flattenedSql = HiveSchemaHelper.getDbCommandParser("oracle", false)
        .buildCommand(testScriptFile.getParentFile().getPath(),
            testScriptFile.getName());
    Assert.assertFalse(flattenedSql.contains("@"));
    Assert.assertFalse(flattenedSql.contains("comment"));
    Assert.assertTrue(flattenedSql.contains(childTab1));
    Assert.assertTrue(flattenedSql.contains(childTab2));
    Assert.assertTrue(flattenedSql.contains(parentTab));
  }

  /**
   * Test script formatting
   */
  @Test
  public void testPostgresFilter() throws Exception {
    String testScript[] = {
        "-- this is a comment",
        "DROP TABLE IF EXISTS fooTab;",
        HiveSchemaHelper.PostgresCommandParser.POSTGRES_STANDARD_STRINGS_OPT + ";",
        "CREATE TABLE fooTab(id INTEGER);",
        "DROP TABLE footab;",
        "-- ending comment"
    };

    String expectedScriptWithOptionPresent[] = {
        "DROP TABLE IF EXISTS fooTab",
        HiveSchemaHelper.PostgresCommandParser.POSTGRES_STANDARD_STRINGS_OPT,
        "CREATE TABLE fooTab(id INTEGER)",
        "DROP TABLE footab",
    };

    NestedScriptParser noDbOptParser = HiveSchemaHelper
        .getDbCommandParser("postgres", false);
    String expectedSQL = StringUtils.join(
        expectedScriptWithOptionPresent, System.getProperty("line.separator")) +
            System.getProperty("line.separator");
    File testScriptFile = generateTestScript(testScript);
    String flattenedSql = noDbOptParser.buildCommand(
        testScriptFile.getParentFile().getPath(), testScriptFile.getName());
    Assert.assertEquals(expectedSQL, flattenedSql);

    String expectedScriptWithOptionAbsent[] = {
        "DROP TABLE IF EXISTS fooTab",
        "CREATE TABLE fooTab(id INTEGER)",
        "DROP TABLE footab",
    };

    NestedScriptParser dbOptParser = HiveSchemaHelper.getDbCommandParser(
        "postgres",
        PostgresCommandParser.POSTGRES_SKIP_STANDARD_STRINGS_DBOPT,
        null, null, null, null, false);
    expectedSQL = StringUtils.join(
        expectedScriptWithOptionAbsent, System.getProperty("line.separator")) +
            System.getProperty("line.separator");
    testScriptFile = generateTestScript(testScript);
    flattenedSql = dbOptParser.buildCommand(
        testScriptFile.getParentFile().getPath(), testScriptFile.getName());
    Assert.assertEquals(expectedSQL, flattenedSql);
  }

  private File generateTestScript(String [] stmts) throws IOException {
    File testScriptFile = File.createTempFile("schematest", ".sql");
    testScriptFile.deleteOnExit();
    FileWriter fstream = new FileWriter(testScriptFile.getPath());
    BufferedWriter out = new BufferedWriter(fstream);
    for (String line: stmts) {
      out.write(line);
      out.newLine();
    }
    out.close();
    return testScriptFile;
  }
}
