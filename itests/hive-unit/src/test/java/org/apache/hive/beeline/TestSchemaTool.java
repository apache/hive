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

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.MetaStoreSchemaInfo;
import org.apache.hive.beeline.HiveSchemaHelper.NestedScriptParser;
import org.apache.hive.beeline.HiveSchemaHelper.PostgresCommandParser;

public class TestSchemaTool extends TestCase {
  private HiveSchemaTool schemaTool;
  private HiveConf hiveConf;
  private String testMetastoreDB;
  private PrintStream errStream;
  private PrintStream outStream;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    testMetastoreDB = System.getProperty("java.io.tmpdir") +
        File.separator + "test_metastore-" + new Random().nextInt();
    System.setProperty(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname,
        "jdbc:derby:" + testMetastoreDB + ";create=true");
    hiveConf = new HiveConf(this.getClass());
    schemaTool = new HiveSchemaTool(
        System.getProperty("test.tmp.dir", "target/tmp"), hiveConf, "derby");
    System.setProperty("beeLine.system.exit", "true");
    errStream = System.err;
    outStream = System.out;
  }

  @Override
  protected void tearDown() throws Exception {
    File metaStoreDir = new File(testMetastoreDB);
    if (metaStoreDir.exists()) {
      FileUtils.forceDeleteOnExit(metaStoreDir);
    }
    System.setOut(outStream);
    System.setErr(errStream);
  }

  /**
   * Test dryrun of schema initialization
   * @throws Exception
   */
  public void testSchemaInitDryRun() throws Exception {
    schemaTool.setDryRun(true);
    schemaTool.doInit("0.7.0");
    schemaTool.setDryRun(false);
    try {
      schemaTool.verifySchemaVersion();
    } catch (HiveMetaException e) {
      // The connection should fail since it the dry run
      return;
    }
    fail("Dry run shouldn't create actual metastore");
  }

  /**
   * Test dryrun of schema upgrade
   * @throws Exception
   */
  public void testSchemaUpgradeDryRun() throws Exception {
    schemaTool.doInit("0.7.0");

    schemaTool.setDryRun(true);
    schemaTool.doUpgrade("0.7.0");
    schemaTool.setDryRun(false);
    try {
      schemaTool.verifySchemaVersion();
    } catch (HiveMetaException e) {
      // The connection should fail since it the dry run
      return;
    }
    fail("Dry run shouldn't upgrade metastore schema");
  }

  /**
   * Test schema initialization
   * @throws Exception
   */
  public void testSchemaInit() throws Exception {
    schemaTool.doInit(MetaStoreSchemaInfo.getHiveSchemaVersion());
    schemaTool.verifySchemaVersion();
    }

  /**
   * Test schema upgrade
   * @throws Exception
   */
  public void testSchemaUpgrade() throws Exception {
    boolean foundException = false;
    // Initialize 0.7.0 schema
    schemaTool.doInit("0.7.0");
    // verify that driver fails due to older version schema
    try {
      schemaTool.verifySchemaVersion();
    } catch (HiveMetaException e) {
      // Expected to fail due to old schema
      foundException = true;
    }
    if (!foundException) {
      throw new Exception(
          "Hive operations shouldn't pass with older version schema");
    }

    // Generate dummy pre-upgrade script with errors
    String invalidPreUpgradeScript = writeDummyPreUpgradeScript(
        0, "upgrade-0.11.0-to-0.12.0.derby.sql", "foo bar;");
    // Generate dummy pre-upgrade scripts with valid SQL
    String validPreUpgradeScript0 = writeDummyPreUpgradeScript(
        0, "upgrade-0.12.0-to-0.13.0.derby.sql",
        "CREATE TABLE schema_test0 (id integer);");
    String validPreUpgradeScript1 = writeDummyPreUpgradeScript(
        1, "upgrade-0.12.0-to-0.13.0.derby.sql",
        "CREATE TABLE schema_test1 (id integer);");

    // Capture system out and err
    schemaTool.setVerbose(true);
    OutputStream stderr = new ByteArrayOutputStream();
    PrintStream errPrintStream = new PrintStream(stderr);
    System.setErr(errPrintStream);
    OutputStream stdout = new ByteArrayOutputStream();
    PrintStream outPrintStream = new PrintStream(stdout);
    System.setOut(outPrintStream);

    // Upgrade schema from 0.7.0 to latest
    schemaTool.doUpgrade("0.7.0");

    // Verify that the schemaTool ran pre-upgrade scripts and ignored errors
    assertTrue(stderr.toString().contains(invalidPreUpgradeScript));
    assertTrue(stderr.toString().contains("foo"));
    assertFalse(stderr.toString().contains(validPreUpgradeScript0));
    assertFalse(stderr.toString().contains(validPreUpgradeScript1));
    assertTrue(stdout.toString().contains(validPreUpgradeScript0));
    assertTrue(stdout.toString().contains(validPreUpgradeScript1));

    // Verify that driver works fine with latest schema
    schemaTool.verifySchemaVersion();
  }

  /**
   * Test script formatting
   * @throws Exception
   */
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
    String flattenedSql = HiveSchemaHelper.getDbCommandParser("derby")
        .buildCommand(testScriptFile.getParentFile().getPath(),
            testScriptFile.getName());

    assertEquals(expectedSQL, flattenedSql);
  }

  /**
   * Test nested script formatting
   * @throws Exception
   */
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
    String flattenedSql = HiveSchemaHelper.getDbCommandParser("derby")
        .buildCommand(testScriptFile.getParentFile().getPath(),
            testScriptFile.getName());
    assertFalse(flattenedSql.contains("RUN"));
    assertFalse(flattenedSql.contains("comment"));
    assertTrue(flattenedSql.contains(childTab1));
    assertTrue(flattenedSql.contains(childTab2));
    assertTrue(flattenedSql.contains(parentTab));
  }

  /**
   * Test nested script formatting
   * @throws Exception
   */
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
    String flattenedSql = HiveSchemaHelper.getDbCommandParser("mysql")
        .buildCommand(testScriptFile.getParentFile().getPath(),
            testScriptFile.getName());
    assertFalse(flattenedSql.contains("RUN"));
    assertFalse(flattenedSql.contains("comment"));
    assertTrue(flattenedSql.contains(childTab1));
    assertTrue(flattenedSql.contains(childTab2));
    assertTrue(flattenedSql.contains(parentTab));
  }

  /**
   * Test script formatting
   * @throws Exception
   */
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
    NestedScriptParser testDbParser = HiveSchemaHelper.getDbCommandParser("mysql");
    String flattenedSql = testDbParser.buildCommand(testScriptFile.getParentFile().getPath(),
        testScriptFile.getName());

    assertEquals(expectedSQL, flattenedSql);
  }

  /**
   * Test script formatting
   * @throws Exception
   */
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
    NestedScriptParser testDbParser = HiveSchemaHelper.getDbCommandParser("mysql");
    String flattenedSql = testDbParser.buildCommand(testScriptFile.getParentFile().getPath(),
        testScriptFile.getName());

    assertEquals(expectedSQL, flattenedSql);
  }

  /**
   * Test nested script formatting
   * @throws Exception
   */
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
    String flattenedSql = HiveSchemaHelper.getDbCommandParser("oracle")
        .buildCommand(testScriptFile.getParentFile().getPath(),
            testScriptFile.getName());
    assertFalse(flattenedSql.contains("@"));
    assertFalse(flattenedSql.contains("comment"));
    assertTrue(flattenedSql.contains(childTab1));
    assertTrue(flattenedSql.contains(childTab2));
    assertTrue(flattenedSql.contains(parentTab));
  }

  /**
   * Test script formatting
   * @throws Exception
   */
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
        .getDbCommandParser("postgres");
    String expectedSQL = StringUtils.join(
        expectedScriptWithOptionPresent, System.getProperty("line.separator")) +
            System.getProperty("line.separator");
    File testScriptFile = generateTestScript(testScript);
    String flattenedSql = noDbOptParser.buildCommand(
        testScriptFile.getParentFile().getPath(), testScriptFile.getName());
    assertEquals(expectedSQL, flattenedSql);

    String expectedScriptWithOptionAbsent[] = {
        "DROP TABLE IF EXISTS fooTab",
        "CREATE TABLE fooTab(id INTEGER)",
        "DROP TABLE footab",
    };

    NestedScriptParser dbOptParser = HiveSchemaHelper.getDbCommandParser(
        "postgres",
        PostgresCommandParser.POSTGRES_SKIP_STANDARD_STRINGS_DBOPT,
        null, null, null);
    expectedSQL = StringUtils.join(
        expectedScriptWithOptionAbsent, System.getProperty("line.separator")) +
            System.getProperty("line.separator");
    testScriptFile = generateTestScript(testScript);
    flattenedSql = dbOptParser.buildCommand(
        testScriptFile.getParentFile().getPath(), testScriptFile.getName());
    assertEquals(expectedSQL, flattenedSql);
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

  /**
   * Write out a dummy pre-upgrade script with given SQL statement.
   */
  private String writeDummyPreUpgradeScript(int index, String upgradeScriptName,
      String sql) throws Exception {
    String preUpgradeScript = "pre-" + index + "-" + upgradeScriptName;
    String dummyPreScriptPath = System.getProperty("test.tmp.dir", "target/tmp") +
        File.separatorChar + "scripts" + File.separatorChar + "metastore" +
        File.separatorChar + "upgrade" + File.separatorChar + "derby" +
        File.separatorChar + preUpgradeScript;
    FileWriter fstream = new FileWriter(dummyPreScriptPath);
    BufferedWriter out = new BufferedWriter(fstream);
    out.write(sql + System.getProperty("line.separator") + ";");
    out.close();
    return preUpgradeScript;
  }
}