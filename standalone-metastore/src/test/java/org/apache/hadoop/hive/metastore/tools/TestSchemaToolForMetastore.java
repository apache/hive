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

package org.apache.hadoop.hive.metastore.tools;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.IMetaStoreSchemaInfo;
import org.apache.hadoop.hive.metastore.MetaStoreSchemaInfoFactory;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(MetastoreCheckinTest.class)
public class TestSchemaToolForMetastore {
  private static final Logger LOG = LoggerFactory.getLogger(TestMetastoreSchemaTool.class);

  private MetastoreSchemaTool schemaTool;
  private Connection conn;
  private Configuration conf;
  private String testMetastoreDB;
  private PrintStream errStream;
  private PrintStream outStream;

  @Before
  public void setUp() throws HiveMetaException, IOException {
    testMetastoreDB = System.getProperty("java.io.tmpdir") +
        File.separator + "test_metastore-" + new Random().nextInt();
    System.setProperty(ConfVars.CONNECTURLKEY.toString(),
        "jdbc:derby:" + testMetastoreDB + ";create=true");
    conf = MetastoreConf.newMetastoreConf();
    schemaTool = new MetastoreSchemaTool(
        System.getProperty("test.tmp.dir", "target/tmp"), conf, "derby");
    schemaTool.setUserName(MetastoreConf.getVar(schemaTool.getConf(), ConfVars.CONNECTION_USER_NAME));
    schemaTool.setPassWord(MetastoreConf.getPassword(schemaTool.getConf(), ConfVars.PWD));
    System.setProperty("beeLine.system.exit", "true");
    errStream = System.err;
    outStream = System.out;
    conn = schemaTool.getConnectionToMetastore(false);
  }

  @After
  public void tearDown() throws IOException, SQLException {
    File metaStoreDir = new File(testMetastoreDB);
    if (metaStoreDir.exists()) {
      FileUtils.forceDeleteOnExit(metaStoreDir);
    }
    System.setOut(outStream);
    System.setErr(errStream);
    if (conn != null) {
      conn.close();
    }
  }

  // Test the sequence validation functionality
  @Test
  public void testValidateSequences() throws Exception {
    schemaTool.doInit();

    // Test empty database
    boolean isValid = schemaTool.validateSequences(conn);
    Assert.assertTrue(isValid);

    // Test valid case
    String[] scripts = new String[] {
        "insert into SEQUENCE_TABLE values('org.apache.hadoop.hive.metastore.model.MDatabase', 100);",
        "insert into DBS values(99, 'test db1', 'hdfs:///tmp', 'db1', 'test', 'test');"
    };
    File scriptFile = generateTestScript(scripts);
    schemaTool.runSqlLine(scriptFile.getPath());
    isValid = schemaTool.validateSequences(conn);
    Assert.assertTrue(isValid);

    // Test invalid case
    scripts = new String[] {
        "delete from SEQUENCE_TABLE;",
        "delete from DBS;",
        "insert into SEQUENCE_TABLE values('org.apache.hadoop.hive.metastore.model.MDatabase', 100);",
        "insert into DBS values(102, 'test db1', 'hdfs:///tmp', 'db1', 'test', 'test');"
    };
    scriptFile = generateTestScript(scripts);
    schemaTool.runSqlLine(scriptFile.getPath());
    isValid = schemaTool.validateSequences(conn);
    Assert.assertFalse(isValid);
  }

  // Test to validate that all tables exist in the HMS metastore.
  @Test
  public void testValidateSchemaTables() throws Exception {
    schemaTool.doInit("1.2.0");

    boolean isValid = schemaTool.validateSchemaTables(conn);
    Assert.assertTrue(isValid);

    // upgrade from 2.0.0 schema and re-validate
    schemaTool.doUpgrade("1.2.0");
    isValid = schemaTool.validateSchemaTables(conn);
    Assert.assertTrue(isValid);

    // Simulate a missing table scenario by renaming a couple of tables
    String[] scripts = new String[] {
        "RENAME TABLE SEQUENCE_TABLE to SEQUENCE_TABLE_RENAMED;",
        "RENAME TABLE NUCLEUS_TABLES to NUCLEUS_TABLES_RENAMED;"
    };

    File scriptFile = generateTestScript(scripts);
    schemaTool.runSqlLine(scriptFile.getPath());
    isValid = schemaTool.validateSchemaTables(conn);
    Assert.assertFalse(isValid);

    // Restored the renamed tables
    scripts = new String[] {
        "RENAME TABLE SEQUENCE_TABLE_RENAMED to SEQUENCE_TABLE;",
        "RENAME TABLE NUCLEUS_TABLES_RENAMED to NUCLEUS_TABLES;"
    };

    scriptFile = generateTestScript(scripts);
    schemaTool.runSqlLine(scriptFile.getPath());
    isValid = schemaTool.validateSchemaTables(conn);
    Assert.assertTrue(isValid);
   }

  // Test the validation of incorrect NULL values in the tables
  @Test
  public void testValidateNullValues() throws Exception {
    schemaTool.doInit();

    // Test empty database
    boolean isValid = schemaTool.validateColumnNullValues(conn);
    Assert.assertTrue(isValid);

    // Test valid case
    createTestHiveTableSchemas();
    isValid = schemaTool.validateColumnNullValues(conn);

    // Test invalid case
    String[] scripts = new String[] {
        "update TBLS set SD_ID=null"
    };
    File scriptFile = generateTestScript(scripts);
    schemaTool.runSqlLine(scriptFile.getPath());
    isValid = schemaTool.validateColumnNullValues(conn);
    Assert.assertFalse(isValid);
  }

  // Test dryrun of schema initialization
  @Test
  public void testSchemaInitDryRun() throws Exception {
    schemaTool.setDryRun(true);
    schemaTool.doInit("3.0.0");
    schemaTool.setDryRun(false);
    try {
      schemaTool.verifySchemaVersion();
    } catch (HiveMetaException e) {
      // The connection should fail since it the dry run
      return;
    }
    Assert.fail("Dry run shouldn't create actual metastore");
  }

  // Test dryrun of schema upgrade
  @Test
  public void testSchemaUpgradeDryRun() throws Exception {
    schemaTool.doInit("1.2.0");

    schemaTool.setDryRun(true);
    schemaTool.doUpgrade("1.2.0");
    schemaTool.setDryRun(false);
    try {
      schemaTool.verifySchemaVersion();
    } catch (HiveMetaException e) {
      // The connection should fail since it the dry run
      return;
    }
    Assert.fail("Dry run shouldn't upgrade metastore schema");
  }

  /**
   * Test schema initialization
   */
  @Test
  public void testSchemaInit() throws Exception {
    IMetaStoreSchemaInfo metastoreSchemaInfo = MetaStoreSchemaInfoFactory.get(conf,
        System.getProperty("test.tmp.dir", "target/tmp"), "derby");
    schemaTool.doInit(metastoreSchemaInfo.getHiveSchemaVersion());
    schemaTool.verifySchemaVersion();
  }

  /**
  * Test validation for schema versions
  */
  @Test
 public void testValidateSchemaVersions() throws Exception {
   schemaTool.doInit();
   boolean isValid = schemaTool.validateSchemaVersions();
   // Test an invalid case with multiple versions
   String[] scripts = new String[] {
       "insert into VERSION values(100, '2.2.0', 'Hive release version 2.2.0')"
   };
   File scriptFile = generateTestScript(scripts);
   schemaTool.runSqlLine(scriptFile.getPath());
   isValid = schemaTool.validateSchemaVersions();
   Assert.assertFalse(isValid);

   scripts = new String[] {
       "delete from VERSION where VER_ID = 100"
   };
   scriptFile = generateTestScript(scripts);
   schemaTool.runSqlLine(scriptFile.getPath());
   isValid = schemaTool.validateSchemaVersions();
   Assert.assertTrue(isValid);

   // Test an invalid case without version
   scripts = new String[] {
       "delete from VERSION"
   };
   scriptFile = generateTestScript(scripts);
   schemaTool.runSqlLine(scriptFile.getPath());
   isValid = schemaTool.validateSchemaVersions();
   Assert.assertFalse(isValid);
 }

  /**
   * Test schema upgrade
   */
  @Test
  public void testSchemaUpgrade() throws Exception {
    boolean foundException = false;
    // Initialize 1.2.0 schema
    schemaTool.doInit("1.2.0");
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
        0, "upgrade-2.3.0-to-3.0.0.derby.sql", "foo bar;");
    // Generate dummy pre-upgrade scripts with valid SQL
    String validPreUpgradeScript0 = writeDummyPreUpgradeScript(
        1, "upgrade-2.3.0-to-3.0.0.derby.sql",
        "CREATE TABLE schema_test0 (id integer);");
    String validPreUpgradeScript1 = writeDummyPreUpgradeScript(
        2, "upgrade-2.3.0-to-3.0.0.derby.sql",
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
    schemaTool.doUpgrade("1.2.0");

    LOG.info("stdout is " + stdout.toString());
    LOG.info("stderr is " + stderr.toString());

    // Verify that the schemaTool ran pre-upgrade scripts and ignored errors
    Assert.assertTrue(stderr.toString().contains(invalidPreUpgradeScript));
    Assert.assertTrue(stderr.toString().contains("foo"));
    Assert.assertFalse(stderr.toString().contains(validPreUpgradeScript0));
    Assert.assertFalse(stderr.toString().contains(validPreUpgradeScript1));
    Assert.assertTrue(stdout.toString().contains(validPreUpgradeScript0));
    Assert.assertTrue(stdout.toString().contains(validPreUpgradeScript1));

    // Verify that driver works fine with latest schema
    schemaTool.verifySchemaVersion();
  }

  /**
   * Test validate uri of locations
   */
  @Test
  public void testValidateLocations() throws Exception {
    schemaTool.doInit();
    URI defaultRoot = new URI("hdfs://myhost.com:8020");
    URI defaultRoot2 = new URI("s3://myhost2.com:8888");
    //check empty DB
    boolean isValid = schemaTool.validateLocations(conn, null);
    Assert.assertTrue(isValid);
    isValid = schemaTool.validateLocations(conn, new URI[] {defaultRoot,defaultRoot2});
    Assert.assertTrue(isValid);

 // Test valid case
    String[] scripts = new String[] {
         "insert into DBS values(2, 'my db', 'hdfs://myhost.com:8020/user/hive/warehouse/mydb', 'mydb', 'public', 'role');",
         "insert into DBS values(7, 'db with bad port', 'hdfs://myhost.com:8020/', 'haDB', 'public', 'role');",
         "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,IS_STOREDASSUBDIRECTORIES,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (1,null,'org.apache.hadoop.mapred.TextInputFormat','N','N','hdfs://myhost.com:8020/user/hive/warehouse/mydb',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null);",
         "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,IS_STOREDASSUBDIRECTORIES,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (2,null,'org.apache.hadoop.mapred.TextInputFormat','N','N','hdfs://myhost.com:8020/user/admin/2015_11_18',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null);",
         "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,IS_STOREDASSUBDIRECTORIES,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (3,null,'org.apache.hadoop.mapred.TextInputFormat','N','N',null,-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null);",
         "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,IS_STOREDASSUBDIRECTORIES,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (4000,null,'org.apache.hadoop.mapred.TextInputFormat','N','N','hdfs://myhost.com:8020/',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null);",
         "insert into TBLS(TBL_ID,CREATE_TIME,DB_ID,LAST_ACCESS_TIME,OWNER,RETENTION,SD_ID,TBL_NAME,TBL_TYPE,VIEW_EXPANDED_TEXT,VIEW_ORIGINAL_TEXT,IS_REWRITE_ENABLED) values (2 ,1435255431,2,0 ,'hive',0,1,'mytal','MANAGED_TABLE',NULL,NULL,'n');",
         "insert into TBLS(TBL_ID,CREATE_TIME,DB_ID,LAST_ACCESS_TIME,OWNER,RETENTION,SD_ID,TBL_NAME,TBL_TYPE,VIEW_EXPANDED_TEXT,VIEW_ORIGINAL_TEXT,IS_REWRITE_ENABLED) values (3 ,1435255431,2,0 ,'hive',0,3,'myView','VIRTUAL_VIEW','select a.col1,a.col2 from foo','select * from foo','n');",
         "insert into TBLS(TBL_ID,CREATE_TIME,DB_ID,LAST_ACCESS_TIME,OWNER,RETENTION,SD_ID,TBL_NAME,TBL_TYPE,VIEW_EXPANDED_TEXT,VIEW_ORIGINAL_TEXT,IS_REWRITE_ENABLED) values (4012 ,1435255431,7,0 ,'hive',0,4000,'mytal4012','MANAGED_TABLE',NULL,NULL,'n');",
         "insert into PARTITIONS(PART_ID,CREATE_TIME,LAST_ACCESS_TIME, PART_NAME,SD_ID,TBL_ID) values(1, 1441402388,0, 'd1=1/d2=1',2,2);",
         "insert into SKEWED_STRING_LIST values(1);",
         "insert into SKEWED_STRING_LIST values(2);",
         "insert into SKEWED_COL_VALUE_LOC_MAP values(1,1,'hdfs://myhost.com:8020/user/hive/warehouse/mytal/HIVE_DEFAULT_LIST_BUCKETING_DIR_NAME/');",
         "insert into SKEWED_COL_VALUE_LOC_MAP values(2,2,'s3://myhost.com:8020/user/hive/warehouse/mytal/HIVE_DEFAULT_LIST_BUCKETING_DIR_NAME/');"
       };
    File scriptFile = generateTestScript(scripts);
    schemaTool.runSqlLine(scriptFile.getPath());
    isValid = schemaTool.validateLocations(conn, null);
    Assert.assertTrue(isValid);
    isValid = schemaTool.validateLocations(conn, new URI[] {defaultRoot, defaultRoot2});
    Assert.assertTrue(isValid);
    scripts = new String[] {
        "delete from SKEWED_COL_VALUE_LOC_MAP;",
        "delete from SKEWED_STRING_LIST;",
        "delete from PARTITIONS;",
        "delete from TBLS;",
        "delete from SDS;",
        "delete from DBS;",
        "insert into DBS values(2, 'my db', '/user/hive/warehouse/mydb', 'mydb', 'public', 'role');",
        "insert into DBS values(4, 'my db2', 'hdfs://myhost.com:8020', '', 'public', 'role');",
        "insert into DBS values(6, 'db with bad port', 'hdfs://myhost.com:8020:', 'zDB', 'public', 'role');",
        "insert into DBS values(7, 'db with bad port', 'hdfs://mynameservice.com/', 'haDB', 'public', 'role');",
        "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,IS_STOREDASSUBDIRECTORIES,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (1,null,'org.apache.hadoop.mapred.TextInputFormat','N','N','hdfs://yourhost.com:8020/user/hive/warehouse/mydb',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null);",
        "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,IS_STOREDASSUBDIRECTORIES,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (2,null,'org.apache.hadoop.mapred.TextInputFormat','N','N','file:///user/admin/2015_11_18',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null);",
        "insert into TBLS(TBL_ID,CREATE_TIME,DB_ID,LAST_ACCESS_TIME,OWNER,RETENTION,SD_ID,TBL_NAME,TBL_TYPE,VIEW_EXPANDED_TEXT,VIEW_ORIGINAL_TEXT,IS_REWRITE_ENABLED) values (2 ,1435255431,2,0 ,'hive',0,1,'mytal','MANAGED_TABLE',NULL,NULL,'n');",
        "insert into PARTITIONS(PART_ID,CREATE_TIME,LAST_ACCESS_TIME, PART_NAME,SD_ID,TBL_ID) values(1, 1441402388,0, 'd1=1/d2=1',2,2);",
        "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,IS_STOREDASSUBDIRECTORIES,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (3000,null,'org.apache.hadoop.mapred.TextInputFormat','N','N','yourhost.com:8020/user/hive/warehouse/mydb',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null);",
        "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,IS_STOREDASSUBDIRECTORIES,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (4000,null,'org.apache.hadoop.mapred.TextInputFormat','N','N','hdfs://myhost.com:8020/',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null);",
        "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,IS_STOREDASSUBDIRECTORIES,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (4001,null,'org.apache.hadoop.mapred.TextInputFormat','N','N','hdfs://myhost.com:8020',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null);",
        "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,IS_STOREDASSUBDIRECTORIES,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (4003,null,'org.apache.hadoop.mapred.TextInputFormat','N','N','hdfs://myhost.com:8020',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null);",
        "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,IS_STOREDASSUBDIRECTORIES,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (4004,null,'org.apache.hadoop.mapred.TextInputFormat','N','N','hdfs://myhost.com:8020',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null);",
        "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,IS_STOREDASSUBDIRECTORIES,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (4002,null,'org.apache.hadoop.mapred.TextInputFormat','N','N','hdfs://myhost.com:8020/',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null);",
        "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,IS_STOREDASSUBDIRECTORIES,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (5000,null,'org.apache.hadoop.mapred.TextInputFormat','N','N','file:///user/admin/2016_11_18',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null);",
        "insert into TBLS(TBL_ID,CREATE_TIME,DB_ID,LAST_ACCESS_TIME,OWNER,RETENTION,SD_ID,TBL_NAME,TBL_TYPE,VIEW_EXPANDED_TEXT,VIEW_ORIGINAL_TEXT,IS_REWRITE_ENABLED) values (3000 ,1435255431,2,0 ,'hive',0,3000,'mytal3000','MANAGED_TABLE',NULL,NULL,'n');",
        "insert into TBLS(TBL_ID,CREATE_TIME,DB_ID,LAST_ACCESS_TIME,OWNER,RETENTION,SD_ID,TBL_NAME,TBL_TYPE,VIEW_EXPANDED_TEXT,VIEW_ORIGINAL_TEXT,IS_REWRITE_ENABLED) values (4011 ,1435255431,4,0 ,'hive',0,4001,'mytal4011','MANAGED_TABLE',NULL,NULL,'n');",
        "insert into TBLS(TBL_ID,CREATE_TIME,DB_ID,LAST_ACCESS_TIME,OWNER,RETENTION,SD_ID,TBL_NAME,TBL_TYPE,VIEW_EXPANDED_TEXT,VIEW_ORIGINAL_TEXT,IS_REWRITE_ENABLED) values (4012 ,1435255431,4,0 ,'hive',0,4002,'','MANAGED_TABLE',NULL,NULL,'n');",
        "insert into TBLS(TBL_ID,CREATE_TIME,DB_ID,LAST_ACCESS_TIME,OWNER,RETENTION,SD_ID,TBL_NAME,TBL_TYPE,VIEW_EXPANDED_TEXT,VIEW_ORIGINAL_TEXT,IS_REWRITE_ENABLED) values (4013 ,1435255431,4,0 ,'hive',0,4003,'mytal4013','MANAGED_TABLE',NULL,NULL,'n');",
        "insert into TBLS(TBL_ID,CREATE_TIME,DB_ID,LAST_ACCESS_TIME,OWNER,RETENTION,SD_ID,TBL_NAME,TBL_TYPE,VIEW_EXPANDED_TEXT,VIEW_ORIGINAL_TEXT,IS_REWRITE_ENABLED) values (4014 ,1435255431,2,0 ,'hive',0,4003,'','MANAGED_TABLE',NULL,NULL,'n');",
        "insert into PARTITIONS(PART_ID,CREATE_TIME,LAST_ACCESS_TIME, PART_NAME,SD_ID,TBL_ID) values(4001, 1441402388,0, 'd1=1/d2=4001',4001,4011);",
        "insert into PARTITIONS(PART_ID,CREATE_TIME,LAST_ACCESS_TIME, PART_NAME,SD_ID,TBL_ID) values(4002, 1441402388,0, 'd1=1/d2=4002',4002,4012);",
        "insert into PARTITIONS(PART_ID,CREATE_TIME,LAST_ACCESS_TIME, PART_NAME,SD_ID,TBL_ID) values(4003, 1441402388,0, 'd1=1/d2=4003',4003,4013);",
        "insert into PARTITIONS(PART_ID,CREATE_TIME,LAST_ACCESS_TIME, PART_NAME,SD_ID,TBL_ID) values(4004, 1441402388,0, 'd1=1/d2=4004',4004,4014);",
        "insert into PARTITIONS(PART_ID,CREATE_TIME,LAST_ACCESS_TIME, PART_NAME,SD_ID,TBL_ID) values(5000, 1441402388,0, 'd1=1/d2=5000',5000,2);",
        "insert into SKEWED_STRING_LIST values(1);",
        "insert into SKEWED_STRING_LIST values(2);",
        "insert into SKEWED_COL_VALUE_LOC_MAP values(1,1,'hdfs://yourhost.com:8020/user/hive/warehouse/mytal/HIVE_DEFAULT_LIST_BUCKETING_DIR_NAME/');",
        "insert into SKEWED_COL_VALUE_LOC_MAP values(2,2,'file:///user/admin/warehouse/mytal/HIVE_DEFAULT_LIST_BUCKETING_DIR_NAME/');"
    };
    scriptFile = generateTestScript(scripts);
    schemaTool.runSqlLine(scriptFile.getPath());
    isValid = schemaTool.validateLocations(conn, null);
    Assert.assertFalse(isValid);
    isValid = schemaTool.validateLocations(conn, new URI[] {defaultRoot, defaultRoot2});
    Assert.assertFalse(isValid);
  }

  @Test
  public void testHiveMetastoreDbPropertiesTable() throws HiveMetaException, IOException {
    schemaTool.doInit("3.0.0");
    validateMetastoreDbPropertiesTable();
  }

  @Test
  public void testMetastoreDbPropertiesAfterUpgrade() throws HiveMetaException, IOException {
    schemaTool.doInit("1.2.0");
    schemaTool.doUpgrade();
    validateMetastoreDbPropertiesTable();
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

  private void validateMetastoreDbPropertiesTable() throws HiveMetaException, IOException {
    boolean isValid = schemaTool.validateSchemaTables(conn);
    Assert.assertTrue(isValid);
    // adding same property key twice should throw unique key constraint violation exception
    String[] scripts = new String[] {
        "insert into METASTORE_DB_PROPERTIES values ('guid', 'test-uuid-1', 'dummy uuid 1')",
        "insert into METASTORE_DB_PROPERTIES values ('guid', 'test-uuid-2', 'dummy uuid 2')", };
    File scriptFile = generateTestScript(scripts);
    Exception ex = null;
    try {
      schemaTool.runSqlLine(scriptFile.getPath());
    } catch (Exception iox) {
      ex = iox;
    }
    Assert.assertTrue(ex != null && ex instanceof IOException);
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
    out.write(sql + System.getProperty("line.separator"));
    out.close();
    return preUpgradeScript;
  }

  // Insert the records in DB to simulate a hive table
  private void createTestHiveTableSchemas() throws IOException {
     String[] scripts = new String[] {
          "insert into DBS values(2, 'my db', 'hdfs://myhost.com:8020/user/hive/warehouse/mydb', 'mydb', 'public', 'role');",
          "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,IS_STOREDASSUBDIRECTORIES,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (1,null,'org.apache.hadoop.mapred.TextInputFormat','N','N','hdfs://myhost.com:8020/user/hive/warehouse/mydb',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null);",
          "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,IS_STOREDASSUBDIRECTORIES,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (2,null,'org.apache.hadoop.mapred.TextInputFormat','N','N','hdfs://myhost.com:8020/user/admin/2015_11_18',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null);",
          "insert into TBLS(TBL_ID,CREATE_TIME,DB_ID,LAST_ACCESS_TIME,OWNER,RETENTION,SD_ID,TBL_NAME,TBL_TYPE,VIEW_EXPANDED_TEXT,VIEW_ORIGINAL_TEXT,IS_REWRITE_ENABLED) values (2 ,1435255431,2,0 ,'hive',0,1,'mytal','MANAGED_TABLE',NULL,NULL,'n');",
          "insert into TBLS(TBL_ID,CREATE_TIME,DB_ID,LAST_ACCESS_TIME,OWNER,RETENTION,SD_ID,TBL_NAME,TBL_TYPE,VIEW_EXPANDED_TEXT,VIEW_ORIGINAL_TEXT,IS_REWRITE_ENABLED) values (3 ,1435255431,2,0 ,'hive',0,2,'aTable','MANAGED_TABLE',NULL,NULL,'n');",
          "insert into PARTITIONS(PART_ID,CREATE_TIME,LAST_ACCESS_TIME, PART_NAME,SD_ID,TBL_ID) values(1, 1441402388,0, 'd1=1/d2=1',2,2);"
        };
     File scriptFile = generateTestScript(scripts);
     schemaTool.runSqlLine(scriptFile.getPath());
  }
}
