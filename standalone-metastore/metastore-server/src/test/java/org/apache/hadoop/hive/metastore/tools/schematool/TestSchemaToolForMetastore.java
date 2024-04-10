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

package org.apache.hadoop.hive.metastore.tools.schematool;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.dbcp2.DelegatingConnection;
import org.apache.commons.lang3.text.StrTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.IMetaStoreSchemaInfo;
import org.apache.hadoop.hive.metastore.MetaStoreSchemaInfoFactory;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.dbinstall.rules.DatabaseRule;
import org.apache.hadoop.hive.metastore.dbinstall.rules.Derby;
import org.apache.hadoop.hive.metastore.dbinstall.rules.Mariadb;
import org.apache.hadoop.hive.metastore.dbinstall.rules.Mssql;
import org.apache.hadoop.hive.metastore.dbinstall.rules.Mysql;
import org.apache.hadoop.hive.metastore.dbinstall.rules.Oracle;
import org.apache.hadoop.hive.metastore.dbinstall.rules.Postgres;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.lang.String.format;

@Category(MetastoreCheckinTest.class)
@RunWith(Parameterized.class)
public class TestSchemaToolForMetastore {
  private static final Pattern IDENTIFIER = Pattern.compile("[A-Z_]+");

  private MetastoreSchemaTool schemaTool;
  private Connection conn;
  private Configuration conf;
  private final DatabaseRule dbms;
  private PrintStream errStream;
  private PrintStream outStream;
  private SchemaToolTaskValidate validator;

  public TestSchemaToolForMetastore(DatabaseRule dbms){
    this.dbms = dbms;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> databases() {
    List<Object[]> dbs = new ArrayList<>();
    dbs.add(new Object[] { new Derby(true) });
    dbs.add(new Object[] { new Mysql() });
    dbs.add(new Object[] { new Oracle() });
    dbs.add(new Object[] { new Postgres() });
    dbs.add(new Object[] { new Mariadb() });
    dbs.add(new Object[] { new Mssql() });
    return dbs;
  }
  
  @Before
  public void setUp() throws Exception {
    dbms.before();
    dbms.createUser();
    conf = MetastoreConf.newMetastoreConf();
    schemaTool = new MetastoreSchemaTool();
    schemaTool.init(System.getProperty("test.tmp.dir", "target/tmp"),
        new String[]{"-dbType", dbms.getDbType(), "--info"}, null, conf);
    schemaTool.setUserName(dbms.getHiveUser());
    schemaTool.setPassWord(dbms.getHivePassword());
    schemaTool.setUrl(dbms.getJdbcUrl());
    schemaTool.setDriver(dbms.getJdbcDriver());
    System.setProperty("beeLine.system.exit", "true");
    errStream = System.err;
    outStream = System.out;
    conn = schemaTool.getConnectionToMetastore(false);

    validator = new SchemaToolTaskValidate();
    validator.setHiveSchemaTool(schemaTool);
  }

  @After
  public void tearDown() throws IOException, SQLException {
    System.setOut(outStream);
    System.setErr(errStream);
    if (conn != null) {
      conn.close();
    }
    dbms.after();
  }

  /*
   * Test the sequence validation functionality
   */
  @Test
  public void testValidateSequences() throws Exception {
    execute(new SchemaToolTaskInit(), "-initSchema");

    // Test empty database
    boolean isValid = validator.validateSequences(conn);
    Assert.assertTrue(isValid);

    String time = String.valueOf(System.currentTimeMillis()/1000);
    // Test valid case
    String[] scripts = new String[] {
        "insert into CTLGS values(99, 'test_cat_1', 'description', 'hdfs://myhost.com:8020/user/hive/warehouse/mydb', " + time + ");",
        "insert into SEQUENCE_TABLE values('org.apache.hadoop.hive.metastore.model.MDatabase', 100);",
        "insert into DBS values(99, 'test db1', 'hdfs:///tmp/ext', 'db1', 'test', 'test', 'test_cat_1', " + time + ", 'hdfs:///tmp/mgd', 'NATIVE', '', '');"
    };
    File scriptFile = generateTestScript(scripts);
    schemaTool.execSql(scriptFile.getPath());
    isValid = validator.validateSequences(conn);
    Assert.assertTrue(isValid);

    // Test invalid case
    scripts = new String[] {
        "delete from SEQUENCE_TABLE;",
        "delete from DBS;",
        "insert into SEQUENCE_TABLE values('org.apache.hadoop.hive.metastore.model.MDatabase', 100);",
        "insert into DBS values(102, 'test db1', 'hdfs:///tmp/ext', 'db1', 'test', 'test', 'test_cat_1', " + time + ", 'hdfs:///tmp/mgd', 'NATIVE', '', '');"
    };
    scriptFile = generateTestScript(scripts);
    schemaTool.execSql(scriptFile.getPath());
    isValid = validator.validateSequences(conn);
    Assert.assertFalse(isValid);
  }

  /*
   * Test to validate that all tables exist in the HMS metastore.
   */
  @Test
  public void testValidateSchemaTables() throws Exception {
    execute(new SchemaToolTaskInit(), "-initSchemaTo 1.2.0");

    boolean isValid = validator.validateSchemaTables(conn);
    Assert.assertTrue(isValid);

    // upgrade from 1.2.0 schema and re-validate
    execute(new SchemaToolTaskUpgrade(), "-upgradeSchemaFrom 1.2.0");
    isValid = validator.validateSchemaTables(conn);
    Assert.assertTrue(isValid);

    // Simulate a missing table scenario by renaming a couple of tables
    Map<String, String> tblRenames = new HashMap<>();
    tblRenames.put("SEQUENCE_TABLE", "SEQUENCE_TABLE_RENAMED");
    if (!dbms.getDbType().equals("mssql")) {
      // HIVE-27748: NUCLEUS_TABLES DDLs are missing from MSSQL metastore installation scripts
      tblRenames.put("NUCLEUS_TABLES", "NUCLEUS_TABLES_RENAMED");
    }
    String[] deleteScripts = new String[tblRenames.size()];
    String[] restoreScripts = new String[tblRenames.size()];
    int i = 0;
    for (Map.Entry<String, String> namePair : tblRenames.entrySet()) {
      deleteScripts[i] = renameTableStmt(namePair.getKey(), namePair.getValue());
      restoreScripts[i] = renameTableStmt(namePair.getValue(), namePair.getKey());
      i++;
    }

    File scriptFile = generateTestScript(deleteScripts);
    schemaTool.execSql(scriptFile.getPath());
    isValid = validator.validateSchemaTables(conn);
    Assert.assertFalse(isValid);

    scriptFile = generateTestScript(restoreScripts);
    schemaTool.execSql(scriptFile.getPath());
    isValid = validator.validateSchemaTables(conn);
    Assert.assertTrue(isValid);

    // Check that an exception from getMetaData() is reported correctly
    try {
      // Make a Connection object that will throw an exception
      BadMetaDataConnection bad = new BadMetaDataConnection(conn);
      validator.validateSchemaTables(bad);
      Assert.fail("did not get expected exception");
    } catch (HiveMetaException hme) {
      String message = hme.getMessage();
      Assert.assertTrue("Bad HiveMetaException message :" + message,
          message.contains("Failed to retrieve schema tables from Hive Metastore DB"));
      Throwable cause = hme.getCause();
      Assert.assertNotNull("HiveMetaException did not contain a cause", cause);
      String causeMessage = cause.getMessage();
      Assert.assertTrue("Bad SQLException message: " + causeMessage, causeMessage.contains(
          BadMetaDataConnection.FAILURE_TEXT));
    }
  }


  // Test the validation of incorrect NULL values in the tables
  @Test
  public void testValidateNullValues() throws Exception {
    execute(new SchemaToolTaskInit(), "-initSchema");

    // Test empty database
    boolean isValid = validator.validateColumnNullValues(conn);
    Assert.assertTrue(isValid);

    // Test valid case
    createTestHiveTableSchemas();
    isValid = validator.validateColumnNullValues(conn);

    // Test invalid case
    String[] scripts = new String[] {
        "update TBLS set SD_ID=null"
    };
    File scriptFile = generateTestScript(scripts);
    schemaTool.execSql(scriptFile.getPath());
    isValid = validator.validateColumnNullValues(conn);
    Assert.assertFalse(isValid);
  }

  // Test dryrun of schema initialization
  @Test
  public void testSchemaInitDryRun() throws Exception {
    schemaTool.setDryRun(true);
    execute(new SchemaToolTaskInit(), "-initSchemaTo 1.2.0");
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
    execute(new SchemaToolTaskInit(), "-initSchemaTo 1.2.0");

    schemaTool.setDryRun(true);
    execute(new SchemaToolTaskUpgrade(), "-upgradeSchemaFrom 1.2.0");
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
        System.getProperty("test.tmp.dir", "target/tmp"), dbms.getDbType());
    execute(new SchemaToolTaskInit(), "-initSchemaTo " + metastoreSchemaInfo.getHiveSchemaVersion());
    schemaTool.verifySchemaVersion();
  }

  /**
   * initOrUpgrade takes init path
   */
  @Test
  public void testSchemaInitOrUgrade1() throws Exception {
    execute(new SchemaToolTaskInit(), "-initOrUpgradeSchema");
    schemaTool.verifySchemaVersion();
  }

  /**
   * initOrUpgrade takes upgrade path
   */
  @Test
  public void testSchemaInitOrUgrade2() throws Exception {
    execute(new SchemaToolTaskInit(), "-initSchemaTo 1.2.0");

    schemaTool.setDryRun(true);
    execute(new SchemaToolTaskUpgrade(), "-initOrUpgradeSchema");
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
  * Test validation for schema versions
  */
  @Test
  public void testValidateSchemaVersions() throws Exception {
    execute(new SchemaToolTaskInit(), "-initSchema");
    boolean isValid = validator.validateSchemaVersions();
    // Test an invalid case with multiple versions
    String[] scripts = new String[] {
        "insert into VERSION values(100, '2.2.0', 'Hive release version 2.2.0')"
    };
    File scriptFile = generateTestScript(scripts);
    schemaTool.execSql(scriptFile.getPath());
    isValid = validator.validateSchemaVersions();
    Assert.assertFalse(isValid);

    scripts = new String[] {
        "delete from VERSION where VER_ID = 100"
    };
    scriptFile = generateTestScript(scripts);
    schemaTool.execSql(scriptFile.getPath());
    isValid = validator.validateSchemaVersions();
    Assert.assertTrue(isValid);

    // Test an invalid case without version
    scripts = new String[] {
        "delete from VERSION"
    };
    scriptFile = generateTestScript(scripts);
    schemaTool.execSql(scriptFile.getPath());
    isValid = validator.validateSchemaVersions();
    Assert.assertFalse(isValid);
  }

  /**
   * Test schema upgrade
   */
  @Test
  public void testSchemaUpgrade() throws Exception {
    boolean foundException = false;
    // Initialize 1.2.0 schema
    execute(new SchemaToolTaskInit(), "-initSchemaTo 1.2.0");
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

    String db = dbms.getDbType();
    // Generate dummy pre-upgrade script with errors
    String invalidPreUpgradeScript = writeDummyPreUpgradeScript(
        0, "upgrade-2.3.0-to-3.0.0."+db+".sql", "foo bar;");
    // Generate dummy pre-upgrade scripts with valid SQL
    String validPreUpgradeScript0 = writeDummyPreUpgradeScript(
        1, "upgrade-2.3.0-to-3.0.0."+db+".sql",
        "CREATE TABLE schema_test0 (id integer);");
    String validPreUpgradeScript1 = writeDummyPreUpgradeScript(
        2, "upgrade-2.3.0-to-3.0.0."+db+".sql",
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
    execute(new SchemaToolTaskUpgrade(), "-upgradeSchemaFrom 1.2.0");

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
    execute(new SchemaToolTaskInit(), "-initSchema");
    URI defaultRoot = new URI("hdfs://myhost.com:8020");
    URI defaultRoot2 = new URI("s3://myhost2.com:8888");
    //check empty DB
    boolean isValid = validator.validateLocations(conn, null);
    Assert.assertTrue(isValid);
    isValid = validator.validateLocations(conn, new URI[] {defaultRoot, defaultRoot2});
    Assert.assertTrue(isValid);

    String time = String.valueOf(System.currentTimeMillis()/1000);
    // Test valid case
    String[] scripts = new String[] {
        "insert into CTLGS values(3, 'test_cat_2', 'description', 'hdfs://myhost.com:8020/user/hive/warehouse/mydb', " + time + ");",
        "insert into DBS values(2, 'my db', 'hdfs://myhost.com:8020/user/hive/warehouse/mydb', 'mydb', 'public', 'role', 'test_cat_2', " + time + ", '', 'NATIVE', '', '');",
        "insert into DBS values(7, 'db with bad port', 'hdfs://myhost.com:8020/', 'haDB', 'public', 'role', 'test_cat_2', " + time + ", '', 'NATIVE', '', '');",
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
    schemaTool.execSql(scriptFile.getPath());
    isValid = validator.validateLocations(conn, null);
    Assert.assertTrue(isValid);
    isValid = validator.validateLocations(conn, new URI[] {defaultRoot, defaultRoot2});
    Assert.assertTrue(isValid);
    scripts = new String[] {
        "delete from SKEWED_COL_VALUE_LOC_MAP;",
        "delete from SKEWED_STRING_LIST;",
        "delete from PARTITIONS;",
        "delete from TBLS;",
        "delete from SDS;",
        "delete from DBS;",
        "insert into DBS values(2, 'my db', '/user/hive/warehouse/mydb', 'mydb', 'public', 'role', 'test_cat_2', " + time + ", '', 'NATIVE', '', '');",
        "insert into DBS values(4, 'my db2', 'hdfs://myhost.com:8020', '', 'public', 'role', 'test_cat_2', " + time + ", '', 'NATIVE', '', '');",
        "insert into DBS values(6, 'db with bad port', 'hdfs://myhost.com:8020:', 'zDB', 'public', 'role', 'test_cat_2', " + time + ", '', 'NATIVE', '', '');",
        "insert into DBS values(7, 'db with bad port', 'hdfs://mynameservice.com/', 'haDB', 'public', 'role', 'test_cat_2', " + time + ", '', 'NATIVE', '', '');",
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
    schemaTool.execSql(scriptFile.getPath());
    isValid = validator.validateLocations(conn, null);
    Assert.assertFalse(isValid);
    isValid = validator.validateLocations(conn, new URI[] {defaultRoot, defaultRoot2});
    Assert.assertFalse(isValid);
  }

  @Test
  public void testHiveMetastoreDbPropertiesTable() throws HiveMetaException, IOException {
    execute(new SchemaToolTaskInit(), "-initSchemaTo 3.0.0");
    validateMetastoreDbPropertiesTable();
  }

  @Test
  public void testMetastoreDbPropertiesAfterUpgrade() throws HiveMetaException, IOException {
    execute(new SchemaToolTaskInit(), "-initSchemaTo 1.2.0");
    execute(new SchemaToolTaskUpgrade(), "-upgradeSchema");
    validateMetastoreDbPropertiesTable();
  }

  private File generateTestScript(String [] stmts) throws IOException {
    File testScriptFile = File.createTempFile("schematest", ".sql");
    testScriptFile.deleteOnExit();
    FileWriter fstream = new FileWriter(testScriptFile.getPath());
    BufferedWriter out = new BufferedWriter(fstream);
    for (String line: stmts) {
      line = quoteIdentifiers(line);
      line = line.replaceAll("'[Nn]'", booleanFalse());
      out.write(line);
      out.newLine();
    }
    out.close();
    return testScriptFile;
  }

  private String quoteIdentifiers(String line) {
    if (!dbms.getDbType().equalsIgnoreCase("postgres")) {
      return line;
    }
    String idWithQuote = "\"$0\"";
    String[] part = line.split("values");
    if (part.length == 2) {
      return IDENTIFIER.matcher(part[0]).replaceAll(idWithQuote) + " values " + part[1];
    } else {
      return IDENTIFIER.matcher(line).replaceAll(idWithQuote);
    }
  }

  private String booleanFalse() {
    switch (dbms.getDbType()) {
    case "derby":
      return "'N'";
    case "postgres":
      return "'0'";
    default:
      return "0";
    }
  }

  private String renameTableStmt(String oldName, String newName) {
    switch (dbms.getDbType()) {
    case "mssql":
      return format("exec sp_rename '%s', '%s';", oldName, newName);
    case "postgres":
    case "oracle":
      return format("alter table %s rename to %s;", oldName, newName);
    default:
      return format("rename table %s to %s;", oldName, newName);
    }
  }

  private void validateMetastoreDbPropertiesTable() throws HiveMetaException, IOException {
    boolean isValid = (boolean) validator.validateSchemaTables(conn);
    Assert.assertTrue(isValid);
    // adding same property key twice should throw unique key constraint violation exception
    String[] scripts = new String[] {
        "insert into METASTORE_DB_PROPERTIES values ('guid', 'test-uuid-1', 'dummy uuid 1');",
        "insert into METASTORE_DB_PROPERTIES values ('guid', 'test-uuid-2', 'dummy uuid 2');", };
    File scriptFile = generateTestScript(scripts);
    Exception ex = null;
    try {
      schemaTool.execSql(scriptFile.getPath());
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
        File.separatorChar + "upgrade" + File.separatorChar + dbms.getDbType() +
        File.separatorChar + preUpgradeScript;
    FileWriter fstream = new FileWriter(dummyPreScriptPath);
    BufferedWriter out = new BufferedWriter(fstream);
    out.write(sql + System.getProperty("line.separator"));
    out.close();
    return preUpgradeScript;
  }

  // Insert the records in DB to simulate a hive table
  private void createTestHiveTableSchemas() throws IOException {
    String time = String.valueOf(System.currentTimeMillis()/1000);
     String[] scripts = new String[] {
          "insert into CTLGS values (2, 'mycat', 'my description', 'hdfs://myhost.com:8020/user/hive/warehouse', " + time + ");",
          "insert into DBS values(2, 'my db', 'hdfs://myhost.com:8020/user/hive/warehouse/mydb', 'mydb', 'public', 'role', 'mycat', " + time + ", '', 'NATIVE', '', '');",
          "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,IS_STOREDASSUBDIRECTORIES,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (1,null,'org.apache.hadoop.mapred.TextInputFormat','N','N','hdfs://myhost.com:8020/user/hive/warehouse/mydb',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null);",
          "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,IS_STOREDASSUBDIRECTORIES,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (2,null,'org.apache.hadoop.mapred.TextInputFormat','N','N','hdfs://myhost.com:8020/user/admin/2015_11_18',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null);",
          "insert into TBLS(TBL_ID,CREATE_TIME,DB_ID,LAST_ACCESS_TIME,OWNER,RETENTION,SD_ID,TBL_NAME,TBL_TYPE,VIEW_EXPANDED_TEXT,VIEW_ORIGINAL_TEXT,IS_REWRITE_ENABLED) values (2 ,1435255431,2,0 ,'hive',0,1,'mytal','MANAGED_TABLE',NULL,NULL,'n');",
          "insert into TBLS(TBL_ID,CREATE_TIME,DB_ID,LAST_ACCESS_TIME,OWNER,RETENTION,SD_ID,TBL_NAME,TBL_TYPE,VIEW_EXPANDED_TEXT,VIEW_ORIGINAL_TEXT,IS_REWRITE_ENABLED) values (3 ,1435255431,2,0 ,'hive',0,2,'aTable','MANAGED_TABLE',NULL,NULL,'n');",
          "insert into PARTITIONS(PART_ID,CREATE_TIME,LAST_ACCESS_TIME, PART_NAME,SD_ID,TBL_ID) values(1, 1441402388,0, 'd1=1/d2=1',2,2);"
        };
     File scriptFile = generateTestScript(scripts);
     schemaTool.execSql(scriptFile.getPath());
  }

  /**
   * A mock Connection class that throws an exception out of getMetaData().
   */
  class BadMetaDataConnection extends DelegatingConnection {
    static final String FAILURE_TEXT = "fault injected";

    BadMetaDataConnection(Connection connection) {
      super(connection);
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
      throw new SQLException(FAILURE_TEXT);
    }
  }

  private void execute(SchemaToolTask task, String taskArgs) throws HiveMetaException {
    String argsBase =
        format("-dbType %s -userName %s -passWord %s ", dbms.getDbType(), dbms.getHiveUser(), dbms.getHivePassword());
    try {
      StrTokenizer tokenizer = new StrTokenizer(argsBase + taskArgs, ' ', '\"');
      SchemaToolCommandLine cl = new SchemaToolCommandLine(tokenizer.getTokenArray(), null);
      task.setCommandLineArguments(cl);
    } catch (Exception e) {
      throw new IllegalStateException("Could not parse comman line \n" + argsBase + taskArgs, e);
    }

    task.setHiveSchemaTool(schemaTool);
    task.execute();
  }
}
