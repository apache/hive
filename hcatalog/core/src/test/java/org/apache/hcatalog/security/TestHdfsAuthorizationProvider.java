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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hcatalog.security;

import static org.apache.hcatalog.HcatTestUtils.perm300;
import static org.apache.hcatalog.HcatTestUtils.perm500;
import static org.apache.hcatalog.HcatTestUtils.perm555;
import static org.apache.hcatalog.HcatTestUtils.perm700;
import static org.apache.hcatalog.HcatTestUtils.perm755;

import java.io.IOException;
import java.util.Random;

import junit.framework.Assert;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hcatalog.HcatTestUtils;
import org.apache.hcatalog.cli.HCatDriver;
import org.apache.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @deprecated 
 */
public class TestHdfsAuthorizationProvider {

  protected HCatDriver hcatDriver;
  protected HiveMetaStoreClient msc;
  protected HiveConf conf;
  protected String whDir;
  protected Path whPath;
  protected FileSystem whFs;
  protected Warehouse wh;
  protected Hive hive;

  @Before
  public void setUp() throws Exception {

    conf = new HiveConf(this.getClass());
    conf.set(ConfVars.PREEXECHOOKS.varname, "");
    conf.set(ConfVars.POSTEXECHOOKS.varname, "");
    conf.set(ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");

    conf.set("hive.metastore.local", "true");
    conf.set(ConfVars.SEMANTIC_ANALYZER_HOOK.varname, HCatSemanticAnalyzer.class.getName());
    conf.setBoolVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED, true);
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        StorageDelegationAuthorizationProvider.class.getName());
    conf.set("fs.pfile.impl", "org.apache.hadoop.fs.ProxyLocalFileSystem");

    whDir = System.getProperty("test.warehouse.dir", "/tmp/testhdfsauthorization_wh");
    conf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, whDir);

    UserGroupInformation ugi = ShimLoader.getHadoopShims().getUGIForConf(conf);
    String username = ShimLoader.getHadoopShims().getShortUserName(ugi);

    whPath = new Path(whDir);
    whFs = whPath.getFileSystem(conf);

    wh = new Warehouse(conf);
    hive = Hive.get(conf);

    //clean up mess in HMS
    HcatTestUtils.cleanupHMS(hive, wh, perm700);

    whFs.delete(whPath, true);
    whFs.mkdirs(whPath, perm755);

    SessionState.start(new CliSessionState(conf));
    hcatDriver = new HCatDriver();
  }

  @After
  public void tearDown() throws IOException {
    whFs.close();
    hcatDriver.close();
    Hive.closeCurrent();
  }

  public Path getDbPath(String dbName) throws MetaException, HiveException {
    return HcatTestUtils.getDbPath(hive, wh, dbName);
  }

  public Path getTablePath(String dbName, String tableName) throws HiveException {
    Table table = hive.getTable(dbName, tableName);
    return table.getPath();
  }

  public Path getPartPath(String partName, String dbName, String tableName) throws HiveException {
    return new Path(getTablePath(dbName, tableName), partName);
  }

  /** Execute the query expecting success*/
  public void exec(String format, Object... args) throws Exception {
    String command = String.format(format, args);
    CommandProcessorResponse resp = hcatDriver.run(command);
    Assert.assertEquals(resp.getErrorMessage(), 0, resp.getResponseCode());
    Assert.assertEquals(resp.getErrorMessage(), null, resp.getErrorMessage());
  }

  /** Execute the query expecting it to fail with AuthorizationException */
  public void execFail(String format, Object... args) throws Exception {
    String command = String.format(format, args);
    CommandProcessorResponse resp = hcatDriver.run(command);
    Assert.assertNotSame(resp.getErrorMessage(), 0, resp.getResponseCode());
    Assert.assertTrue((resp.getResponseCode() == 40000) || (resp.getResponseCode() == 403));
    if (resp.getErrorMessage() != null) {
      Assert.assertTrue(resp.getErrorMessage().contains("org.apache.hadoop.security.AccessControlException"));
    }
  }


  /**
   * Tests whether the warehouse directory is writable by the current user (as defined by Hadoop)
   */
  @Test
  public void testWarehouseIsWritable() throws Exception {
    Path top = new Path(whPath, "_foobarbaz12_");
    try {
      whFs.mkdirs(top);
    } finally {
      whFs.delete(top, true);
    }
  }

  @Test
  public void testShowDatabases() throws Exception {
    exec("CREATE DATABASE doo");
    exec("SHOW DATABASES");

    whFs.setPermission(whPath, perm300); //revoke r
    execFail("SHOW DATABASES");
  }

  @Test
  public void testDatabaseOps() throws Exception {
    exec("SHOW TABLES");
    exec("SHOW TABLE EXTENDED LIKE foo1");

    whFs.setPermission(whPath, perm700);
    exec("CREATE DATABASE doo");
    exec("DESCRIBE DATABASE doo");
    exec("USE doo");
    exec("SHOW TABLES");
    exec("SHOW TABLE EXTENDED LIKE foo1");
    exec("DROP DATABASE doo");

    //custom location
    Path dbPath = new Path(whPath, new Random().nextInt() + "/mydb");
    whFs.mkdirs(dbPath, perm700);
    exec("CREATE DATABASE doo2 LOCATION '%s'", dbPath.toUri());
    exec("DESCRIBE DATABASE doo2", dbPath.toUri());
    exec("USE doo2");
    exec("SHOW TABLES");
    exec("SHOW TABLE EXTENDED LIKE foo1");
    exec("DROP DATABASE doo2", dbPath.toUri());

    //custom non-existing location
    exec("CREATE DATABASE doo3 LOCATION '%s/subpath'", dbPath.toUri());
  }

  @Test
  public void testCreateDatabaseFail1() throws Exception {
    whFs.setPermission(whPath, perm500);
    execFail("CREATE DATABASE doo"); //in the default location

    whFs.setPermission(whPath, perm555);
    execFail("CREATE DATABASE doo2");
  }

  @Test
  public void testCreateDatabaseFail2() throws Exception {
    //custom location
    Path dbPath = new Path(whPath, new Random().nextInt() + "/mydb");

    whFs.mkdirs(dbPath, perm700);
    whFs.setPermission(dbPath, perm500);
    execFail("CREATE DATABASE doo2 LOCATION '%s'", dbPath.toUri());
  }

  @Test
  public void testDropDatabaseFail1() throws Exception {
    whFs.setPermission(whPath, perm700);
    exec("CREATE DATABASE doo"); //in the default location

    whFs.setPermission(getDbPath("doo"), perm500); //revoke write
    execFail("DROP DATABASE doo");
  }

  @Test
  public void testDropDatabaseFail2() throws Exception {
    //custom location
    Path dbPath = new Path(whPath, new Random().nextInt() + "/mydb");

    whFs.mkdirs(dbPath, perm700);
    exec("CREATE DATABASE doo2 LOCATION '%s'", dbPath.toUri());

    whFs.setPermission(dbPath, perm500);
    execFail("DROP DATABASE doo2");
  }

  @Test
  public void testDescSwitchDatabaseFail() throws Exception {
    whFs.setPermission(whPath, perm700);
    exec("CREATE DATABASE doo");
    whFs.setPermission(getDbPath("doo"), perm300); //revoke read
    execFail("DESCRIBE DATABASE doo");
    execFail("USE doo");

    //custom location
    Path dbPath = new Path(whPath, new Random().nextInt() + "/mydb");
    whFs.mkdirs(dbPath, perm700);
    exec("CREATE DATABASE doo2 LOCATION '%s'", dbPath.toUri());
    whFs.mkdirs(dbPath, perm300); //revoke read
    execFail("DESCRIBE DATABASE doo2", dbPath.toUri());
    execFail("USE doo2");
  }

  @Test
  public void testShowTablesFail() throws Exception {
    whFs.setPermission(whPath, perm700);
    exec("CREATE DATABASE doo");
    exec("USE doo");
    whFs.setPermission(getDbPath("doo"), perm300); //revoke read
    execFail("SHOW TABLES");
    execFail("SHOW TABLE EXTENDED LIKE foo1");
  }

  @Test
  public void testTableOps() throws Exception {
    //default db
    exec("CREATE TABLE foo1 (foo INT) STORED AS RCFILE");
    exec("DESCRIBE foo1");
    exec("DROP TABLE foo1");

    //default db custom location
    Path tablePath = new Path(whPath, new Random().nextInt() + "/mytable");
    whFs.mkdirs(tablePath, perm700);
    exec("CREATE EXTERNAL TABLE foo2 (foo INT) STORED AS RCFILE LOCATION '%s'", tablePath);
    exec("DESCRIBE foo2");
    exec("DROP TABLE foo2");

    //default db custom non existing location
    exec("CREATE EXTERNAL TABLE foo3 (foo INT) STORED AS RCFILE LOCATION '%s/subpath'", tablePath);
    exec("DESCRIBE foo3");
    exec("DROP TABLE foo3");

    //non default db
    exec("CREATE DATABASE doo");
    exec("USE doo");

    exec("CREATE TABLE foo4 (foo INT) STORED AS RCFILE");
    exec("DESCRIBE foo4");
    exec("DROP TABLE foo4");

    //non-default db custom location
    tablePath = new Path(whPath, new Random().nextInt() + "/mytable");
    whFs.mkdirs(tablePath, perm700);
    exec("CREATE EXTERNAL TABLE foo5 (foo INT) STORED AS RCFILE LOCATION '%s'", tablePath);
    exec("DESCRIBE foo5");
    exec("DROP TABLE foo5");

    //non-default db custom non existing location
    exec("CREATE EXTERNAL TABLE foo6 (foo INT) STORED AS RCFILE LOCATION '%s/subpath'", tablePath);
    exec("DESCRIBE foo6");
    exec("DROP TABLE foo6");

    exec("DROP TABLE IF EXISTS foo_non_exists");

    exec("CREATE TABLE foo1 (foo INT) STORED AS RCFILE");
    exec("DESCRIBE EXTENDED foo1");
    exec("DESCRIBE FORMATTED foo1");
    exec("DESCRIBE foo1.foo");

    //deep non-existing path for the table
    tablePath = new Path(whPath, new Random().nextInt() + "/mytable");
    whFs.mkdirs(tablePath, perm700);
    exec("CREATE EXTERNAL TABLE foo2 (foo INT) STORED AS RCFILE LOCATION '%s/a/a/a/'", tablePath);
  }

  @Test
  public void testCreateTableFail1() throws Exception {
    //default db
    whFs.mkdirs(whPath, perm500); //revoke w
    execFail("CREATE TABLE foo1 (foo INT) STORED AS RCFILE");
  }

  @Test
  public void testCreateTableFail2() throws Exception {
    //default db custom location
    Path tablePath = new Path(whPath, new Random().nextInt() + "/mytable");
    whFs.mkdirs(tablePath, perm500);
    execFail("CREATE EXTERNAL TABLE foo2 (foo INT) STORED AS RCFILE LOCATION '%s'", tablePath);

    //default db custom non existing location
    execFail("CREATE EXTERNAL TABLE foo3 (foo INT) STORED AS RCFILE LOCATION '%s/subpath'", tablePath);
  }

  @Test
  public void testCreateTableFail3() throws Exception {
    //non default db
    exec("CREATE DATABASE doo");
    whFs.setPermission(getDbPath("doo"), perm500);

    execFail("CREATE TABLE doo.foo4 (foo INT) STORED AS RCFILE");

    //non-default db custom location, permission to write to tablePath, but not on db path
    Path tablePath = new Path(whPath, new Random().nextInt() + "/mytable");
    whFs.mkdirs(tablePath, perm700);
    exec("USE doo");
    execFail("CREATE EXTERNAL TABLE foo5 (foo INT) STORED AS RCFILE LOCATION '%s'", tablePath);
  }

  @Test
  public void testCreateTableFail4() throws Exception {
    //non default db
    exec("CREATE DATABASE doo");

    //non-default db custom location
    Path tablePath = new Path(whPath, new Random().nextInt() + "/mytable");
    whFs.mkdirs(tablePath, perm500);
    execFail("CREATE EXTERNAL TABLE doo.foo5 (foo INT) STORED AS RCFILE LOCATION '%s'", tablePath);

    //non-default db custom non existing location
    execFail("CREATE EXTERNAL TABLE doo.foo6 (foo INT) STORED AS RCFILE LOCATION '%s/a/a/a/'", tablePath);
  }

  @Test
  public void testDropTableFail1() throws Exception {
    //default db
    exec("CREATE TABLE foo1 (foo INT) STORED AS RCFILE");
    whFs.mkdirs(getTablePath("default", "foo1"), perm500); //revoke w
    execFail("DROP TABLE foo1");
  }

  @Test
  public void testDropTableFail2() throws Exception {
    //default db custom location
    Path tablePath = new Path(whPath, new Random().nextInt() + "/mytable");
    exec("CREATE EXTERNAL TABLE foo2 (foo INT) STORED AS RCFILE LOCATION '%s'", tablePath);
    whFs.mkdirs(tablePath, perm500);
    execFail("DROP TABLE foo2");
  }

  @Test
  public void testDropTableFail4() throws Exception {
    //non default db
    exec("CREATE DATABASE doo");

    //non-default db custom location
    Path tablePath = new Path(whPath, new Random().nextInt() + "/mytable");

    exec("CREATE EXTERNAL TABLE doo.foo5 (foo INT) STORED AS RCFILE LOCATION '%s'", tablePath);
    whFs.mkdirs(tablePath, perm500);
    exec("USE doo"); //There is no DROP TABLE doo.foo5 support in Hive
    execFail("DROP TABLE foo5");
  }

  @Test
  public void testDescTableFail() throws Exception {
    //default db
    exec("CREATE TABLE foo1 (foo INT) STORED AS RCFILE");
    whFs.mkdirs(getTablePath("default", "foo1"), perm300); //revoke read
    execFail("DESCRIBE foo1");

    //default db custom location
    Path tablePath = new Path(whPath, new Random().nextInt() + "/mytable");
    whFs.mkdirs(tablePath, perm700);
    exec("CREATE EXTERNAL TABLE foo2 (foo INT) STORED AS RCFILE LOCATION '%s'", tablePath);
    whFs.mkdirs(tablePath, perm300); //revoke read
    execFail("DESCRIBE foo2");
  }

  @Test
  public void testAlterTableRename() throws Exception {
    exec("CREATE TABLE foo1 (foo INT) STORED AS RCFILE");
    exec("ALTER TABLE foo1 RENAME TO foo2");

    Path tablePath = new Path(whPath, new Random().nextInt() + "/mytable");
    exec("CREATE EXTERNAL TABLE foo3 (foo INT) STORED AS RCFILE LOCATION '%s'", tablePath);
    exec("ALTER TABLE foo3 RENAME TO foo4");
  }

  @Test
  public void testAlterTableRenameFail() throws Exception {
    exec("CREATE TABLE foo1 (foo INT) STORED AS RCFILE");
    whFs.mkdirs(getTablePath("default", "foo1"), perm500); //revoke write
    execFail("ALTER TABLE foo1 RENAME TO foo2");

    Path tablePath = new Path(whPath, new Random().nextInt() + "/mytable");
    exec("CREATE EXTERNAL TABLE foo3 (foo INT) STORED AS RCFILE LOCATION '%s'", tablePath);
    whFs.mkdirs(tablePath, perm500); //revoke write
    execFail("ALTER TABLE foo3 RENAME TO foo4");
  }

  @Test
  public void testAlterTableRelocate() throws Exception {
    exec("CREATE TABLE foo1 (foo INT) STORED AS RCFILE");
    Path tablePath = new Path(whPath, new Random().nextInt() + "/mytable");
    exec("ALTER TABLE foo1 SET LOCATION '%s'", tablePath.makeQualified(whFs));

    tablePath = new Path(whPath, new Random().nextInt() + "/mytable2");
    exec("CREATE EXTERNAL TABLE foo3 (foo INT) STORED AS RCFILE LOCATION '%s'",
        tablePath.makeQualified(whFs));
    tablePath = new Path(whPath, new Random().nextInt() + "/mytable2");
    exec("ALTER TABLE foo3 SET LOCATION '%s'", tablePath.makeQualified(whFs));
  }

  @Test
  public void testAlterTableRelocateFail() throws Exception {
    exec("CREATE TABLE foo1 (foo INT) STORED AS RCFILE");
    Path tablePath = new Path(whPath, new Random().nextInt() + "/mytable");
    whFs.mkdirs(tablePath, perm500); //revoke write
    execFail("ALTER TABLE foo1 SET LOCATION '%s'", tablePath.makeQualified(whFs));

    //dont have access to new table loc
    tablePath = new Path(whPath, new Random().nextInt() + "/mytable2");
    exec("CREATE EXTERNAL TABLE foo3 (foo INT) STORED AS RCFILE LOCATION '%s'",
        tablePath.makeQualified(whFs));
    tablePath = new Path(whPath, new Random().nextInt() + "/mytable2");
    whFs.mkdirs(tablePath, perm500); //revoke write
    execFail("ALTER TABLE foo3 SET LOCATION '%s'", tablePath.makeQualified(whFs));

    //have access to new table loc, but not old table loc
    tablePath = new Path(whPath, new Random().nextInt() + "/mytable3");
    exec("CREATE EXTERNAL TABLE foo4 (foo INT) STORED AS RCFILE LOCATION '%s'",
        tablePath.makeQualified(whFs));
    whFs.mkdirs(tablePath, perm500); //revoke write
    tablePath = new Path(whPath, new Random().nextInt() + "/mytable3");
    execFail("ALTER TABLE foo4 SET LOCATION '%s'", tablePath.makeQualified(whFs));
  }

  @Test
  public void testAlterTable() throws Exception {
    exec("CREATE TABLE foo1 (foo INT) PARTITIONED BY (b STRING) STORED AS TEXTFILE");
    exec("ALTER TABLE foo1 SET TBLPROPERTIES ('foo'='bar')");
    exec("ALTER TABLE foo1 SET SERDEPROPERTIES ('foo'='bar')");
    exec("ALTER TABLE foo1 ADD COLUMNS (foo2 INT)");
  }

  @Test
  public void testAddDropPartition() throws Exception {
    exec("CREATE TABLE foo1 (foo INT) PARTITIONED BY (b STRING) STORED AS TEXTFILE");
    exec("ALTER TABLE foo1 ADD PARTITION (b='2010-10-10')");
    exec("ALTER TABLE foo1 ADD IF NOT EXISTS PARTITION (b='2010-10-10')");
    String relPath = new Random().nextInt() + "/mypart";
    exec("ALTER TABLE foo1 ADD PARTITION (b='2010-10-11') LOCATION '%s'", relPath);

    exec("ALTER TABLE foo1 PARTITION (b='2010-10-10') SET FILEFORMAT RCFILE");

    exec("ALTER TABLE foo1 PARTITION (b='2010-10-10') SET FILEFORMAT INPUTFORMAT "
        + "'org.apache.hadoop.hive.ql.io.RCFileInputFormat' OUTPUTFORMAT "
        + "'org.apache.hadoop.hive.ql.io.RCFileOutputFormat' inputdriver "
        + "'mydriver' outputdriver 'yourdriver'");

    exec("ALTER TABLE foo1 DROP PARTITION (b='2010-10-10')");
    exec("ALTER TABLE foo1 DROP PARTITION (b='2010-10-11')");
  }

  @Test
  public void testAddPartitionFail1() throws Exception {
    exec("CREATE TABLE foo1 (foo INT) PARTITIONED BY (b STRING) STORED AS TEXTFILE");
    whFs.mkdirs(getTablePath("default", "foo1"), perm500);
    execFail("ALTER TABLE foo1 ADD PARTITION (b='2010-10-10')");
  }

  @Test
  public void testAddPartitionFail2() throws Exception {
    exec("CREATE TABLE foo1 (foo INT) PARTITIONED BY (b STRING) STORED AS TEXTFILE");
    String relPath = new Random().nextInt() + "/mypart";
    Path partPath = new Path(getTablePath("default", "foo1"), relPath);
    whFs.mkdirs(partPath, perm500);
    exec("ALTER TABLE foo1 ADD PARTITION (b='2010-10-10') LOCATION '%s'", partPath);
  }

  @Test
  public void testDropPartitionFail1() throws Exception {
    exec("CREATE TABLE foo1 (foo INT) PARTITIONED BY (b STRING) STORED AS TEXTFILE");
    exec("ALTER TABLE foo1 ADD PARTITION (b='2010-10-10')");
    whFs.mkdirs(getPartPath("b=2010-10-10", "default", "foo1"), perm500);
    execFail("ALTER TABLE foo1 DROP PARTITION (b='2010-10-10')");
  }

  @Test
  public void testDropPartitionFail2() throws Exception {
    exec("CREATE TABLE foo1 (foo INT) PARTITIONED BY (b STRING) STORED AS TEXTFILE");
    String relPath = new Random().nextInt() + "/mypart";
    Path partPath = new Path(getTablePath("default", "foo1"), relPath);
    whFs.mkdirs(partPath, perm700);
    exec("ALTER TABLE foo1 ADD PARTITION (b='2010-10-10') LOCATION '%s'", partPath);
    whFs.mkdirs(partPath, perm500); //revoke write
    execFail("ALTER TABLE foo1 DROP PARTITION (b='2010-10-10')");
  }

  @Test
  public void testAlterTableFail() throws Exception {
    exec("CREATE TABLE foo1 (foo INT) PARTITIONED BY (boo STRING) STORED AS TEXTFILE");
    whFs.mkdirs(getTablePath("default", "foo1"), perm500); //revoke write
    execFail("ALTER TABLE foo1 SET TBLPROPERTIES ('foo'='bar')");
    execFail("ALTER TABLE foo1 SET SERDEPROPERTIES ('foo'='bar')");
    execFail("ALTER TABLE foo1 ADD COLUMNS (foo2 INT)");
  }

  @Test
  public void testShowTables() throws Exception {
    exec("CREATE TABLE foo1 (foo INT) PARTITIONED BY (boo STRING) STORED AS TEXTFILE");
    exec("SHOW PARTITIONS foo1");

    whFs.mkdirs(getTablePath("default", "foo1"), perm300); //revoke read
    execFail("SHOW PARTITIONS foo1");
  }

  @Test
  public void testAlterTablePartRename() throws Exception {
    exec("CREATE TABLE foo1 (foo INT) PARTITIONED BY (b STRING) STORED AS RCFILE");
    Path loc = new Path(whPath, new Random().nextInt() + "/mypart");
    exec("ALTER TABLE foo1 ADD PARTITION (b='2010-10-16') LOCATION '%s'", loc);
    exec("ALTER TABLE foo1 PARTITION (b='2010-10-16') RENAME TO PARTITION (b='2010-10-17')");
  }

  @Test
  public void testAlterTablePartRenameFail() throws Exception {
    exec("CREATE TABLE foo1 (foo INT) PARTITIONED BY (b STRING) STORED AS RCFILE");
    Path loc = new Path(whPath, new Random().nextInt() + "/mypart");
    exec("ALTER TABLE foo1 ADD PARTITION (b='2010-10-16') LOCATION '%s'", loc);
    whFs.setPermission(loc, perm500); //revoke w
    execFail("ALTER TABLE foo1 PARTITION (b='2010-10-16') RENAME TO PARTITION (b='2010-10-17')");
  }

  @Test
  public void testAlterTablePartRelocate() throws Exception {
    exec("CREATE TABLE foo1 (foo INT) PARTITIONED BY (b STRING) STORED AS RCFILE");
    exec("ALTER TABLE foo1 ADD PARTITION (b='2010-10-16')");
    Path partPath = new Path(whPath, new Random().nextInt() + "/mypart");
    exec("ALTER TABLE foo1 PARTITION (b='2010-10-16') SET LOCATION '%s'", partPath.makeQualified(whFs));
  }

  @Test
  public void testAlterTablePartRelocateFail() throws Exception {
    exec("CREATE TABLE foo1 (foo INT) PARTITIONED BY (b STRING) STORED AS RCFILE");

    Path oldLoc = new Path(whPath, new Random().nextInt() + "/mypart");
    Path newLoc = new Path(whPath, new Random().nextInt() + "/mypart2");

    exec("ALTER TABLE foo1 ADD PARTITION (b='2010-10-16') LOCATION '%s'", oldLoc);
    whFs.mkdirs(oldLoc, perm500);
    execFail("ALTER TABLE foo1 PARTITION (b='2010-10-16') SET LOCATION '%s'", newLoc.makeQualified(whFs));
    whFs.mkdirs(oldLoc, perm700);
    whFs.mkdirs(newLoc, perm500);
    execFail("ALTER TABLE foo1 PARTITION (b='2010-10-16') SET LOCATION '%s'", newLoc.makeQualified(whFs));
  }

}
