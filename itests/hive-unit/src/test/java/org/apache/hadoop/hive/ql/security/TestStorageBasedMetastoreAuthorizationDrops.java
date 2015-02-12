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

package org.apache.hadoop.hive.ql.security;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.HadoopShims.MiniDFSShim;
import org.apache.hadoop.hive.shims.Utils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test cases focusing on drop table permission checks
 */
public class TestStorageBasedMetastoreAuthorizationDrops extends StorageBasedMetastoreTestBase {

  protected static MiniDFSShim dfs = null;

  @Override
  protected HiveConf createHiveConf() throws Exception {
    // Hadoop FS ACLs do not work with LocalFileSystem, so set up MiniDFS.
    HiveConf conf = super.createHiveConf();

    String currentUserName = Utils.getUGI().getShortUserName();
    conf.set("hadoop.proxyuser." + currentUserName + ".groups", "*");
    conf.set("hadoop.proxyuser." + currentUserName + ".hosts", "*");
    dfs = ShimLoader.getHadoopShims().getMiniDfs(conf, 4, true, null);
    FileSystem fs = dfs.getFileSystem();

    Path warehouseDir = new Path(new Path(fs.getUri()), "/warehouse");
    fs.mkdirs(warehouseDir);
    conf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, warehouseDir.toString());
    conf.setBoolVar(HiveConf.ConfVars.HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS, true);

    // Set up scratch directory
    Path scratchDir = new Path(new Path(fs.getUri()), "/scratchdir");
    conf.setVar(HiveConf.ConfVars.SCRATCHDIR, scratchDir.toString());

    return conf;
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();

    if (dfs != null) {
      dfs.shutdown();
      dfs = null;
    }
  }

  @Test
  public void testDropDatabase() throws Exception {
    dropDatabaseByOtherUser("-rwxrwxrwx", 0);
    dropDatabaseByOtherUser("-rwxrwxrwt", 1);
  }

  /**
   * Creates db and tries to drop as 'other' user
   * @param perm - permission for warehouse dir
   * @param expectedRet - expected return code for drop by other user
   * @throws Exception
   */
  public void dropDatabaseByOtherUser(String perm, int expectedRet) throws Exception {
    String dbName = getTestDbName();
    setPermissions(clientHiveConf.getVar(ConfVars.METASTOREWAREHOUSE), perm);

    CommandProcessorResponse resp = driver.run("create database " + dbName);
    Assert.assertEquals(0, resp.getResponseCode());
    Database db = msc.getDatabase(dbName);
    validateCreateDb(db, dbName);

    InjectableDummyAuthenticator.injectMode(true);


    resp = driver.run("drop database " + dbName);
    Assert.assertEquals(expectedRet, resp.getResponseCode());

  }

  @Test
  public void testDropTable() throws Exception {
    dropTableByOtherUser("-rwxrwxrwx", 0);
    dropTableByOtherUser("-rwxrwxrwt", 1);
  }

  /**
   * @param perm dir permission for database dir
   * @param expectedRet expected return code on drop table
   * @throws Exception
   */
  public void dropTableByOtherUser(String perm, int expectedRet) throws Exception {
    String dbName = getTestDbName();
    String tblName = getTestTableName();
    setPermissions(clientHiveConf.getVar(ConfVars.METASTOREWAREHOUSE), "-rwxrwxrwx");

    CommandProcessorResponse resp = driver.run("create database " + dbName);
    Assert.assertEquals(0, resp.getResponseCode());
    Database db = msc.getDatabase(dbName);
    validateCreateDb(db, dbName);

    setPermissions(db.getLocationUri(), perm);

    String dbDotTable = dbName + "." + tblName;
    resp = driver.run("create table " + dbDotTable + "(i int)");
    Assert.assertEquals(0, resp.getResponseCode());


    InjectableDummyAuthenticator.injectMode(true);
    resp = driver.run("drop table " + dbDotTable);
    Assert.assertEquals(expectedRet, resp.getResponseCode());
  }

  /**
   * Drop view should not be blocked by SBA. View will not have any location to drop.
   * @throws Exception
   */
  @Test
  public void testDropView() throws Exception {
    String dbName = getTestDbName();
    String tblName = getTestTableName();
    String viewName = "view" + tblName;
    setPermissions(clientHiveConf.getVar(ConfVars.METASTOREWAREHOUSE), "-rwxrwxrwx");

    CommandProcessorResponse resp = driver.run("create database " + dbName);
    Assert.assertEquals(0, resp.getResponseCode());
    Database db = msc.getDatabase(dbName);
    validateCreateDb(db, dbName);

    setPermissions(db.getLocationUri(), "-rwxrwxrwt");

    String dbDotTable = dbName + "." + tblName;
    resp = driver.run("create table " + dbDotTable + "(i int)");
    Assert.assertEquals(0, resp.getResponseCode());

    String dbDotView = dbName + "." + viewName;
    resp = driver.run("create view " + dbDotView + " as select * from " +  dbDotTable);
    Assert.assertEquals(0, resp.getResponseCode());

    resp = driver.run("drop view " + dbDotView);
    Assert.assertEquals(0, resp.getResponseCode());

    resp = driver.run("drop table " + dbDotTable);
    Assert.assertEquals(0, resp.getResponseCode());
  }

  @Test
  public void testDropPartition() throws Exception {
    dropPartitionByOtherUser("-rwxrwxrwx", 0);
    dropPartitionByOtherUser("-rwxrwxrwt", 1);
  }

  /**
   * @param perm permissions for table dir
   * @param expectedRet expected return code
   * @throws Exception
   */
  public void dropPartitionByOtherUser(String perm, int expectedRet) throws Exception {
    String dbName = getTestDbName();
    String tblName = getTestTableName();
    setPermissions(clientHiveConf.getVar(ConfVars.METASTOREWAREHOUSE), "-rwxrwxrwx");

    CommandProcessorResponse resp = driver.run("create database " + dbName);
    Assert.assertEquals(0, resp.getResponseCode());
    Database db = msc.getDatabase(dbName);
    validateCreateDb(db, dbName);
    setPermissions(db.getLocationUri(), "-rwxrwxrwx");

    String dbDotTable = dbName + "." + tblName;
    resp = driver.run("create table " + dbDotTable + "(i int) partitioned by (b string)");
    Assert.assertEquals(0, resp.getResponseCode());
    Table tab = msc.getTable(dbName, tblName);
    setPermissions(tab.getSd().getLocation(), perm);

    resp = driver.run("alter table " + dbDotTable + " add partition (b='2011')");
    Assert.assertEquals(0, resp.getResponseCode());

    InjectableDummyAuthenticator.injectMode(true);
    resp = driver.run("alter table " + dbDotTable + " drop partition (b='2011')");
    Assert.assertEquals(expectedRet, resp.getResponseCode());
  }

}
