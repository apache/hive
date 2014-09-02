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

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.security.authorization.AuthorizationPreEventListener;
import org.apache.hadoop.hive.ql.security.authorization.StorageBasedAuthorizationProvider;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Test cases focusing on drop table permission checks
 */
public class TestStorageBasedMetastoreAuthorizationDrops extends TestCase{
  protected HiveConf clientHiveConf;
  protected HiveMetaStoreClient msc;
  protected Driver driver;
  protected UserGroupInformation ugi;
  private static int objNum = 0;

  protected String getAuthorizationProvider(){
    return StorageBasedAuthorizationProvider.class.getName();
  }

  protected HiveConf createHiveConf() throws Exception {
    return new HiveConf(this.getClass());
  }

  @Override
  protected void setUp() throws Exception {

    super.setUp();

    int port = MetaStoreUtils.findFreePort();

    // Turn on metastore-side authorization
    System.setProperty(HiveConf.ConfVars.METASTORE_PRE_EVENT_LISTENERS.varname,
        AuthorizationPreEventListener.class.getName());
    System.setProperty(HiveConf.ConfVars.HIVE_METASTORE_AUTHORIZATION_MANAGER.varname,
        getAuthorizationProvider());
    System.setProperty(HiveConf.ConfVars.HIVE_METASTORE_AUTHENTICATOR_MANAGER.varname,
        InjectableDummyAuthenticator.class.getName());

    MetaStoreUtils.startMetaStore(port, ShimLoader.getHadoopThriftAuthBridge());

    clientHiveConf = createHiveConf();

    // Turn off client-side authorization
    clientHiveConf.setBoolVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED,false);

    clientHiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + port);
    clientHiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    clientHiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");

    clientHiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    clientHiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");

    ugi = ShimLoader.getHadoopShims().getUGIForConf(clientHiveConf);

    SessionState.start(new CliSessionState(clientHiveConf));
    msc = new HiveMetaStoreClient(clientHiveConf, null);
    driver = new Driver(clientHiveConf);

    setupFakeUser();
  }


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
  private void dropDatabaseByOtherUser(String perm, int expectedRet) throws Exception {
    String dbName = getTestDbName();
    setPermissions(clientHiveConf.getVar(ConfVars.METASTOREWAREHOUSE), perm);

    CommandProcessorResponse resp = driver.run("create database " + dbName);
    assertEquals(0, resp.getResponseCode());
    Database db = msc.getDatabase(dbName);
    validateCreateDb(db, dbName);

    InjectableDummyAuthenticator.injectMode(true);


    resp = driver.run("drop database " + dbName);
    assertEquals(expectedRet, resp.getResponseCode());

  }

  public void testDropTable() throws Exception {
    dropTableByOtherUser("-rwxrwxrwx", 0);
    dropTableByOtherUser("-rwxrwxrwt", 1);
  }

  /**
   * @param perm dir permission for database dir
   * @param expectedRet expected return code on drop table
   * @throws Exception
   */
  private void dropTableByOtherUser(String perm, int expectedRet) throws Exception {
    String dbName = getTestDbName();
    String tblName = getTestTableName();
    setPermissions(clientHiveConf.getVar(ConfVars.METASTOREWAREHOUSE), "-rwxrwxrwx");

    CommandProcessorResponse resp = driver.run("create database " + dbName);
    assertEquals(0, resp.getResponseCode());
    Database db = msc.getDatabase(dbName);
    validateCreateDb(db, dbName);

    setPermissions(db.getLocationUri(), perm);

    String dbDotTable = dbName + "." + tblName;
    resp = driver.run("create table " + dbDotTable + "(i int)");
    assertEquals(0, resp.getResponseCode());


    InjectableDummyAuthenticator.injectMode(true);
    resp = driver.run("drop table " + dbDotTable);
    assertEquals(expectedRet, resp.getResponseCode());
  }


  public void testDropPartition() throws Exception {
    dropPartitionByOtherUser("-rwxrwxrwx", 0);
    dropPartitionByOtherUser("-rwxrwxrwt", 1);
  }

  /**
   * @param perm permissions for table dir
   * @param expectedRet expected return code
   * @throws Exception
   */
  private void dropPartitionByOtherUser(String perm, int expectedRet) throws Exception {
    String dbName = getTestDbName();
    String tblName = getTestTableName();
    setPermissions(clientHiveConf.getVar(ConfVars.METASTOREWAREHOUSE), "-rwxrwxrwx");

    CommandProcessorResponse resp = driver.run("create database " + dbName);
    assertEquals(0, resp.getResponseCode());
    Database db = msc.getDatabase(dbName);
    validateCreateDb(db, dbName);
    setPermissions(db.getLocationUri(), "-rwxrwxrwx");

    String dbDotTable = dbName + "." + tblName;
    resp = driver.run("create table " + dbDotTable + "(i int) partitioned by (b string)");
    assertEquals(0, resp.getResponseCode());
    Table tab = msc.getTable(dbName, tblName);
    setPermissions(tab.getSd().getLocation(), perm);

    resp = driver.run("alter table " + dbDotTable + " add partition (b='2011')");
    assertEquals(0, resp.getResponseCode());

    InjectableDummyAuthenticator.injectMode(true);
    resp = driver.run("alter table " + dbDotTable + " drop partition (b='2011')");
    assertEquals(expectedRet, resp.getResponseCode());
  }

  private void setupFakeUser() {
    String fakeUser = "mal";
    List<String> fakeGroupNames = new ArrayList<String>();
    fakeGroupNames.add("groupygroup");

    InjectableDummyAuthenticator.injectUserName(fakeUser);
    InjectableDummyAuthenticator.injectGroupNames(fakeGroupNames);
    InjectableDummyAuthenticator.injectMode(true);
  }

  private String setupUser() {
    return ugi.getUserName();
  }

  private String getTestTableName() {
    return this.getClass().getSimpleName() + "tab" + ++objNum;
  }

  private String getTestDbName() {
    return this.getClass().getSimpleName() + "db" + ++objNum;
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    InjectableDummyAuthenticator.injectMode(false);
  }

  protected void setPermissions(String locn, String permissions) throws Exception {
    FileSystem fs = FileSystem.get(new URI(locn), clientHiveConf);
    fs.setPermission(new Path(locn), FsPermission.valueOf(permissions));
  }

  private void validateCreateDb(Database expectedDb, String dbName) {
    assertEquals(expectedDb.getName().toLowerCase(), dbName.toLowerCase());
  }

  private void validateCreateTable(Table expectedTable, String tblName, String dbName) {
    assertNotNull(expectedTable);
    assertEquals(expectedTable.getTableName().toLowerCase(),tblName.toLowerCase());
    assertEquals(expectedTable.getDbName().toLowerCase(),dbName.toLowerCase());
  }
}
