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

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * TestClientSideAuthorizationProvider : Simple base test for client side
 * Authorization Providers. By default, tests DefaultHiveAuthorizationProvider
 */
public class TestClientSideAuthorizationProvider extends TestCase {
  protected HiveConf clientHiveConf;
  protected HiveMetaStoreClient msc;
  protected Driver driver;
  protected UserGroupInformation ugi;


  protected String getAuthorizationProvider(){
    return DefaultHiveAuthorizationProvider.class.getName();
  }


  @Override
  protected void setUp() throws Exception {

    super.setUp();

    int port = MetaStoreUtils.findFreePort();

    // Turn off metastore-side authorization
    System.setProperty(HiveConf.ConfVars.METASTORE_PRE_EVENT_LISTENERS.varname,
        "");

    MetaStoreUtils.startMetaStore(port, ShimLoader.getHadoopThriftAuthBridge());

    clientHiveConf = new HiveConf(this.getClass());

    // Turn on client-side authorization
    clientHiveConf.setBoolVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED,true);
    clientHiveConf.set(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER.varname,
        getAuthorizationProvider());
    clientHiveConf.set(HiveConf.ConfVars.HIVE_AUTHENTICATOR_MANAGER.varname,
        InjectableDummyAuthenticator.class.getName());
    clientHiveConf.set(HiveConf.ConfVars.HIVE_AUTHORIZATION_TABLE_OWNER_GRANTS.varname, "");

    clientHiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + port);
    clientHiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    clientHiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");

    clientHiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    clientHiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");

    ugi = ShimLoader.getHadoopShims().getUGIForConf(clientHiveConf);

    SessionState.start(new CliSessionState(clientHiveConf));
    msc = new HiveMetaStoreClient(clientHiveConf, null);
    driver = new Driver(clientHiveConf);
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  private void validateCreateDb(Database expectedDb, String dbName) {
    assertEquals(expectedDb.getName().toLowerCase(), dbName.toLowerCase());
  }

  private void validateCreateTable(Table expectedTable, String tblName, String dbName) {
    assertNotNull(expectedTable);
    assertEquals(expectedTable.getTableName().toLowerCase(),tblName.toLowerCase());
    assertEquals(expectedTable.getDbName().toLowerCase(),dbName.toLowerCase());
  }

  protected String getTestDbName(){
    return "smp_cl_db";
  }

  protected String getTestTableName(){
    return "smp_cl_tbl";
  }

  public void testSimplePrivileges() throws Exception {
    String dbName = getTestDbName();
    String tblName = getTestTableName();

    String userName = ugi.getUserName();

    CommandProcessorResponse ret = driver.run("create database " + dbName);
    assertEquals(0,ret.getResponseCode());
    Database db = msc.getDatabase(dbName);
    String dbLocn = db.getLocationUri();

    validateCreateDb(db,dbName);
    disallowCreateInDb(dbName, userName, dbLocn);

    driver.run("use " + dbName);
    ret = driver.run(
        String.format("create table %s (a string) partitioned by (b string)", tblName));

    // failure from not having permissions to create table
    assertNoPrivileges(ret);

    allowCreateInDb(dbName, userName, dbLocn);

    driver.run("use " + dbName);
    ret = driver.run(
        String.format("create table %s (a string) partitioned by (b string)", tblName));

    assertEquals(0,ret.getResponseCode()); // now it succeeds.
    Table tbl = msc.getTable(dbName, tblName);

    validateCreateTable(tbl,tblName, dbName);

    String fakeUser = "mal";
    List<String> fakeGroupNames = new ArrayList<String>();
    fakeGroupNames.add("groupygroup");

    InjectableDummyAuthenticator.injectUserName(fakeUser);
    InjectableDummyAuthenticator.injectGroupNames(fakeGroupNames);
    InjectableDummyAuthenticator.injectMode(true);

    ret = driver.run(
        String.format("create table %s (a string) partitioned by (b string)", tblName+"mal"));

    assertNoPrivileges(ret);

    disallowCreateInTbl(tbl.getTableName(), userName, tbl.getSd().getLocation());
    ret = driver.run("alter table "+tblName+" add partition (b='2011')");
    assertNoPrivileges(ret);

    InjectableDummyAuthenticator.injectMode(false);
    allowCreateInTbl(tbl.getTableName(), userName, tbl.getSd().getLocation());

    ret = driver.run("alter table "+tblName+" add partition (b='2011')");
    assertEquals(0,ret.getResponseCode());

    allowDropOnTable(tblName, userName, tbl.getSd().getLocation());
    allowDropOnDb(dbName,userName,db.getLocationUri());
    driver.run("drop database if exists "+getTestDbName()+" cascade");

  }

  protected void allowCreateInTbl(String tableName, String userName, String location)
      throws Exception{
    driver.run("grant create on table "+tableName+" to user "+userName);
  }

  protected void disallowCreateInTbl(String tableName, String userName, String location)
      throws Exception {
    // nothing needed here by default
  }


  protected void allowCreateInDb(String dbName, String userName, String location)
      throws Exception {
    driver.run("grant create on database "+dbName+" to user "+userName);
  }

  protected void disallowCreateInDb(String dbName, String userName, String location)
      throws Exception {
    // nothing needed here by default
  }

  protected void allowDropOnTable(String tblName, String userName, String location)
      throws Exception {
    driver.run("grant drop on table "+tblName+" to user "+userName);
  }

  protected void allowDropOnDb(String dbName, String userName, String location)
      throws Exception {
    driver.run("grant drop on database "+dbName+" to user "+userName);
  }

  protected void assertNoPrivileges(CommandProcessorResponse ret){
    assertNotNull(ret);
    assertFalse(0 == ret.getResponseCode());
    assertTrue(ret.getErrorMessage().indexOf("No privilege") != -1);
  }


}
