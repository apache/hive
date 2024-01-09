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

package org.apache.hadoop.hive.ql.security;

import java.util.ArrayList;
import java.util.List;



import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

/**
 * TestClientSideAuthorizationProvider : Simple base test for client side
 * Authorization Providers. By default, tests DefaultHiveAuthorizationProvider
 */
public class TestClientSideAuthorizationProvider {
  protected HiveConf clientHiveConf;
  protected HiveMetaStoreClient msc;
  protected IDriver driver;
  protected UserGroupInformation ugi;


  protected String getAuthorizationProvider(){
    return DefaultHiveAuthorizationProvider.class.getName();
  }


  @Before
  public void setUp() throws Exception {



    // Turn off metastore-side authorization
    System.setProperty(HiveConf.ConfVars.METASTORE_PRE_EVENT_LISTENERS.varname,
        "");

    int port = MetaStoreTestUtils.startMetaStoreWithRetry();

    clientHiveConf = new HiveConf(this.getClass());

    // Turn on client-side authorization
    clientHiveConf.setBoolVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED,true);
    clientHiveConf.set(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER.varname,
        getAuthorizationProvider());
    clientHiveConf.set(HiveConf.ConfVars.HIVE_AUTHENTICATOR_MANAGER.varname,
        InjectableDummyAuthenticator.class.getName());
    clientHiveConf.set(HiveConf.ConfVars.HIVE_AUTHORIZATION_TABLE_OWNER_GRANTS.varname, "");
    clientHiveConf.setVar(HiveConf.ConfVars.HIVE_MAPRED_MODE, "nonstrict");
    clientHiveConf.setVar(HiveConf.ConfVars.METASTORE_URIS, "thrift://localhost:" + port);
    clientHiveConf.setIntVar(HiveConf.ConfVars.METASTORE_THRIFT_CONNECTION_RETRIES, 3);
    clientHiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");

    clientHiveConf.set(HiveConf.ConfVars.PRE_EXEC_HOOKS.varname, "");
    clientHiveConf.set(HiveConf.ConfVars.POST_EXEC_HOOKS.varname, "");

    ugi = Utils.getUGI();

    SessionState.start(new CliSessionState(clientHiveConf));
    msc = new HiveMetaStoreClient(clientHiveConf);
    driver = DriverFactory.newDriver(clientHiveConf);
  }

  @After
  public void tearDown() throws Exception {

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

  @Test
  public void testSimplePrivileges() throws Exception {
    String dbName = getTestDbName();
    String tblName = getTestTableName();

    String userName = ugi.getUserName();

    allowCreateDatabase(userName);

    driver.run("create database " + dbName);
    Database db = msc.getDatabase(dbName);
    String dbLocn = db.getLocationUri();

    disallowCreateDatabase(userName);

    validateCreateDb(db,dbName);
    disallowCreateInDb(dbName, userName, dbLocn);

    driver.run("use " + dbName);
    try {
      driver.run(String.format("create table %s (a string) partitioned by (b string)", tblName));
    } catch (CommandProcessorException e) {
      // failure from not having permissions to create table
      assertNoPrivileges(e);
    }

    allowCreateInDb(dbName, userName, dbLocn);

    driver.run("use " + dbName);
    driver.run(String.format("create table %s (a string) partitioned by (b string)", tblName));

    Table tbl = msc.getTable(dbName, tblName);

    validateCreateTable(tbl,tblName, dbName);

    String fakeUser = "mal";
    List<String> fakeGroupNames = new ArrayList<String>();
    fakeGroupNames.add("groupygroup");

    InjectableDummyAuthenticator.injectUserName(fakeUser);
    InjectableDummyAuthenticator.injectGroupNames(fakeGroupNames);
    InjectableDummyAuthenticator.injectMode(true);

    allowSelectOnTable(tbl.getTableName(), fakeUser, tbl.getSd().getLocation());
    driver.run(String.format("select * from %s limit 10", tblName));

    try {
      driver.run(String.format("create table %s (a string) partitioned by (b string)", tblName+"mal"));
    } catch (CommandProcessorException e) {
      assertNoPrivileges(e);
    }

    disallowCreateInTbl(tbl.getTableName(), userName, tbl.getSd().getLocation());

    try {
      driver.run("alter table "+tblName+" add partition (b='2011')");
    } catch (CommandProcessorException e) {
      assertNoPrivileges(e);
    }

    InjectableDummyAuthenticator.injectMode(false);
    allowCreateInTbl(tbl.getTableName(), userName, tbl.getSd().getLocation());

    driver.run("alter table "+tblName+" add partition (b='2011')");

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

  protected void allowCreateDatabase(String userName)
      throws Exception {
    driver.run("grant create to user "+userName);
  }

  protected void disallowCreateDatabase(String userName)
      throws Exception {
    driver.run("revoke create from user "+userName);
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

  protected void allowSelectOnTable(String tblName, String userName, String location)
      throws Exception {
    driver.run("grant select on table "+tblName+" to user "+userName);
  }

  protected void assertNoPrivileges(CommandProcessorException ret){
    assertNotNull(ret);
    assertFalse(0 == ret.getResponseCode());
    assertTrue(ret.getMessage().indexOf("No privilege") != -1);
  }


}
