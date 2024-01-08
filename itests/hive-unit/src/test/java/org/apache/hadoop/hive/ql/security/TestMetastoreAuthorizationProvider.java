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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Collections;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.security.authorization.AuthorizationPreEventListener;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveMetastoreAuthorizationProvider;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

/**
 * TestHiveMetastoreAuthorizationProvider. Test case for
 * HiveMetastoreAuthorizationProvider, and by default,
 * for DefaultHiveMetaStoreAuthorizationProvider
 * using {@link org.apache.hadoop.hive.metastore.AuthorizationPreEventListener}
 * and {@link org.apache.hadoop.hive.}
 *
 * Note that while we do use the hive driver to test, that is mostly for test
 * writing ease, and it has the same effect as using a metastore client directly
 * because we disable hive client-side authorization for this test, and only
 * turn on server-side auth.
 *
 * This test is also intended to be extended to provide tests for other
 * authorization providers like StorageBasedAuthorizationProvider
 */
public class TestMetastoreAuthorizationProvider {
  protected HiveConf clientHiveConf;
  protected HiveMetaStoreClient msc;
  protected IDriver driver;
  protected UserGroupInformation ugi;


  protected String getAuthorizationProvider(){
    return DefaultHiveMetastoreAuthorizationProvider.class.getName();
  }

  protected HiveConf createHiveConf() throws Exception {
    return new HiveConf(this.getClass());
  }

  protected String getProxyUserName() {
    return null;
  }

  @Before
  public void setUp() throws Exception {



    // Turn on metastore-side authorization
    System.setProperty(HiveConf.ConfVars.METASTORE_PRE_EVENT_LISTENERS.varname,
        AuthorizationPreEventListener.class.getName());
    System.setProperty(HiveConf.ConfVars.HIVE_METASTORE_AUTHORIZATION_MANAGER.varname,
        getAuthorizationProvider());
    System.setProperty(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER.varname,
        getAuthorizationProvider());
    setupMetaStoreReadAuthorization();
    System.setProperty(HiveConf.ConfVars.HIVE_METASTORE_AUTHENTICATOR_MANAGER.varname,
        InjectableDummyAuthenticator.class.getName());
    System.setProperty(HiveConf.ConfVars.HIVE_AUTHORIZATION_TABLE_OWNER_GRANTS.varname, "");

    int port = MetaStoreTestUtils.startMetaStoreWithRetry();

    clientHiveConf = createHiveConf();

    // Turn off client-side authorization
    clientHiveConf.setBoolVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED,false);

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

  protected void setupMetaStoreReadAuthorization() {
    // read authorization does not work with default/legacy authorization mode
    // It is a chicken and egg problem granting select privilege to database, as the
    // grant statement would invoke get_database which needs select privilege
    System.setProperty(HiveConf.ConfVars.HIVE_METASTORE_AUTHORIZATION_AUTH_READS.varname, "false");
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
    return "smp_ms_db";
  }

  protected String getTestTableName(){
    return "smp_ms_tbl";
  }

  protected boolean isTestEnabled() {
    return true;
  }

  protected String setupUser() {
    return ugi.getUserName();
  }

  @Test
  public void testSimplePrivileges() throws Exception {
    if (!isTestEnabled()) {
      System.out.println("Skipping test " + this.getClass().getName());
      return;
    }
    String dbName = getTestDbName();
    String tblName = getTestTableName();
    String userName = setupUser();
    String loc = clientHiveConf.get(HiveConf.ConfVars.HIVE_METASTORE_WAREHOUSE_EXTERNAL.varname) + "/" + dbName;
    String mLoc = clientHiveConf.get(HiveConf.ConfVars.METASTORE_WAREHOUSE.varname) + "/" + dbName;
    allowCreateDatabase(userName);
    driver.run("create database " + dbName + " location '" + loc + "' managedlocation '" + mLoc + "'");
    Database db = msc.getDatabase(dbName);
    String dbLocn = db.getManagedLocationUri();
    validateCreateDb(db, dbName);
    allowCreateInDb(dbName, userName, dbLocn);
    disallowCreateInDb(dbName, userName, dbLocn);
    disallowCreateDatabase(userName);
    driver.run("use " + dbName);
    try {
      driver.run(String.format("create table %s (a string) partitioned by (b string)", tblName));
      assert false;
    } catch (CommandProcessorException e) {
      assertEquals(40000, e.getResponseCode());
    }

    // Even if table location is specified table creation should fail
    String tblNameLoc = tblName + "_loc";
    String tblLocation = new Path(dbLocn).getParent().toUri() + "/" + tblNameLoc;

    if (mayTestLocation()) {
      driver.run("use " + dbName);
      try {
        driver.run(String.format(
            "create table %s (a string) partitioned by (b string) location '" +tblLocation + "'", tblNameLoc));
      } catch (CommandProcessorException e) {
        assertEquals(40000, e.getResponseCode());
      }
    }

    // failure from not having permissions to create table
    ArrayList<FieldSchema> fields = new ArrayList<FieldSchema>(2);
    fields.add(new FieldSchema("a", serdeConstants.STRING_TYPE_NAME, ""));
    Table ttbl = new Table();
    ttbl.setDbName(dbName);
    ttbl.setTableName(tblName);
    StorageDescriptor sd = new StorageDescriptor();
    ttbl.setSd(sd);
    sd.setCols(fields);
    sd.setParameters(new HashMap<String, String>());
    sd.getParameters().put("test_param_1", "Use this for comments etc");
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setName(ttbl.getTableName());
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.getSerdeInfo().getParameters().put(
        org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT, "1");
    sd.getSerdeInfo().setSerializationLib(
        org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
    sd.setInputFormat(HiveInputFormat.class.getName());
    sd.setOutputFormat(HiveOutputFormat.class.getName());
    ttbl.setPartitionKeys(new ArrayList<FieldSchema>());

    MetaException me = null;
    try {
      msc.createTable(ttbl);
    } catch (MetaException e){
      me = e;
    }
    assertNoPrivileges(me);

    allowCreateInDb(dbName, userName, dbLocn);
    driver.run("use " + dbName);
    driver.run(String.format("create table %s (a string) partitioned by (b string)", tblName));

    Table tbl = msc.getTable(dbName, tblName);
    Assert.assertTrue(tbl.isSetId());
    tbl.unsetId();
    validateCreateTable(tbl, tblName, dbName);

    // Table creation should succeed even if location is specified
    if (mayTestLocation()) {
      driver.run("use " + dbName);
      driver.run(String.format(
          "create table %s (a string) partitioned by (b string) location '" + tblLocation + "'", tblNameLoc));
      Table tblLoc = msc.getTable(dbName, tblNameLoc);
      validateCreateTable(tblLoc, tblNameLoc, dbName);
    }

    String fakeUser = "mal";
    List<String> fakeGroupNames = new ArrayList<String>();
    fakeGroupNames.add("groupygroup");

    InjectableDummyAuthenticator.injectUserName(fakeUser);
    InjectableDummyAuthenticator.injectGroupNames(fakeGroupNames);
    InjectableDummyAuthenticator.injectMode(true);

    try {
      driver.run(String.format("create table %s (a string) partitioned by (b string)", tblName+"mal"));
    } catch (CommandProcessorException e) {
      assertEquals(40000, e.getResponseCode());
    }

    ttbl.setTableName(tblName+"mal");
    me = null;
    try {
      msc.createTable(ttbl);
    } catch (MetaException e){
      me = e;
    }
    assertNoPrivileges(me);

    allowCreateInTbl(tbl.getTableName(), userName, tbl.getSd().getLocation());
    disallowCreateInTbl(tbl.getTableName(), userName, tbl.getSd().getLocation());
    try {
      driver.run("alter table "+tblName+" add partition (b='2011')");
    } catch (CommandProcessorException e) {
      assertEquals(40000, e.getResponseCode());
    }

    List<String> ptnVals = new ArrayList<String>();
    ptnVals.add("b=2011");
    Partition tpart = new Partition();
    tpart.setDbName(dbName);
    tpart.setTableName(tblName);
    tpart.setValues(ptnVals);
    tpart.setParameters(new HashMap<String, String>());
    tpart.setSd(tbl.getSd().deepCopy());
    tpart.getSd().setSerdeInfo(tbl.getSd().getSerdeInfo().deepCopy());
    tpart.getSd().setLocation(tbl.getSd().getLocation() + "/tpart");

    me = null;
    try {
      msc.add_partition(tpart);
    } catch (MetaException e){
      me = e;
    }
    assertNoPrivileges(me);

    InjectableDummyAuthenticator.injectMode(false);
    allowCreateInTbl(tbl.getTableName(), userName, tbl.getSd().getLocation());

    driver.run("alter table "+tblName+" add partition (b='2011')");

    String proxyUserName = getProxyUserName();
    if (proxyUserName != null) {
      // for storage based authorization, user having proxy privilege should be allowed to do operation
      // even if the file permission is not there.
      InjectableDummyAuthenticator.injectUserName(proxyUserName);
      InjectableDummyAuthenticator.injectGroupNames(Collections.singletonList(proxyUserName));
      InjectableDummyAuthenticator.injectMode(true);
      disallowCreateInTbl(tbl.getTableName(), proxyUserName, tbl.getSd().getLocation());
      driver.run("alter table "+tblName+" add partition (b='2012')");
      InjectableDummyAuthenticator.injectMode(false);
    }

    allowDropOnTable(tblName, userName, tbl.getSd().getLocation());
    allowDropOnDb(dbName, userName, db.getLocationUri());
    driver.run("drop database if exists "+getTestDbName()+" cascade");

    InjectableDummyAuthenticator.injectUserName(userName);
    InjectableDummyAuthenticator.injectGroupNames(Arrays.asList(ugi.getGroupNames()));
    InjectableDummyAuthenticator.injectMode(true);
    allowCreateDatabase(userName);
    driver.run("create database " + dbName);
    db = msc.getDatabase(dbName);
    dbLocn = db.getLocationUri();
    allowCreateInDb(dbName, userName, dbLocn);
    dbLocn = db.getManagedLocationUri();
     if (dbLocn != null) {
       allowCreateInDb(dbName, userName, dbLocn);
     }
    tbl.setTableType("EXTERNAL_TABLE");
    msc.createTable(tbl);

    allowDropOnTable(tblName, userName, tbl.getSd().getLocation());
    disallowDropOnTable(tblName, userName, tbl.getSd().getLocation());
    try {
      driver.run("drop table "+tbl.getTableName());
    } catch (CommandProcessorException e) {
      assertEquals(40000, e.getResponseCode());
    }
  }

  protected boolean mayTestLocation() {
    return true;
  }

  protected void allowCreateDatabase(String userName)
      throws Exception {
    driver.run("grant create to user "+userName);
  }

  protected void disallowCreateDatabase(String userName)
      throws Exception {
    driver.run("revoke create from user "+userName);
  }

  protected void allowCreateInTbl(String tableName, String userName, String location)
      throws Exception{
    driver.run("grant create on table "+tableName+" to user "+userName);
  }

  protected void disallowCreateInTbl(String tableName, String userName, String location)
      throws Exception {
    driver.run("revoke create on table "+tableName+" from user "+userName);
  }


  protected void allowCreateInDb(String dbName, String userName, String location)
      throws Exception {
    driver.run("grant create on database "+dbName+" to user "+userName);
  }

  protected void disallowCreateInDb(String dbName, String userName, String location)
      throws Exception {
    driver.run("revoke create on database "+dbName+" from user "+userName);
  }

  protected void allowDropOnTable(String tblName, String userName, String location)
      throws Exception {
    driver.run("grant drop on table "+tblName+" to user "+userName);
  }

  protected void disallowDropOnTable(String tblName, String userName, String location)
      throws Exception {
    driver.run("revoke drop on table "+tblName+" from user "+userName);
  }

  protected void allowDropOnDb(String dbName, String userName, String location)
      throws Exception {
    driver.run("grant drop on database "+dbName+" to user "+userName);
  }

  protected void assertNoPrivileges(MetaException me){
    assertNotNull(me);
    assertTrue(me.getMessage().indexOf("No privilege") != -1);
  }

}
