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
import java.util.HashMap;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.security.authorization.AuthorizationPreEventListener;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveMetastoreAuthorizationProvider;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * TestDefaultHiveMetaStoreAuthorizationProvider. Test case for
 * DefaultHiveMetastoreAuthorizationProvider
 * using {@link org.apache.hadoop.hive.metastore.AuthorizationPreEventListener}
 *
 * Note that while we do use the hive driver to test, that is mostly for test
 * writing ease, and it has the same effect as using a metastore client directly
 * because we disable hive client-side authorization for this test, and only
 * turn on server-side auth.
 */
public class TestDefaultHiveMetastoreAuthorizationProvider extends TestCase {
  private HiveConf clientHiveConf;
  private HiveMetaStoreClient msc;
  private Driver driver;
  private UserGroupInformation ugi;

  @Override
  protected void setUp() throws Exception {

    super.setUp();

    int port = MetaStoreUtils.findFreePort();

    System.setProperty(HiveConf.ConfVars.METASTORE_PRE_EVENT_LISTENERS.varname,
        AuthorizationPreEventListener.class.getName());
    System.setProperty(HiveConf.ConfVars.HIVE_METASTORE_AUTHORIZATION_MANAGER.varname,
        DefaultHiveMetastoreAuthorizationProvider.class.getName());
    System.setProperty(HiveConf.ConfVars.HIVE_METASTORE_AUTHENTICATOR_MANAGER.varname,
        InjectableDummyAuthenticator.class.getName());
    System.setProperty(HiveConf.ConfVars.HIVE_AUTHORIZATION_TABLE_OWNER_GRANTS.varname, "");


    MetaStoreUtils.startMetaStore(port, ShimLoader.getHadoopThriftAuthBridge());

    clientHiveConf = new HiveConf(this.getClass());

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
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  private void validateCreateDb(Database expectedDb, String dbName) {
    assertEquals(expectedDb.getName(), dbName);
  }

  private void validateCreateTable(Table expectedTable, String tblName, String dbName) {
    assertNotNull(expectedTable);
    assertEquals(expectedTable.getTableName(),tblName);
    assertEquals(expectedTable.getDbName(),dbName);
  }

  public void testSimplePrivileges() throws Exception {
    String dbName = "smpdb";
    String tblName = "smptbl";

    String userName = ugi.getUserName();

    CommandProcessorResponse ret = driver.run("create database " + dbName);
    assertEquals(0,ret.getResponseCode());
    Database db = msc.getDatabase(dbName);

    validateCreateDb(db,dbName);

    driver.run("use " + dbName);
    ret = driver.run(
        String.format("create table %s (a string) partitioned by (b string)", tblName));

    assertEquals(1,ret.getResponseCode());
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
    ttbl.setPartitionKeys(new ArrayList<FieldSchema>());

    MetaException me = null;
    try {
      msc.createTable(ttbl);
    } catch (MetaException e){
      me = e;
    }
    assertNotNull(me);
    assertTrue(me.getMessage().indexOf("No privilege") != -1);

    driver.run("grant create on database "+dbName+" to user "+userName);

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

    assertEquals(1,ret.getResponseCode());

    ttbl.setTableName(tblName+"mal");
    me = null;
    try {
      msc.createTable(ttbl);
    } catch (MetaException e){
      me = e;
    }
    assertNotNull(me);
    assertTrue(me.getMessage().indexOf("No privilege") != -1);

    ret = driver.run("alter table "+tblName+" add partition (b='2011')");
    assertEquals(1,ret.getResponseCode());

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
    assertNotNull(me);
    assertTrue(me.getMessage().indexOf("No privilege") != -1);

    InjectableDummyAuthenticator.injectMode(false);

    ret = driver.run("alter table "+tblName+" add partition (b='2011')");
    assertEquals(0,ret.getResponseCode());

  }

}
