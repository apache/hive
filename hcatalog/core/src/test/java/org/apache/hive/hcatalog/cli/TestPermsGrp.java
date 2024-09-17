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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.cli;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.security.Policy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hive.hcatalog.DerbyPolicy;
import org.apache.hive.hcatalog.NoExitSecurityManager;
import org.apache.hive.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

/**
 * TestPermsGrp.
 */
public class TestPermsGrp {

  private boolean isServerRunning = false;
  private HiveConf hcatConf;
  private Warehouse clientWH;
  private HiveMetaStoreClient msc;
  private static final Logger LOG = LoggerFactory.getLogger(TestPermsGrp.class);

  @After
  public void tearDown() throws Exception {
    System.setSecurityManager(securityManager);
  }

  @Before
  public void setUp() throws Exception {

    if (isServerRunning) {
      return;
    }

    hcatConf = new HiveConf(this.getClass());
    MetaStoreTestUtils.startMetaStoreWithRetry(hcatConf);

    isServerRunning = true;

    securityManager = System.getSecurityManager();
    System.setSecurityManager(new NoExitSecurityManager());
    Policy.setPolicy(new DerbyPolicy());

    hcatConf.setIntVar(HiveConf.ConfVars.METASTORE_THRIFT_CONNECTION_RETRIES, 3);
    hcatConf.setIntVar(HiveConf.ConfVars.METASTORE_THRIFT_FAILURE_RETRIES, 3);
    hcatConf.setTimeVar(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, 60, TimeUnit.SECONDS);
    hcatConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    hcatConf.set(HiveConf.ConfVars.METASTORE_WAREHOUSE.varname,
        MetastoreConf.getVar(hcatConf, MetastoreConf.ConfVars.WAREHOUSE));
    hcatConf.set(HiveConf.ConfVars.METASTORE_CONNECT_URL_KEY.varname,
        MetastoreConf.getVar(hcatConf, MetastoreConf.ConfVars.CONNECT_URL_KEY));
    hcatConf.set(HiveConf.ConfVars.METASTORE_URIS.varname,
        MetastoreConf.getVar(hcatConf, MetastoreConf.ConfVars.THRIFT_URIS));
    clientWH = new Warehouse(hcatConf);
    msc = new HiveMetaStoreClient(hcatConf);
  }

  @Test
  public void testCustomPerms() throws Exception {

    String dbName = Warehouse.DEFAULT_DATABASE_NAME;
    String tblName = "simptbl";
    String typeName = "Person";

    try {

      // Lets first test for default permissions, this is the case when user specified nothing.
      Table tbl = getTable(dbName, tblName, typeName);
      msc.createTable(tbl);
      Database db = Hive.get(hcatConf).getDatabase(dbName);
      Path dfsPath = clientWH.getDefaultTablePath(db, tblName);
      cleanupTbl(dbName, tblName, typeName);

      // Next user did specify perms.
      int ret = callHCatCli(new String[]{"-e", "create table simptbl (name string) stored as RCFILE",
          "-p", "rwx-wx---"});
      assertEquals(ret, 0);
      dfsPath = clientWH.getDefaultTablePath(db, tblName);
      assertEquals(FsPermission.valueOf("drwx-wx---"), dfsPath.getFileSystem(hcatConf).getFileStatus(dfsPath).getPermission());

      cleanupTbl(dbName, tblName, typeName);

      // User specified perms in invalid format.
      hcatConf.set(HCatConstants.HCAT_PERMS, "rwx");
      // make sure create table fails.
      ret = callHCatCli(new String[]{"-e", "create table simptbl (name string) stored as RCFILE", "-p", "rwx"});
      assertFalse(ret == 0);

      // No physical dir gets created.
      dfsPath = clientWH.getDefaultTablePath(db, tblName);
      try {
        dfsPath.getFileSystem(hcatConf).getFileStatus(dfsPath);
        fail();
      } catch (Exception fnfe) {
        assertTrue(fnfe instanceof FileNotFoundException);
      }

      // And no metadata gets created.
      try {
        msc.getTable(Warehouse.DEFAULT_DATABASE_NAME, tblName);
        fail();
      } catch (Exception e) {
        assertTrue(e instanceof NoSuchObjectException);
        assertEquals("hive.default.simptbl table not found", e.getMessage());
      }

      // test for invalid group name
      hcatConf.set(HCatConstants.HCAT_PERMS, "drw-rw-rw-");
      hcatConf.set(HCatConstants.HCAT_GROUP, "THIS_CANNOT_BE_A_VALID_GRP_NAME_EVER");

      // create table must fail.
      ret = callHCatCli(new String[]{"-e", "create table simptbl (name string) stored as RCFILE", "-p", "rw-rw-rw-", "-g", "THIS_CANNOT_BE_A_VALID_GRP_NAME_EVER"});
      assertFalse(ret == 0);

      try {
        // no metadata should get created.
        msc.getTable(dbName, tblName);
        fail();
      } catch (Exception e) {
        assertTrue(e instanceof NoSuchObjectException);
        assertEquals("hive.default.simptbl table not found", e.getMessage());
      }
      try {
        // neither dir should get created.
        dfsPath.getFileSystem(hcatConf).getFileStatus(dfsPath);
        fail();
      } catch (Exception e) {
        assertTrue(e instanceof FileNotFoundException);
      }

    } catch (Exception e) {
      LOG.error("testCustomPerms failed.", e);
      throw e;
    }
  }

  private int callHCatCli(String[] args) throws Exception {
    List<String> argsList = new ArrayList<String>();
    argsList.add(System.getProperty("java.home") + "/bin/java");
    argsList.add(HCatCli.class.getName());
    argsList.add("-Dhive.support.concurrency=false");
    argsList
        .add("-Dhive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    argsList.add("-D" + HiveConf.ConfVars.METASTORE_THRIFT_CONNECTION_RETRIES.varname + "=3");
    argsList.add("-D" + HiveConf.ConfVars.METASTORE_THRIFT_FAILURE_RETRIES.varname + "=3");
    argsList.add("-D" + HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT.varname + "=60");
    argsList.add("-D" + HiveConf.ConfVars.METASTORE_WAREHOUSE.varname + "="
        + MetastoreConf.getVar(hcatConf, MetastoreConf.ConfVars.WAREHOUSE));
    argsList.add("-D" + HiveConf.ConfVars.METASTORE_CONNECT_URL_KEY.varname + "="
        + MetastoreConf.getVar(hcatConf, MetastoreConf.ConfVars.CONNECT_URL_KEY));
    argsList.add("-D" + HiveConf.ConfVars.METASTORE_URIS.varname + "="
        + MetastoreConf.getVar(hcatConf, MetastoreConf.ConfVars.THRIFT_URIS));
    argsList.add("-D" + HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname + "=" + HCatSemanticAnalyzer.class.getName());
    argsList.add("-D" + HiveConf.ConfVars.PRE_EXEC_HOOKS.varname + "=");
    argsList.add("-D" + HiveConf.ConfVars.POST_EXEC_HOOKS.varname + "=");
    argsList.add("-D" + HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname + "=false");

    argsList.add("-D" + "test.warehouse.dir=" + System.getProperty("test.warehouse.dir"));
    argsList.addAll(Arrays.asList(args));
    ProcessBuilder builder = new ProcessBuilder().command(argsList.toArray(new String[] {}));
    builder.environment().put("CLASSPATH", System.getProperty("java.class.path"));

    Process p = builder.start();

    String line;
    BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
    while ((line = r.readLine()) != null) {
        System.out.println(line);
    }
    r.close();

    r = new BufferedReader(new InputStreamReader(p.getErrorStream()));
    while ((line = r.readLine()) != null) {
        System.err.println(line);
    }
    r.close();

    int ret = p.waitFor();

    return ret;
  }

  private void silentDropDatabase(String dbName) throws MetaException, TException {
    try {
      for (String tableName : msc.getTables(dbName, "*")) {
        msc.dropTable(dbName, tableName);
      }

    } catch (NoSuchObjectException e) {
    }
  }

  private void cleanupTbl(String dbName, String tblName, String typeName) throws NoSuchObjectException, MetaException, TException, InvalidOperationException {

    msc.dropTable(dbName, tblName);
    msc.dropType(typeName);
  }

  private Table getTable(String dbName, String tblName, String typeName) throws NoSuchObjectException, MetaException, TException, AlreadyExistsException, InvalidObjectException {

    msc.dropTable(dbName, tblName);
    silentDropDatabase(dbName);


    msc.dropType(typeName);
    Type typ1 = new Type();
    typ1.setName(typeName);
    typ1.setFields(new ArrayList<FieldSchema>(1));
    typ1.getFields().add(new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));
    msc.createType(typ1);

    Table tbl = new Table();
    tbl.setDbName(dbName);
    tbl.setTableName(tblName);
    StorageDescriptor sd = new StorageDescriptor();
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setName(tblName);
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());
    sd.setInputFormat(HiveInputFormat.class.getName());
    sd.setOutputFormat(HiveOutputFormat.class.getName());
    tbl.setSd(sd);
    sd.setCols(typ1.getFields());

    sd.setSerdeInfo(new SerDeInfo());
    return tbl;
  }


  private SecurityManager securityManager;

}
