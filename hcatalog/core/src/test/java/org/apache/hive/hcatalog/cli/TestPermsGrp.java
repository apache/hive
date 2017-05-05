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
package org.apache.hive.hcatalog.cli;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
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
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hive.hcatalog.ExitException;
import org.apache.hive.hcatalog.NoExitSecurityManager;
import org.apache.hive.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestPermsGrp extends TestCase {

  private boolean isServerRunning = false;
  private int msPort;
  private HiveConf hcatConf;
  private Warehouse clientWH;
  private HiveMetaStoreClient msc;
  private static final Logger LOG = LoggerFactory.getLogger(TestPermsGrp.class);

  @Override
  protected void tearDown() throws Exception {
    System.setSecurityManager(securityManager);
  }

  @Override
  protected void setUp() throws Exception {

    if (isServerRunning) {
      return;
    }


    msPort = MetaStoreUtils.findFreePort();
    MetaStoreUtils.startMetaStore(msPort, ShimLoader.getHadoopThriftAuthBridge());

    isServerRunning = true;

    securityManager = System.getSecurityManager();
    System.setSecurityManager(new NoExitSecurityManager());

    hcatConf = new HiveConf(this.getClass());
    hcatConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://127.0.0.1:" + msPort);
    hcatConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    hcatConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTFAILURERETRIES, 3);

    hcatConf.set(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname, HCatSemanticAnalyzer.class.getName());
    hcatConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hcatConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hcatConf.setTimeVar(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, 60, TimeUnit.SECONDS);
    hcatConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    clientWH = new Warehouse(hcatConf);
    msc = new HiveMetaStoreClient(hcatConf);
    System.setProperty(HiveConf.ConfVars.PREEXECHOOKS.varname, " ");
    System.setProperty(HiveConf.ConfVars.POSTEXECHOOKS.varname, " ");
  }

  public void testCustomPerms() throws Exception {

    String dbName = MetaStoreUtils.DEFAULT_DATABASE_NAME;
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
      try {
        callHCatCli(new String[]{"-e", "create table simptbl (name string) stored as RCFILE", "-p", "rwx-wx---"});
      } catch (Exception e) {
        assertTrue(e instanceof ExitException);
        assertEquals(((ExitException) e).getStatus(), 0);
      }
      dfsPath = clientWH.getDefaultTablePath(db, tblName);
      assertTrue(dfsPath.getFileSystem(hcatConf).getFileStatus(dfsPath).getPermission().equals(FsPermission.valueOf("drwx-wx---")));

      cleanupTbl(dbName, tblName, typeName);

      // User specified perms in invalid format.
      hcatConf.set(HCatConstants.HCAT_PERMS, "rwx");
      // make sure create table fails.
      try {
        callHCatCli(new String[]{"-e", "create table simptbl (name string) stored as RCFILE", "-p", "rwx"});
        assert false;
      } catch (Exception me) {
        assertTrue(me instanceof ExitException);
      }
      // No physical dir gets created.
      dfsPath = clientWH.getDefaultTablePath(db, tblName);
      try {
        dfsPath.getFileSystem(hcatConf).getFileStatus(dfsPath);
        assert false;
      } catch (Exception fnfe) {
        assertTrue(fnfe instanceof FileNotFoundException);
      }

      // And no metadata gets created.
      try {
        msc.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tblName);
        assert false;
      } catch (Exception e) {
        assertTrue(e instanceof NoSuchObjectException);
        assertEquals("default.simptbl table not found", e.getMessage());
      }

      // test for invalid group name
      hcatConf.set(HCatConstants.HCAT_PERMS, "drw-rw-rw-");
      hcatConf.set(HCatConstants.HCAT_GROUP, "THIS_CANNOT_BE_A_VALID_GRP_NAME_EVER");

      try {
        // create table must fail.
        callHCatCli(new String[]{"-e", "create table simptbl (name string) stored as RCFILE", "-p", "rw-rw-rw-", "-g", "THIS_CANNOT_BE_A_VALID_GRP_NAME_EVER"});
        assert false;
      } catch (Exception me) {
        assertTrue(me instanceof SecurityException);
      }

      try {
        // no metadata should get created.
        msc.getTable(dbName, tblName);
        assert false;
      } catch (Exception e) {
        assertTrue(e instanceof NoSuchObjectException);
        assertEquals("default.simptbl table not found", e.getMessage());
      }
      try {
        // neither dir should get created.
        dfsPath.getFileSystem(hcatConf).getFileStatus(dfsPath);
        assert false;
      } catch (Exception e) {
        assertTrue(e instanceof FileNotFoundException);
      }

    } catch (Exception e) {
      LOG.error("testCustomPerms failed.", e);
      throw e;
    }
  }

  private void callHCatCli(String[] args) {
    List<String> argsList = new ArrayList<String>();
    argsList.add("-Dhive.support.concurrency=false");
    argsList
        .add("-Dhive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    argsList.addAll(Arrays.asList(args));
    HCatCli.main(argsList.toArray(new String[]{}));
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
