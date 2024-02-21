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

package org.apache.hadoop.hive.metastore;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ReplChangeManager.RecycleType;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.EncryptionZoneUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 * TestMetaStoreAuthorization.
 */
public class TestMetaStoreMultipleEncryptionZones {
  private static HiveMetaStoreClient client;
  private static HiveConf hiveConf;
  private static Configuration conf;
  private static Warehouse warehouse;
  private static FileSystem warehouseFs;
  private static MiniDFSCluster miniDFSCluster;
  private static String cmroot;
  private static FileSystem fs;
  private static String cmrootEncrypted;
  private static String jksFile = System.getProperty("java.io.tmpdir") + "/test.jks";
  private static String cmrootFallBack;

  @BeforeClass
  public static void setUp() throws Exception {
    //Create secure cluster
    conf = new Configuration();
    conf.set("hadoop.security.key.provider.path", "jceks://file" + jksFile);
    conf.set("dfs.namenode.acls.enabled", "true");
    miniDFSCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    DFSTestUtil.createKey("test_key_cm", miniDFSCluster, conf);
    DFSTestUtil.createKey("test_key_db", miniDFSCluster, conf);
    hiveConf = new HiveConf(TestReplChangeManager.class);
    hiveConf.setBoolean(HiveConf.ConfVars.REPL_CM_ENABLED.varname, true);
    hiveConf.setInt(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 60);
    hiveConf.set(HiveConf.ConfVars.METASTORE_WAREHOUSE.varname,
            "hdfs://" + miniDFSCluster.getNameNode().getHostAndPort()
                    + HiveConf.ConfVars.METASTORE_WAREHOUSE.defaultStrVal);

    cmroot = "hdfs://" + miniDFSCluster.getNameNode().getHostAndPort() + "/cmroot";
    cmrootFallBack = "hdfs://" + miniDFSCluster.getNameNode().getHostAndPort() + "/cmrootFallback";
    cmrootEncrypted = "cmrootEncrypted";
    hiveConf.set(HiveConf.ConfVars.REPL_CM_DIR.varname, cmroot);
    hiveConf.set(HiveConf.ConfVars.REPL_CM_ENCRYPTED_DIR.varname, cmrootEncrypted);
    hiveConf.set(HiveConf.ConfVars.REPL_CM_FALLBACK_NONENCRYPTED_DIR.varname, cmrootFallBack);
    initReplChangeManager();

    try {
      client = new HiveMetaStoreClient(hiveConf);
    } catch (Throwable e) {
      System.err.println("Unable to open the metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw e;
    }
  }

  private static void initReplChangeManager() throws Exception{
    warehouse = new Warehouse(hiveConf);
    warehouseFs = warehouse.getWhRoot().getFileSystem(hiveConf);
    fs = new Path(cmroot).getFileSystem(hiveConf);
    fs.mkdirs(warehouse.getWhRoot());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    try {
      miniDFSCluster.shutdown();
      client.close();
    } catch (Throwable e) {
      System.err.println("Unable to close metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw e;
    }
  }

  @Test
  public void dropTableWithDifferentEncryptionZonesDifferentKey() throws Throwable {
    String dbName1 = "encrdbdiffkey1";
    String dbName2 = "encrdbdiffkey2";
    String tblName1 = "encrtbl1";
    String tblName2 = "encrtbl2";
    String typeName = "Person";

    silentDropDatabase(dbName1);
    silentDropDatabase(dbName2);
    new DatabaseBuilder()
            .setName(dbName1)
            .addParam("repl.source.for", "1, 2, 3")
            .create(client, hiveConf);

    new DatabaseBuilder()
            .setName(dbName2)
            .addParam("repl.source.for", "1, 2, 3")
            .create(client, hiveConf);

    client.dropType(typeName);
    Type typ1 = new Type();
    typ1.setName(typeName);
    typ1.setFields(new ArrayList<>(2));
    typ1.getFields().add(
            new FieldSchema("name", ColumnType.STRING_TYPE_NAME, ""));
    typ1.getFields().add(
            new FieldSchema("income", ColumnType.INT_TYPE_NAME, ""));
    client.createType(typ1);

    Path dirDb1 = new Path(warehouse.getWhRoot(), dbName1 +".db");
    warehouseFs.delete(dirDb1, true);
    warehouseFs.mkdirs(dirDb1);
    EncryptionZoneUtils.createEncryptionZone(dirDb1, "test_key_db", conf);
    Path dirTbl1 = new Path(dirDb1, tblName1);
    warehouseFs.mkdirs(dirTbl1);
    Path part11 = new Path(dirTbl1, "part1");
    createFile(part11, "testClearer11");

    Path dirDb2 = new Path(warehouse.getWhRoot(), dbName2 +".db");
    warehouseFs.delete(dirDb2, true);
    warehouseFs.mkdirs(dirDb2);
    EncryptionZoneUtils.createEncryptionZone(dirDb2, "test_key_cm", conf);
    Path dirTbl2 = new Path(dirDb2, tblName2);
    warehouseFs.mkdirs(dirTbl2);
    Path part12 = new Path(dirTbl2, "part1");
    createFile(part12, "testClearer12");

    new TableBuilder()
            .setDbName(dbName1)
            .setTableName(tblName1)
            .setCols(typ1.getFields())
            .setNumBuckets(1)
            .addBucketCol("name")
            .addStorageDescriptorParam("test_param_1", "Use this for comments etc")
            .create(client, hiveConf);

    Table tbl = client.getTable(dbName1, tblName1);
    Assert.assertNotNull(tbl);

    new TableBuilder()
            .setDbName(dbName2)
            .setTableName(tblName2)
            .setCols(typ1.getFields())
            .setNumBuckets(1)
            .addBucketCol("name")
            .addStorageDescriptorParam("test_param_1", "Use this for comments etc")
            .create(client, hiveConf);

    boolean exceptionThrown = false;
    try {
      client.dropTable(dbName1, tblName1);
    } catch (MetaException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("can't be moved from encryption zone"));
    }
    assertFalse(exceptionThrown);
    assertFalse(warehouseFs.exists(part11));
    try {
      client.getTable(dbName1, tblName1);
    } catch (NoSuchObjectException e) {
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);
    exceptionThrown = false;
    try {
      client.dropTable(dbName2, tblName2);
    } catch (MetaException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("can't be moved from encryption zone"));
    }
    assertFalse(exceptionThrown);
    assertFalse(warehouseFs.exists(part12));
    try {
      client.getTable(dbName2, tblName2);
    } catch (NoSuchObjectException e) {
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);


  }

  @Test
  public void dropTableWithTableAtEncryptionZoneRoot() throws Throwable {
    String dbName = "encrdbroot";
    String tblName1 = "encrtbl1";
    String tblName2 = "encrtbl2";
    String typeName = "Person";

    silentDropDatabase(dbName);
    new DatabaseBuilder()
            .setName(dbName)
            .addParam("repl.source.for", "1, 2, 3")
            .create(client, hiveConf);

    client.dropType(typeName);
    Type typ1 = new Type();
    typ1.setName(typeName);
    typ1.setFields(new ArrayList<>(2));
    typ1.getFields().add(
            new FieldSchema("name", ColumnType.STRING_TYPE_NAME, ""));
    typ1.getFields().add(
            new FieldSchema("income", ColumnType.INT_TYPE_NAME, ""));
    client.createType(typ1);

    new TableBuilder()
            .setDbName(dbName)
            .setTableName(tblName1)
            .setCols(typ1.getFields())
            .setNumBuckets(1)
            .addBucketCol("name")
            .addStorageDescriptorParam("test_param_1", "Use this for comments etc")
            .create(client, hiveConf);

    Table tbl = client.getTable(dbName, tblName1);
    Assert.assertNotNull(tbl);

    new TableBuilder()
            .setDbName(dbName)
            .setTableName(tblName2)
            .setCols(typ1.getFields())
            .setNumBuckets(1)
            .addBucketCol("name")
            .addStorageDescriptorParam("test_param_1", "Use this for comments etc")
            .create(client, hiveConf);

    Path dirDb = new Path(warehouse.getWhRoot(), dbName +".db");
    warehouseFs.mkdirs(dirDb);
    Path dirTbl1 = new Path(dirDb, tblName1);
    warehouseFs.mkdirs(dirTbl1);
    EncryptionZoneUtils.createEncryptionZone(dirTbl1, "test_key_db", conf);
    Path part11 = new Path(dirTbl1, "part1");
    createFile(part11, "testClearer11");

    Path dirTbl2 = new Path(dirDb, tblName2);
    warehouseFs.mkdirs(dirTbl2);
    EncryptionZoneUtils.createEncryptionZone(dirTbl2, "test_key_cm", conf);
    Path part12 = new Path(dirTbl2, "part1");
    createFile(part12, "testClearer12");

    boolean exceptionThrown = false;
    try {
      client.dropTable(dbName, tblName1);
    } catch (MetaException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("can't be moved from encryption zone"));
    }
    assertFalse(exceptionThrown);
    assertFalse(warehouseFs.exists(part11));
    try {
      client.getTable(dbName, tblName1);
    } catch (NoSuchObjectException e) {
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);
    exceptionThrown = false;
    try {
      client.dropTable(dbName, tblName2);
    } catch (MetaException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("can't be moved from encryption zone"));
    }
    assertFalse(exceptionThrown);
    assertFalse(warehouseFs.exists(part12));
    try {
      client.getTable(dbName, tblName2);
    } catch (NoSuchObjectException e) {
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);
    assertTrue(warehouseFs.exists(new Path(dirTbl1, cmrootEncrypted)));
    assertTrue(warehouseFs.exists(new Path(dirTbl2, cmrootEncrypted)));
  }

  @Test
  public void dropTableWithDifferentEncryptionZonesSameKey() throws Throwable {
    String dbName1 = "encrdbsamekey1";
    String dbName2 = "encrdbsamekey2";
    String tblName1 = "encrtbl1";
    String tblName2 = "encrtbl2";
    String typeName = "Person";

    silentDropDatabase(dbName1);
    silentDropDatabase(dbName2);
    new DatabaseBuilder()
            .setName(dbName1)
            .addParam("repl.source.for", "1, 2, 3")
            .create(client, hiveConf);

    new DatabaseBuilder()
            .setName(dbName2)
            .addParam("repl.source.for", "1, 2, 3")
            .create(client, hiveConf);

    client.dropType(typeName);
    Type typ1 = new Type();
    typ1.setName(typeName);
    typ1.setFields(new ArrayList<>(2));
    typ1.getFields().add(
            new FieldSchema("name", ColumnType.STRING_TYPE_NAME, ""));
    typ1.getFields().add(
            new FieldSchema("income", ColumnType.INT_TYPE_NAME, ""));
    client.createType(typ1);

    Path dirDb1 = new Path(warehouse.getWhRoot(), dbName1 +".db");
    warehouseFs.mkdirs(dirDb1);
    EncryptionZoneUtils.createEncryptionZone(dirDb1, "test_key_db", conf);
    Path dirTbl1 = new Path(dirDb1, tblName1);
    warehouseFs.mkdirs(dirTbl1);
    Path part11 = new Path(dirTbl1, "part1");
    createFile(part11, "testClearer11");

    Path dirDb2 = new Path(warehouse.getWhRoot(), dbName2 +".db");
    warehouseFs.mkdirs(dirDb2);
    EncryptionZoneUtils.createEncryptionZone(dirDb2, "test_key_db", conf);
    Path dirTbl2 = new Path(dirDb2, tblName2);
    warehouseFs.mkdirs(dirTbl2);
    Path part12 = new Path(dirTbl2, "part1");
    createFile(part12, "testClearer12");

    new TableBuilder()
            .setDbName(dbName1)
            .setTableName(tblName1)
            .setCols(typ1.getFields())
            .setNumBuckets(1)
            .addBucketCol("name")
            .addStorageDescriptorParam("test_param_1", "Use this for comments etc")
            .create(client, hiveConf);

    Table tbl = client.getTable(dbName1, tblName1);
    Assert.assertNotNull(tbl);

    new TableBuilder()
            .setDbName(dbName2)
            .setTableName(tblName2)
            .setCols(typ1.getFields())
            .setNumBuckets(1)
            .addBucketCol("name")
            .addStorageDescriptorParam("test_param_1", "Use this for comments etc")
            .create(client, hiveConf);

    boolean exceptionThrown = false;
    try {
      client.dropTable(dbName1, tblName1);
    } catch (MetaException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("can't be moved from encryption zone"));
    }
    assertFalse(exceptionThrown);
    assertFalse(warehouseFs.exists(part11));
    try {
      client.getTable(dbName1, tblName1);
    } catch (NoSuchObjectException e) {
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);
    exceptionThrown = false;
    try {
      client.dropTable(dbName2, tblName2);
    } catch (MetaException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("can't be moved from encryption zone"));
    }
    assertFalse(exceptionThrown);
    assertFalse(warehouseFs.exists(part12));
    try {
      client.getTable(dbName2, tblName2);
    } catch (NoSuchObjectException e) {
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);

  }

  @Test
  public void dropTableWithSameEncryptionZones() throws Throwable {
    String dbName = "encrdb3";
    String tblName1 = "encrtbl1";
    String tblName2 = "encrtbl2";
    String typeName = "Person";
    silentDropDatabase(dbName);

    new DatabaseBuilder()
            .setName(dbName)
            .addParam("repl.source.for", "1, 2, 3")
            .create(client, hiveConf);

    client.dropType(typeName);
    Type typ1 = new Type();
    typ1.setName(typeName);
    typ1.setFields(new ArrayList<>(2));
    typ1.getFields().add(
            new FieldSchema("name", ColumnType.STRING_TYPE_NAME, ""));
    typ1.getFields().add(
            new FieldSchema("income", ColumnType.INT_TYPE_NAME, ""));
    client.createType(typ1);

    new TableBuilder()
            .setDbName(dbName)
            .setTableName(tblName1)
            .setCols(typ1.getFields())
            .setNumBuckets(1)
            .addBucketCol("name")
            .addStorageDescriptorParam("test_param_1", "Use this for comments etc")
            .create(client, hiveConf);

    Table tbl = client.getTable(dbName, tblName1);
    Assert.assertNotNull(tbl);

    new TableBuilder()
            .setDbName(dbName)
            .setTableName(tblName2)
            .setCols(typ1.getFields())
            .setNumBuckets(1)
            .addBucketCol("name")
            .addStorageDescriptorParam("test_param_1", "Use this for comments etc")
            .create(client, hiveConf);

    Path dirDb = new Path(warehouse.getWhRoot(), dbName +".db");
    warehouseFs.delete(dirDb, true);
    warehouseFs.mkdirs(dirDb);
    EncryptionZoneUtils.createEncryptionZone(dirDb, "test_key_db", conf);
    Path dirTbl1 = new Path(dirDb, tblName1);
    warehouseFs.mkdirs(dirTbl1);
    Path part11 = new Path(dirTbl1, "part1");
    createFile(part11, "testClearer11");

    Path dirTbl2 = new Path(dirDb, tblName2);
    warehouseFs.mkdirs(dirTbl2);
    Path part12 = new Path(dirTbl2, "part1");
    createFile(part12, "testClearer12");

    boolean exceptionThrown = false;
    try {
      client.dropTable(dbName, tblName1);
    } catch (MetaException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("can't be moved from encryption zone"));
    }
    assertFalse(exceptionThrown);
    assertFalse(warehouseFs.exists(part11));
    try {
      client.getTable(dbName, tblName1);
    } catch (NoSuchObjectException e) {
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);
    exceptionThrown = false;
    try {
      client.dropTable(dbName, tblName2);
    } catch (MetaException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("can't be moved from encryption zone"));
    }
    assertFalse(exceptionThrown);
    assertFalse(warehouseFs.exists(part12));
    try {
      client.getTable(dbName, tblName2);
    } catch (NoSuchObjectException e) {
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);
  }

  @Test
  public void dropTableWithoutEncryptionZonesForCm() throws Throwable {
    String dbName = "simpdb1";
    String tblName = "simptbl";
    String typeName = "Person";
    silentDropDatabase(dbName);
    new DatabaseBuilder()
            .setName(dbName)
            .addParam("repl.source.for", "1, 2, 3")
            .create(client, hiveConf);

    client.dropType(typeName);
    Type typ1 = new Type();
    typ1.setName(typeName);
    typ1.setFields(new ArrayList<>(2));
    typ1.getFields().add(
            new FieldSchema("name", ColumnType.STRING_TYPE_NAME, ""));
    typ1.getFields().add(
            new FieldSchema("income", ColumnType.INT_TYPE_NAME, ""));
    client.createType(typ1);

    new TableBuilder()
            .setDbName(dbName)
            .setTableName(tblName)
            .setCols(typ1.getFields())
            .setNumBuckets(1)
            .addBucketCol("name")
            .addStorageDescriptorParam("test_param_1", "Use this for comments etc")
            .create(client, hiveConf);

    Table tbl = client.getTable(dbName, tblName);
    Assert.assertNotNull(tbl);

    Path dirDb = new Path(warehouse.getWhRoot(), dbName +".db");
    warehouseFs.mkdirs(dirDb);
    Path dirTbl1 = new Path(dirDb, tblName);
    warehouseFs.mkdirs(dirTbl1);
    Path part11 = new Path(dirTbl1, "part1");
    createFile(part11, "testClearer11");

    boolean exceptionThrown = false;
    try {
      client.dropTable(dbName, tblName);
    } catch (Exception e) {
      exceptionThrown = true;
    }
    assertFalse(exceptionThrown);
    assertFalse(warehouseFs.exists(part11));
    try {
      client.getTable(dbName, tblName);
    } catch (NoSuchObjectException e) {
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);
  }

  @Test
  public void dropExternalTableWithSameEncryptionZonesForCm() throws Throwable {
    String dbName = "encrdb4";
    String tblName1 = "encrtbl1";
    String tblName2 = "encrtbl2";
    String typeName = "Person";
    silentDropDatabase(dbName);
    new DatabaseBuilder()
            .setName(dbName)
            .addParam("repl.source.for", "1, 2, 3")
            .create(client, hiveConf);

    client.dropType(typeName);
    Type typ1 = new Type();
    typ1.setName(typeName);
    typ1.setFields(new ArrayList<>(2));
    typ1.getFields().add(
            new FieldSchema("name", ColumnType.STRING_TYPE_NAME, ""));
    typ1.getFields().add(
            new FieldSchema("income", ColumnType.INT_TYPE_NAME, ""));
    client.createType(typ1);

    new TableBuilder()
            .setDbName(dbName)
            .setTableName(tblName1)
            .setCols(typ1.getFields())
            .setNumBuckets(1)
            .addBucketCol("name")
            .addTableParam("EXTERNAL", "true")
            .addTableParam("external.table.purge", "true")
            .addStorageDescriptorParam("test_param_1", "Use this for comments etc")
            .create(client, hiveConf);

    Table tbl = client.getTable(dbName, tblName1);
    Assert.assertNotNull(tbl);

    new TableBuilder()
            .setDbName(dbName)
            .setTableName(tblName2)
            .setCols(typ1.getFields())
            .setNumBuckets(1)
            .addBucketCol("name")
            .addTableParam("EXTERNAL", "true")
            .addTableParam("external.table.purge", "true")
            .addStorageDescriptorParam("test_param_1", "Use this for comments etc")
            .create(client, hiveConf);

    Path dirDb = new Path(warehouse.getWhRoot(), dbName +".db");
    warehouseFs.delete(dirDb, true);
    warehouseFs.mkdirs(dirDb);
    EncryptionZoneUtils.createEncryptionZone(dirDb, "test_key_db", conf);
    Path dirTbl1 = new Path(dirDb, tblName1);
    warehouseFs.mkdirs(dirTbl1);
    Path part11 = new Path(dirTbl1, "part1");
    createFile(part11, "testClearer11");

    Path dirTbl2 = new Path(dirDb, tblName2);
    warehouseFs.mkdirs(dirTbl2);
    Path part12 = new Path(dirTbl2, "part1");
    createFile(part12, "testClearer12");

    boolean exceptionThrown = false;
    try {
      client.dropTable(dbName, tblName1);
    } catch (MetaException e) {
      exceptionThrown = true;
    }
    assertFalse(exceptionThrown);
    assertFalse(warehouseFs.exists(part11));
    try {
      client.getTable(dbName, tblName1);
    } catch (NoSuchObjectException e) {
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);
    exceptionThrown = false;
    try {
      client.dropTable(dbName, tblName2);
    } catch (MetaException e) {
      exceptionThrown = true;
    }
    assertFalse(exceptionThrown);
    assertFalse(warehouseFs.exists(part11));
    try {
      client.getTable(dbName, tblName2);
    } catch (NoSuchObjectException e) {
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);
  }

  @Test
  public void dropExternalTableWithDifferentEncryptionZones() throws Throwable {
    String dbName = "encrdb5";
    String tblName1 = "encrtbl1";
    String tblName2 = "encrtbl2";
    String typeName = "Person";

    silentDropDatabase(dbName);
    new DatabaseBuilder()
            .setName(dbName)
            .addParam("repl.source.for", "1, 2, 3")
            .create(client, hiveConf);

    client.dropType(typeName);
    Type typ1 = new Type();
    typ1.setName(typeName);
    typ1.setFields(new ArrayList<>(2));
    typ1.getFields().add(
            new FieldSchema("name", ColumnType.STRING_TYPE_NAME, ""));
    typ1.getFields().add(
            new FieldSchema("income", ColumnType.INT_TYPE_NAME, ""));
    client.createType(typ1);

    new TableBuilder()
            .setDbName(dbName)
            .setTableName(tblName1)
            .setCols(typ1.getFields())
            .setNumBuckets(1)
            .addBucketCol("name")
            .addTableParam("EXTERNAL", "true")
            .addTableParam("external.table.purge", "true")
            .addStorageDescriptorParam("test_param_1", "Use this for comments etc")
            .create(client, hiveConf);

    Table tbl = client.getTable(dbName, tblName1);
    Assert.assertNotNull(tbl);

    new TableBuilder()
            .setDbName(dbName)
            .setTableName(tblName2)
            .setCols(typ1.getFields())
            .setNumBuckets(1)
            .addBucketCol("name")
            .addTableParam("EXTERNAL", "true")
            .addTableParam("external.table.purge", "true")
            .addStorageDescriptorParam("test_param_1", "Use this for comments etc")
            .create(client, hiveConf);

    Path dirDb = new Path(warehouse.getWhRoot(), dbName +".db");
    warehouseFs.mkdirs(dirDb);
    Path dirTbl1 = new Path(dirDb, tblName1);
    warehouseFs.mkdirs(dirTbl1);
    EncryptionZoneUtils.createEncryptionZone(dirTbl1, "test_key_db", conf);
    Path part11 = new Path(dirTbl1, "part1");
    createFile(part11, "testClearer11");

    Path dirTbl2 = new Path(dirDb, tblName2);
    warehouseFs.mkdirs(dirTbl2);
    EncryptionZoneUtils.createEncryptionZone(dirTbl2, "test_key_db", conf);
    Path part12 = new Path(dirTbl2, "part1");
    createFile(part12, "testClearer12");

    boolean exceptionThrown = false;
    try {
      client.dropTable(dbName, tblName1);
    } catch (MetaException e) {
      exceptionThrown = true;
    }
    assertFalse(exceptionThrown);
    assertFalse(warehouseFs.exists(part11));
    try {
      client.getTable(dbName, tblName1);
    } catch (NoSuchObjectException e) {
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);
    exceptionThrown = false;
    try {
      client.dropTable(dbName, tblName2);
    } catch (MetaException e) {
      exceptionThrown = true;
    }
    assertFalse(exceptionThrown);
    assertFalse(warehouseFs.exists(part12));
    try {
      client.getTable(dbName, tblName2);
    } catch (NoSuchObjectException e) {
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);
  }

  @Test
  public void dropExternalTableWithDifferentEncryptionZonesDifferentKey() throws Throwable {
    String dbName = "encrdb6";
    String tblName1 = "encrtbl1";
    String tblName2 = "encrtbl2";
    String typeName = "Person";

    silentDropDatabase(dbName);
    new DatabaseBuilder()
            .setName(dbName)
            .addParam("repl.source.for", "1, 2, 3")
            .create(client, hiveConf);

    client.dropType(typeName);
    Type typ1 = new Type();
    typ1.setName(typeName);
    typ1.setFields(new ArrayList<>(2));
    typ1.getFields().add(
            new FieldSchema("name", ColumnType.STRING_TYPE_NAME, ""));
    typ1.getFields().add(
            new FieldSchema("income", ColumnType.INT_TYPE_NAME, ""));
    client.createType(typ1);

    new TableBuilder()
            .setDbName(dbName)
            .setTableName(tblName1)
            .setCols(typ1.getFields())
            .setNumBuckets(1)
            .addBucketCol("name")
            .addTableParam("EXTERNAL", "true")
            .addTableParam("external.table.purge", "true")
            .addStorageDescriptorParam("test_param_1", "Use this for comments etc")
            .create(client, hiveConf);

    Table tbl = client.getTable(dbName, tblName1);
    Assert.assertNotNull(tbl);

    new TableBuilder()
            .setDbName(dbName)
            .setTableName(tblName2)
            .setCols(typ1.getFields())
            .setNumBuckets(1)
            .addBucketCol("name")
            .addTableParam("EXTERNAL", "true")
            .addTableParam("external.table.purge", "true")
            .addStorageDescriptorParam("test_param_1", "Use this for comments etc")
            .create(client, hiveConf);

    Path dirDb = new Path(warehouse.getWhRoot(), dbName +".db");
    warehouseFs.mkdirs(dirDb);
    Path dirTbl1 = new Path(dirDb, tblName1);
    warehouseFs.mkdirs(dirTbl1);
    EncryptionZoneUtils.createEncryptionZone(dirTbl1, "test_key_db", conf);
    Path part11 = new Path(dirTbl1, "part1");
    createFile(part11, "testClearer11");

    Path dirTbl2 = new Path(dirDb, tblName2);
    warehouseFs.mkdirs(dirTbl2);
    EncryptionZoneUtils.createEncryptionZone(dirTbl2, "test_key_cm", conf);
    Path part12 = new Path(dirTbl2, "part1");
    createFile(part12, "testClearer12");

    boolean exceptionThrown = false;
    try {
      client.dropTable(dbName, tblName1);
    } catch (MetaException e) {
      exceptionThrown = true;
    }
    assertFalse(exceptionThrown);
    assertFalse(warehouseFs.exists(part11));
    try {
      client.getTable(dbName, tblName1);
    } catch (NoSuchObjectException e) {
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);
    exceptionThrown = false;
    try {
      client.dropTable(dbName, tblName2);
    } catch (MetaException e) {
      exceptionThrown = true;
    }
    assertFalse(exceptionThrown);
    assertFalse(warehouseFs.exists(part12));
    try {
      client.getTable(dbName, tblName2);
    } catch (NoSuchObjectException e) {
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);
  }

  @Test
  public void dropExternalTableWithoutEncryptionZonesForCm() throws Throwable {
    String dbName = "simpdb2";
    String tblName = "simptbl";
    String typeName = "Person";
    silentDropDatabase(dbName);
    new DatabaseBuilder()
            .setName(dbName)
            .addParam("repl.source.for", "1, 2, 3")
            .create(client, hiveConf);

    client.dropType(typeName);
    Type typ1 = new Type();
    typ1.setName(typeName);
    typ1.setFields(new ArrayList<>(2));
    typ1.getFields().add(
            new FieldSchema("name", ColumnType.STRING_TYPE_NAME, ""));
    typ1.getFields().add(
            new FieldSchema("income", ColumnType.INT_TYPE_NAME, ""));
    client.createType(typ1);

    new TableBuilder()
            .setDbName(dbName)
            .setTableName(tblName)
            .setCols(typ1.getFields())
            .setNumBuckets(1)
            .addBucketCol("name")
            .addTableParam("EXTERNAL", "true")
            .addTableParam("external.table.purge", "true")
            .addStorageDescriptorParam("test_param_1", "Use this for comments etc")
            .create(client, hiveConf);

    Table tbl = client.getTable(dbName, tblName);
    Assert.assertNotNull(tbl);

    Path dirDb = new Path(warehouse.getWhRoot(), dbName +".db");
    warehouseFs.mkdirs(dirDb);
    Path dirTbl1 = new Path(dirDb, tblName);
    warehouseFs.mkdirs(dirTbl1);
    Path part11 = new Path(dirTbl1, "part1");
    createFile(part11, "testClearer11");

    boolean exceptionThrown = false;
    try {
      client.dropTable(dbName, tblName);
    } catch (Exception e) {
      exceptionThrown = true;
    }
    assertFalse(exceptionThrown);
    assertFalse(warehouseFs.exists(part11));
    try {
      client.getTable(dbName, tblName);
    } catch (NoSuchObjectException e) {
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);
  }

  @Test
  public void truncateTableWithDifferentEncryptionZones() throws Throwable {
    String dbName1 = "encrdbtrunc1";
    String dbName2 = "encrdbtrunc2";
    String tblName1 = "encrtbl1";
    String tblName2 = "encrtbl2";
    String typeName = "Person";

    silentDropDatabase(dbName1);
    silentDropDatabase(dbName2);
    new DatabaseBuilder()
            .setName(dbName1)
            .addParam("repl.source.for", "1, 2, 3")
            .create(client, hiveConf);

    new DatabaseBuilder()
            .setName(dbName2)
            .addParam("repl.source.for", "1, 2, 3")
            .create(client, hiveConf);

    client.dropType(typeName);
    Type typ1 = new Type();
    typ1.setName(typeName);
    typ1.setFields(new ArrayList<>(2));
    typ1.getFields().add(
            new FieldSchema("name", ColumnType.STRING_TYPE_NAME, ""));
    typ1.getFields().add(
            new FieldSchema("income", ColumnType.INT_TYPE_NAME, ""));
    client.createType(typ1);

    Path dirDb1 = new Path(warehouse.getWhRoot(), dbName1 +".db");
    warehouseFs.delete(dirDb1, true);
    warehouseFs.mkdirs(dirDb1);
    EncryptionZoneUtils.createEncryptionZone(dirDb1, "test_key_db", conf);
    Path dirTbl1 = new Path(dirDb1, tblName1);
    warehouseFs.mkdirs(dirTbl1);
    Path part11 = new Path(dirTbl1, "part1");
    createFile(part11, "testClearer11");

    Path dirDb2 = new Path(warehouse.getWhRoot(), dbName2 +".db");
    warehouseFs.delete(dirDb2, true);
    warehouseFs.mkdirs(dirDb2);
    EncryptionZoneUtils.createEncryptionZone(dirDb2, "test_key_db", conf);
    Path dirTbl2 = new Path(dirDb2, tblName2);
    warehouseFs.mkdirs(dirTbl2);
    Path part12 = new Path(dirTbl2, "part1");
    createFile(part12, "testClearer12");

    new TableBuilder()
            .setDbName(dbName1)
            .setTableName(tblName1)
            .setCols(typ1.getFields())
            .setNumBuckets(1)
            .addBucketCol("name")
            .addStorageDescriptorParam("test_param_1", "Use this for comments etc")
            .create(client, hiveConf);

    Table tbl = client.getTable(dbName1, tblName1);
    Assert.assertNotNull(tbl);

    new TableBuilder()
            .setDbName(dbName2)
            .setTableName(tblName2)
            .setCols(typ1.getFields())
            .setNumBuckets(1)
            .addBucketCol("name")
            .addStorageDescriptorParam("test_param_1", "Use this for comments etc")
            .create(client, hiveConf);

    boolean exceptionThrown = false;
    try {
      client.truncateTable(dbName1, tblName1, null);
    } catch (MetaException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("can't be moved from encryption zone"));
    }
    assertFalse(exceptionThrown);
    assertFalse(warehouseFs.exists(part11));
    assertNotNull(client.getTable(dbName1, tblName1));
    exceptionThrown = false;
    try {
      client.truncateTable(dbName2, tblName2, null);
    } catch (MetaException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("can't be moved from encryption zone"));
    }
    assertFalse(exceptionThrown);
    assertFalse(warehouseFs.exists(part12));
    assertNotNull(client.getTable(dbName2, tblName2));
  }

  @Test
  public void truncateTableWithDifferentEncryptionZonesDifferentKey() throws Throwable {
    String dbName1 = "encrdb1";
    String dbName2 = "encrdb2";
    String tblName1 = "encrtbl1";
    String tblName2 = "encrtbl2";
    String typeName = "Person";

    silentDropDatabase(dbName1);
    silentDropDatabase(dbName2);
    new DatabaseBuilder()
            .setName(dbName1)
            .addParam("repl.source.for", "1, 2, 3")
            .create(client, hiveConf);

    new DatabaseBuilder()
            .setName(dbName2)
            .addParam("repl.source.for", "1, 2, 3")
            .create(client, hiveConf);

    client.dropType(typeName);
    Type typ1 = new Type();
    typ1.setName(typeName);
    typ1.setFields(new ArrayList<>(2));
    typ1.getFields().add(
            new FieldSchema("name", ColumnType.STRING_TYPE_NAME, ""));
    typ1.getFields().add(
            new FieldSchema("income", ColumnType.INT_TYPE_NAME, ""));
    client.createType(typ1);

    Path dirDb1 = new Path(warehouse.getWhRoot(), dbName1 +".db");
    warehouseFs.mkdirs(dirDb1);
    EncryptionZoneUtils.createEncryptionZone(dirDb1, "test_key_db", conf);
    Path dirTbl1 = new Path(dirDb1, tblName1);
    warehouseFs.mkdirs(dirTbl1);
    Path part11 = new Path(dirTbl1, "part1");
    createFile(part11, "testClearer11");

    Path dirDb2 = new Path(warehouse.getWhRoot(), dbName2 +".db");
    warehouseFs.mkdirs(dirDb2);
    EncryptionZoneUtils.createEncryptionZone(dirDb2, "test_key_db", conf);
    Path dirTbl2 = new Path(dirDb2, tblName2);
    warehouseFs.mkdirs(dirTbl2);
    Path part12 = new Path(dirTbl2, "part1");
    createFile(part12, "testClearer12");

    new TableBuilder()
            .setDbName(dbName1)
            .setTableName(tblName1)
            .setCols(typ1.getFields())
            .setNumBuckets(1)
            .addBucketCol("name")
            .addStorageDescriptorParam("test_param_1", "Use this for comments etc")
            .create(client, hiveConf);

    Table tbl = client.getTable(dbName1, tblName1);
    Assert.assertNotNull(tbl);

    new TableBuilder()
            .setDbName(dbName2)
            .setTableName(tblName2)
            .setCols(typ1.getFields())
            .setNumBuckets(1)
            .addBucketCol("name")
            .addStorageDescriptorParam("test_param_1", "Use this for comments etc")
            .create(client, hiveConf);

    boolean exceptionThrown = false;
    try {
      client.truncateTable(dbName1, tblName1, null);
    } catch (MetaException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("can't be moved from encryption zone"));
    }
    assertFalse(exceptionThrown);
    assertFalse(warehouseFs.exists(part11));
    assertNotNull(client.getTable(dbName1, tblName1));
    exceptionThrown = false;
    try {
      client.truncateTable(dbName2, tblName2, null);
    } catch (MetaException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("can't be moved from encryption zone"));
    }
    assertFalse(exceptionThrown);
    assertFalse(warehouseFs.exists(part12));
    assertNotNull(client.getTable(dbName2, tblName2));
  }

  @Test
  public void truncateTableWithSameEncryptionZones() throws Throwable {
    String dbName = "encrdb9";
    String tblName1 = "encrtbl1";
    String tblName2 = "encrtbl2";
    String typeName = "Person";
    client.dropTable(dbName, tblName1);
    client.dropTable(dbName, tblName2);
    silentDropDatabase(dbName);
    new DatabaseBuilder()
            .setName(dbName)
            .addParam("repl.source.for", "1, 2, 3")
            .create(client, hiveConf);

    client.dropType(typeName);
    Type typ1 = new Type();
    typ1.setName(typeName);
    typ1.setFields(new ArrayList<>(2));
    typ1.getFields().add(
            new FieldSchema("name", ColumnType.STRING_TYPE_NAME, ""));
    typ1.getFields().add(
            new FieldSchema("income", ColumnType.INT_TYPE_NAME, ""));
    client.createType(typ1);

    new TableBuilder()
            .setDbName(dbName)
            .setTableName(tblName1)
            .setCols(typ1.getFields())
            .setNumBuckets(1)
            .addBucketCol("name")
            .addStorageDescriptorParam("test_param_1", "Use this for comments etc")
            .create(client, hiveConf);

    Table tbl = client.getTable(dbName, tblName1);
    Assert.assertNotNull(tbl);

    new TableBuilder()
            .setDbName(dbName)
            .setTableName(tblName2)
            .setCols(typ1.getFields())
            .setNumBuckets(1)
            .addBucketCol("name")
            .addStorageDescriptorParam("test_param_1", "Use this for comments etc")
            .create(client, hiveConf);

    Path dirDb = new Path(warehouse.getWhRoot(), dbName +".db");
    warehouseFs.delete(dirDb, true);
    warehouseFs.mkdirs(dirDb);
    EncryptionZoneUtils.createEncryptionZone(dirDb, "test_key_db", conf);
    Path dirTbl1 = new Path(dirDb, tblName1);
    warehouseFs.mkdirs(dirTbl1);
    Path part11 = new Path(dirTbl1, "part1");
    createFile(part11, "testClearer11");

    Path dirTbl2 = new Path(dirDb, tblName2);
    warehouseFs.mkdirs(dirTbl2);
    Path part12 = new Path(dirTbl2, "part1");
    createFile(part12, "testClearer12");

    boolean exceptionThrown = false;
    try {
      client.truncateTable(dbName, tblName1, null);
    } catch (MetaException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("can't be moved from encryption zone"));
    }
    assertFalse(exceptionThrown);
    assertFalse(warehouseFs.exists(part11));
    try {
      client.getTable(dbName, tblName1);
    } catch (NoSuchObjectException e) {
      exceptionThrown = true;
    }
    assertFalse(exceptionThrown);

    try {
      client.truncateTable(dbName, tblName2, null);
    } catch (MetaException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("can't be moved from encryption zone"));
    }
    assertFalse(exceptionThrown);
    assertFalse(warehouseFs.exists(part12));
    try {
      client.getTable(dbName, tblName2);
    } catch (NoSuchObjectException e) {
      exceptionThrown = true;
    }
    assertFalse(exceptionThrown);
  }

  @Test
  public void truncateTableWithoutEncryptionZonesForCm() throws Throwable {
    String dbName = "simpdb3";
    String tblName = "simptbl";
    String typeName = "Person";
    client.dropTable(dbName, tblName);
    silentDropDatabase(dbName);

    new DatabaseBuilder()
            .setName(dbName)
            .addParam("repl.source.for", "1, 2, 3")
            .create(client, hiveConf);

    client.dropType(typeName);
    Type typ1 = new Type();
    typ1.setName(typeName);
    typ1.setFields(new ArrayList<>(2));
    typ1.getFields().add(
            new FieldSchema("name", ColumnType.STRING_TYPE_NAME, ""));
    typ1.getFields().add(
            new FieldSchema("income", ColumnType.INT_TYPE_NAME, ""));
    client.createType(typ1);

    new TableBuilder()
            .setDbName(dbName)
            .setTableName(tblName)
            .setCols(typ1.getFields())
            .setNumBuckets(1)
            .addBucketCol("name")
            .addStorageDescriptorParam("test_param_1", "Use this for comments etc")
            .create(client, hiveConf);

    Table tbl2 = client.getTable(dbName, tblName);
    Assert.assertNotNull(tbl2);

    Path dirDb = new Path(warehouse.getWhRoot(), dbName +".db");
    warehouseFs.mkdirs(dirDb);
    Path dirTbl1 = new Path(dirDb, tblName);
    warehouseFs.mkdirs(dirTbl1);
    Path part11 = new Path(dirTbl1, "part1");
    createFile(part11, "testClearer11");

    boolean exceptionThrown = false;
    try {
      client.truncateTable(dbName, tblName, null);
    } catch (Exception e) {
      exceptionThrown = true;
    }
    assertFalse(exceptionThrown);
    assertFalse(warehouseFs.exists(part11));
    try {
      client.getTable(dbName, tblName);
    } catch (NoSuchObjectException e) {
      exceptionThrown = true;
    }
    assertFalse(exceptionThrown);
  }

  @Test
  public void recycleFailureWithDifferentEncryptionZonesForCm() throws Throwable {
    Path dirDb = new Path(warehouse.getWhRoot(), "db2");
    warehouseFs.delete(dirDb, true);
    warehouseFs.mkdirs(dirDb);
    Path dirTbl1 = new Path(dirDb, "tbl1");
    warehouseFs.mkdirs(dirTbl1);
    EncryptionZoneUtils.createEncryptionZone(dirTbl1, "test_key_db", conf);
    Path part11 = new Path(dirTbl1, "part1");
    createFile(part11, "testClearer11");

    boolean exceptionThrown = false;
    try {
      ReplChangeManager.getInstance(hiveConf).recycle(dirTbl1, RecycleType.MOVE, false);
    } catch (RemoteException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("can't be moved from encryption zone"));
    }
    assertFalse(exceptionThrown);
  }

  @Test
  public void testClearerEncrypted() throws Exception {
    HiveConf hiveConfCmClearer = new HiveConf(TestReplChangeManager.class);
    hiveConfCmClearer.setBoolean(HiveConf.ConfVars.REPL_CM_ENABLED.varname, true);
    hiveConfCmClearer.setInt(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 60);
    hiveConfCmClearer.set(HiveConf.ConfVars.METASTORE_WAREHOUSE.varname,
            "hdfs://" + miniDFSCluster.getNameNode().getHostAndPort()
                    + HiveConf.ConfVars.METASTORE_WAREHOUSE.defaultStrVal);

    String cmrootCmClearer = "hdfs://" + miniDFSCluster.getNameNode().getHostAndPort() + "/cmrootClearer";
    hiveConfCmClearer.set(HiveConf.ConfVars.REPL_CM_DIR.varname, cmrootCmClearer);
    Warehouse warehouseCmClearer = new Warehouse(hiveConfCmClearer);
    FileSystem cmfs = new Path(cmrootCmClearer).getFileSystem(hiveConfCmClearer);
    cmfs.mkdirs(warehouseCmClearer.getWhRoot());

    FileSystem fsWarehouse = warehouseCmClearer.getWhRoot().getFileSystem(hiveConfCmClearer);
    long now = System.currentTimeMillis();
    Path dirDb = new Path(warehouseCmClearer.getWhRoot(), "db1");
    fsWarehouse.delete(dirDb, true);
    fsWarehouse.mkdirs(dirDb);
    Path dirTbl1 = new Path(dirDb, "tbl1");
    fsWarehouse.mkdirs(dirTbl1);
    EncryptionZoneUtils.createEncryptionZone(dirTbl1, "test_key_db", conf);
    Path part11 = new Path(dirTbl1, "part1");
    createFile(part11, "testClearer11");
    String fileChksum11 = ReplChangeManager.checksumFor(part11, fsWarehouse);
    Path part12 = new Path(dirTbl1, "part2");
    createFile(part12, "testClearer12");
    String fileChksum12 = ReplChangeManager.checksumFor(part12, fsWarehouse);
    Path dirTbl2 = new Path(dirDb, "tbl2");
    fsWarehouse.mkdirs(dirTbl2);
    EncryptionZoneUtils.createEncryptionZone(dirTbl2, "test_key_db", conf);
    Path part21 = new Path(dirTbl2, "part1");
    createFile(part21, "testClearer21");
    String fileChksum21 = ReplChangeManager.checksumFor(part21, fsWarehouse);
    Path part22 = new Path(dirTbl2, "part2");
    createFile(part22, "testClearer22");
    String fileChksum22 = ReplChangeManager.checksumFor(part22, fsWarehouse);
    Path dirTbl3 = new Path(dirDb, "tbl3");
    fsWarehouse.mkdirs(dirTbl3);
    EncryptionZoneUtils.createEncryptionZone(dirTbl3, "test_key_cm", conf);
    Path part31 = new Path(dirTbl3, "part1");
    createFile(part31, "testClearer31");
    String fileChksum31 = ReplChangeManager.checksumFor(part31, fsWarehouse);
    Path part32 = new Path(dirTbl3, "part2");
    createFile(part32, "testClearer32");
    String fileChksum32 = ReplChangeManager.checksumFor(part32, fsWarehouse);

    ReplChangeManager.getInstance(hiveConfCmClearer).recycle(dirTbl1, RecycleType.MOVE, false);
    ReplChangeManager.getInstance(hiveConfCmClearer).recycle(dirTbl2, RecycleType.MOVE, false);
    ReplChangeManager.getInstance(hiveConfCmClearer).recycle(dirTbl3, RecycleType.MOVE, true);

    assertTrue(fsWarehouse.exists(ReplChangeManager.getCMPath(hiveConfCmClearer, part11.getName(), fileChksum11,
            ReplChangeManager.getInstance(conf).getCmRoot(part11).toString())));
    assertTrue(fsWarehouse.exists(ReplChangeManager.getCMPath(hiveConfCmClearer, part12.getName(), fileChksum12,
            ReplChangeManager.getInstance(conf).getCmRoot(part12).toString())));
    assertTrue(fsWarehouse.exists(ReplChangeManager.getCMPath(hiveConfCmClearer, part21.getName(), fileChksum21,
            ReplChangeManager.getInstance(conf).getCmRoot(part21).toString())));
    assertTrue(fsWarehouse.exists(ReplChangeManager.getCMPath(hiveConfCmClearer, part22.getName(), fileChksum22,
            ReplChangeManager.getInstance(conf).getCmRoot(part22).toString())));
    assertTrue(fsWarehouse.exists(ReplChangeManager.getCMPath(hiveConfCmClearer, part31.getName(), fileChksum31,
            ReplChangeManager.getInstance(conf).getCmRoot(part31).toString())));
    assertTrue(fsWarehouse.exists(ReplChangeManager.getCMPath(hiveConfCmClearer, part32.getName(), fileChksum32,
            ReplChangeManager.getInstance(conf).getCmRoot(part32).toString())));

    fsWarehouse.setTimes(ReplChangeManager.getCMPath(hiveConfCmClearer, part11.getName(), fileChksum11,
            ReplChangeManager.getInstance(conf).getCmRoot(part11).toString()),
            now - 7 * 86400 * 1000 * 2, now - 7 * 86400 * 1000 * 2);
    fsWarehouse.setTimes(ReplChangeManager.getCMPath(hiveConfCmClearer, part21.getName(), fileChksum21,
            ReplChangeManager.getInstance(conf).getCmRoot(part21).toString()),
            now - 7 * 86400 * 1000 * 2, now - 7 * 86400 * 1000 * 2);
    fsWarehouse.setTimes(ReplChangeManager.getCMPath(hiveConfCmClearer, part31.getName(), fileChksum31,
            ReplChangeManager.getInstance(conf).getCmRoot(part31).toString()),
            now - 7 * 86400 * 1000 * 2, now - 7 * 86400 * 1000 * 2);
    fsWarehouse.setTimes(ReplChangeManager.getCMPath(hiveConfCmClearer, part32.getName(), fileChksum32,
            ReplChangeManager.getInstance(conf).getCmRoot(part32).toString()),
            now - 7 * 86400 * 1000 * 2, now - 7 * 86400 * 1000 * 2);

    ReplChangeManager.scheduleCMClearer(hiveConfCmClearer);

    long start = System.currentTimeMillis();
    long end;
    boolean cleared = false;
    do {
      Thread.sleep(200);
      end = System.currentTimeMillis();
      if (end - start > 5000) {
        Assert.fail("timeout, cmroot has not been cleared");
      }
      if (!fsWarehouse.exists(ReplChangeManager.getCMPath(hiveConfCmClearer, part11.getName(), fileChksum11,
              ReplChangeManager.getInstance(conf).getCmRoot(part11).toString())) &&
              fsWarehouse.exists(ReplChangeManager.getCMPath(hiveConfCmClearer, part12.getName(), fileChksum12,
                      ReplChangeManager.getInstance(conf).getCmRoot(part12).toString())) &&
              !fsWarehouse.exists(ReplChangeManager.getCMPath(hiveConfCmClearer, part21.getName(), fileChksum21,
                      ReplChangeManager.getInstance(conf).getCmRoot(part21).toString())) &&
              fsWarehouse.exists(ReplChangeManager.getCMPath(hiveConfCmClearer, part22.getName(), fileChksum22,
                      ReplChangeManager.getInstance(conf).getCmRoot(part22).toString())) &&
              !fsWarehouse.exists(ReplChangeManager.getCMPath(hiveConfCmClearer, part31.getName(), fileChksum31,
                      ReplChangeManager.getInstance(conf).getCmRoot(part31).toString())) &&
              !fsWarehouse.exists(ReplChangeManager.getCMPath(hiveConfCmClearer, part32.getName(), fileChksum32,
                      ReplChangeManager.getInstance(conf).getCmRoot(part32).toString()))) {
        cleared = true;
      }
    } while (!cleared);
  }

  @Test
  public void testCmRootAclPermissions() throws Exception {
    HiveConf hiveConfAclPermissions = new HiveConf(TestReplChangeManager.class);
    hiveConfAclPermissions.setBoolean(HiveConf.ConfVars.REPL_CM_ENABLED.varname, true);
    hiveConfAclPermissions.setInt(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 60);
    hiveConfAclPermissions.set(HiveConf.ConfVars.METASTORE_WAREHOUSE.varname,
            "hdfs://" + miniDFSCluster.getNameNode().getHostAndPort()
                    + HiveConf.ConfVars.METASTORE_WAREHOUSE.defaultStrVal);

    String cmRootAclPermissions = "hdfs://" + miniDFSCluster.getNameNode().getHostAndPort() + "/cmRootAclPermissions";
    hiveConfAclPermissions.set(HiveConf.ConfVars.REPL_CM_DIR.varname, cmRootAclPermissions);
    Warehouse warehouseCmPermissions = new Warehouse(hiveConfAclPermissions);
    FileSystem cmfs = new Path(cmRootAclPermissions).getFileSystem(hiveConfAclPermissions);
    cmfs.mkdirs(warehouseCmPermissions.getWhRoot());

    FileSystem fsWarehouse = warehouseCmPermissions.getWhRoot().getFileSystem(hiveConfAclPermissions);
    //change the group of warehouse for testing
    Path warehouse = new Path(hiveConfAclPermissions.get(HiveConf.ConfVars.METASTORE_WAREHOUSE.varname));
    fsWarehouse.setOwner(warehouse, null, "testgroup");

    long now = System.currentTimeMillis();
    Path dirDb = new Path(warehouseCmPermissions.getWhRoot(), "db_perm");
    fsWarehouse.delete(dirDb, true);
    fsWarehouse.mkdirs(dirDb);
    Path dirTbl1 = new Path(dirDb, "tbl1");
    fsWarehouse.mkdirs(dirTbl1);
    EncryptionZoneUtils.createEncryptionZone(dirTbl1, "test_key_db", conf);
    Path part11 = new Path(dirTbl1, "part1");
    createFile(part11, "testClearer11");
    String fileChksum11 = ReplChangeManager.checksumFor(part11, fsWarehouse);
    Path part12 = new Path(dirTbl1, "part2");
    createFile(part12, "testClearer12");
    String fileChksum12 = ReplChangeManager.checksumFor(part12, fsWarehouse);
    Path dirTbl2 = new Path(dirDb, "tbl2");
    fsWarehouse.mkdirs(dirTbl2);
    EncryptionZoneUtils.createEncryptionZone(dirTbl2, "test_key_db", conf);
    Path part21 = new Path(dirTbl2, "part1");
    createFile(part21, "testClearer21");
    String fileChksum21 = ReplChangeManager.checksumFor(part21, fsWarehouse);
    Path part22 = new Path(dirTbl2, "part2");
    createFile(part22, "testClearer22");
    String fileChksum22 = ReplChangeManager.checksumFor(part22, fsWarehouse);
    Path dirTbl3 = new Path(dirDb, "tbl3");
    fsWarehouse.mkdirs(dirTbl3);
    EncryptionZoneUtils.createEncryptionZone(dirTbl3, "test_key_cm", conf);
    Path part31 = new Path(dirTbl3, "part1");
    createFile(part31, "testClearer31");
    String fileChksum31 = ReplChangeManager.checksumFor(part31, fsWarehouse);
    Path part32 = new Path(dirTbl3, "part2");
    createFile(part32, "testClearer32");
    String fileChksum32 = ReplChangeManager.checksumFor(part32, fsWarehouse);

    final UserGroupInformation proxyUserUgi =
            UserGroupInformation.createUserForTesting("impala", new String[] {"testgroup"});

    fsWarehouse.setOwner(dirDb, "impala", "default");
    fsWarehouse.setOwner(dirTbl1, "impala", "default");
    fsWarehouse.setOwner(dirTbl2, "impala", "default");
    fsWarehouse.setOwner(dirTbl3, "impala", "default");
    fsWarehouse.setOwner(part11, "impala", "default");
    fsWarehouse.setOwner(part12, "impala", "default");
    fsWarehouse.setOwner(part21, "impala", "default");
    fsWarehouse.setOwner(part22, "impala", "default");
    fsWarehouse.setOwner(part31, "impala", "default");
    fsWarehouse.setOwner(part32, "impala", "default");

    proxyUserUgi.doAs((PrivilegedExceptionAction<Void>) () -> {
      try {
        //impala doesn't have access but it belongs to a group which has access through acl.
        ReplChangeManager.getInstance(hiveConfAclPermissions).recycle(dirTbl1, RecycleType.MOVE, false);
        ReplChangeManager.getInstance(hiveConfAclPermissions).recycle(dirTbl2, RecycleType.MOVE, false);
        ReplChangeManager.getInstance(hiveConfAclPermissions).recycle(dirTbl3, RecycleType.MOVE, true);
      } catch (Exception e) {
        Assert.fail();
      }
      return null;
    });

    String cmEncrypted = hiveConf.get(HiveConf.ConfVars.REPL_CM_ENCRYPTED_DIR.varname, cmrootEncrypted);
    AclStatus aclStatus = fsWarehouse.getAclStatus(new Path(dirTbl1 + Path.SEPARATOR + cmEncrypted));
    AclStatus aclStatus2 = fsWarehouse.getAclStatus(new Path(dirTbl2 + Path.SEPARATOR + cmEncrypted));
    AclStatus aclStatus3 = fsWarehouse.getAclStatus(new Path(dirTbl3 + Path.SEPARATOR + cmEncrypted));
    AclEntry expectedAcl = new AclEntry.Builder().setScope(ACCESS).setType(GROUP).setName("testgroup").
            setPermission(fsWarehouse.getFileStatus(warehouse).getPermission().getGroupAction()).build();
    Assert.assertTrue(aclStatus.getEntries().contains(expectedAcl));
    Assert.assertTrue(aclStatus2.getEntries().contains(expectedAcl));
    Assert.assertTrue(aclStatus3.getEntries().contains(expectedAcl));

    assertTrue(fsWarehouse.exists(ReplChangeManager.getCMPath(hiveConfAclPermissions, part11.getName(), fileChksum11,
            ReplChangeManager.getInstance(conf).getCmRoot(part11).toString())));
    assertTrue(fsWarehouse.exists(ReplChangeManager.getCMPath(hiveConfAclPermissions, part12.getName(), fileChksum12,
            ReplChangeManager.getInstance(conf).getCmRoot(part12).toString())));
    assertTrue(fsWarehouse.exists(ReplChangeManager.getCMPath(hiveConfAclPermissions, part21.getName(), fileChksum21,
            ReplChangeManager.getInstance(conf).getCmRoot(part21).toString())));
    assertTrue(fsWarehouse.exists(ReplChangeManager.getCMPath(hiveConfAclPermissions, part22.getName(), fileChksum22,
            ReplChangeManager.getInstance(conf).getCmRoot(part22).toString())));
    assertTrue(fsWarehouse.exists(ReplChangeManager.getCMPath(hiveConfAclPermissions, part31.getName(), fileChksum31,
            ReplChangeManager.getInstance(conf).getCmRoot(part31).toString())));
    assertTrue(fsWarehouse.exists(ReplChangeManager.getCMPath(hiveConfAclPermissions, part32.getName(), fileChksum32,
            ReplChangeManager.getInstance(conf).getCmRoot(part32).toString())));

    fsWarehouse.setTimes(ReplChangeManager.getCMPath(hiveConfAclPermissions, part11.getName(), fileChksum11,
            ReplChangeManager.getInstance(conf).getCmRoot(part11).toString()),
            now - 7 * 86400 * 1000 * 2, now - 7 * 86400 * 1000 * 2);
    fsWarehouse.setTimes(ReplChangeManager.getCMPath(hiveConfAclPermissions, part21.getName(), fileChksum21,
            ReplChangeManager.getInstance(conf).getCmRoot(part21).toString()),
            now - 7 * 86400 * 1000 * 2, now - 7 * 86400 * 1000 * 2);
    fsWarehouse.setTimes(ReplChangeManager.getCMPath(hiveConfAclPermissions, part31.getName(), fileChksum31,
            ReplChangeManager.getInstance(conf).getCmRoot(part31).toString()),
            now - 7 * 86400 * 1000 * 2, now - 7 * 86400 * 1000 * 2);
    fsWarehouse.setTimes(ReplChangeManager.getCMPath(hiveConfAclPermissions, part32.getName(), fileChksum32,
            ReplChangeManager.getInstance(conf).getCmRoot(part32).toString()),
            now - 7 * 86400 * 1000 * 2, now - 7 * 86400 * 1000 * 2);

    ReplChangeManager.scheduleCMClearer(hiveConfAclPermissions);

    long start = System.currentTimeMillis();
    long end;
    boolean cleared = false;
    do {
      Thread.sleep(200);
      end = System.currentTimeMillis();
      if (end - start > 5000) {
        Assert.fail("timeout, cmroot has not been cleared");
      }
      if (!fsWarehouse.exists(ReplChangeManager.getCMPath(hiveConfAclPermissions, part11.getName(), fileChksum11,
              ReplChangeManager.getInstance(conf).getCmRoot(part11).toString())) &&
              fsWarehouse.exists(ReplChangeManager.getCMPath(hiveConfAclPermissions, part12.getName(), fileChksum12,
                      ReplChangeManager.getInstance(conf).getCmRoot(part12).toString())) &&
              !fsWarehouse.exists(ReplChangeManager.getCMPath(hiveConfAclPermissions, part21.getName(), fileChksum21,
                      ReplChangeManager.getInstance(conf).getCmRoot(part21).toString())) &&
              fsWarehouse.exists(ReplChangeManager.getCMPath(hiveConfAclPermissions, part22.getName(), fileChksum22,
                      ReplChangeManager.getInstance(conf).getCmRoot(part22).toString())) &&
              !fsWarehouse.exists(ReplChangeManager.getCMPath(hiveConfAclPermissions, part31.getName(), fileChksum31,
                      ReplChangeManager.getInstance(conf).getCmRoot(part31).toString())) &&
              !fsWarehouse.exists(ReplChangeManager.getCMPath(hiveConfAclPermissions, part32.getName(), fileChksum32,
                      ReplChangeManager.getInstance(conf).getCmRoot(part32).toString()))) {
        cleared = true;
      }
    } while (!cleared);
  }

  @Test
  public void testCmrootEncrypted() throws Exception {
    HiveConf encryptedHiveConf = new HiveConf(TestReplChangeManager.class);
    encryptedHiveConf.setBoolean(HiveConf.ConfVars.REPL_CM_ENABLED.varname, true);
    encryptedHiveConf.setInt(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 60);
    encryptedHiveConf.set(HiveConf.ConfVars.METASTORE_WAREHOUSE.varname,
            "hdfs://" + miniDFSCluster.getNameNode().getHostAndPort()
                    + HiveConf.ConfVars.METASTORE_WAREHOUSE.defaultStrVal);

    String cmrootdirEncrypted = "hdfs://" + miniDFSCluster.getNameNode().getHostAndPort() + "/cmrootDirEncrypted";
    encryptedHiveConf.set(HiveConf.ConfVars.REPL_CM_DIR.varname, cmrootdirEncrypted);
    FileSystem cmrootdirEncryptedFs = new Path(cmrootdirEncrypted).getFileSystem(hiveConf);
    cmrootdirEncryptedFs.mkdirs(new Path(cmrootdirEncrypted));
    encryptedHiveConf.set(HiveConf.ConfVars.REPL_CM_FALLBACK_NONENCRYPTED_DIR.varname, cmrootFallBack);

    //Create cm in encrypted zone
    EncryptionZoneUtils.createEncryptionZone(new Path(cmrootdirEncrypted), "test_key_db", conf);
    ReplChangeManager.resetReplChangeManagerInstance();
    Warehouse warehouseEncrypted = new Warehouse(encryptedHiveConf);
    FileSystem warehouseFsEncrypted = warehouseEncrypted.getWhRoot().getFileSystem(encryptedHiveConf);
    FileSystem fsCmEncrypted = new Path(cmrootdirEncrypted).getFileSystem(encryptedHiveConf);
    fsCmEncrypted.mkdirs(warehouseEncrypted.getWhRoot());

    Path dirDb = new Path(warehouseEncrypted.getWhRoot(), "db3");
    warehouseFsEncrypted.delete(dirDb, true);
    warehouseFsEncrypted.mkdirs(dirDb);
    Path dirTbl1 = new Path(dirDb, "tbl1");
    warehouseFsEncrypted.mkdirs(dirTbl1);
    EncryptionZoneUtils.createEncryptionZone(dirTbl1, "test_key_db", conf);
    Path part11 = new Path(dirTbl1, "part1");
    createFile(part11, "testClearer11");

    boolean exceptionThrown = false;
    try {
      ReplChangeManager.getInstance(encryptedHiveConf).recycle(dirTbl1, RecycleType.MOVE, false);
    } catch (RemoteException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("can't be moved from encryption zone"));
    }
    assertFalse(exceptionThrown);

    Path dirDbUnEncrypted = new Path(warehouseEncrypted.getWhRoot(), "db3en");
    warehouseFsEncrypted.delete(dirDbUnEncrypted, true);
    warehouseFsEncrypted.mkdirs(dirDbUnEncrypted);
    Path dirTblun1 = new Path(dirDbUnEncrypted, "tbl1");
    warehouseFsEncrypted.mkdirs(dirTblun1);
    Path partun11 = new Path(dirTblun1, "part1");
    createFile(partun11, "testClearer11");

    exceptionThrown = false;
    try {
      ReplChangeManager.getInstance(encryptedHiveConf).recycle(dirDbUnEncrypted, RecycleType.MOVE, false);
    } catch (IOException e) {
      exceptionThrown = true;
    }
    assertFalse(exceptionThrown);
    cmrootdirEncryptedFs.delete(new Path(cmrootdirEncrypted), true);
    ReplChangeManager.resetReplChangeManagerInstance();
    initReplChangeManager();
  }

  @Test
  public void testCmrootFallbackEncrypted() throws Exception {
    HiveConf encryptedHiveConf = new HiveConf(TestReplChangeManager.class);
    encryptedHiveConf.setBoolean(HiveConf.ConfVars.REPL_CM_ENABLED.varname, true);
    encryptedHiveConf.setInt(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 60);
    encryptedHiveConf.set(HiveConf.ConfVars.METASTORE_WAREHOUSE.varname,
            "hdfs://" + miniDFSCluster.getNameNode().getHostAndPort()
                    + HiveConf.ConfVars.METASTORE_WAREHOUSE.defaultStrVal);
    String cmrootdirEncrypted = "hdfs://" + miniDFSCluster.getNameNode().getHostAndPort() + "/cmrootIsEncrypted";
    String cmRootFallbackEncrypted = "hdfs://" + miniDFSCluster.getNameNode().getHostAndPort()
            + "/cmrootFallbackEncrypted";
    FileSystem cmrootdirEncryptedFs = new Path(cmrootdirEncrypted).getFileSystem(encryptedHiveConf);
    try {
      cmrootdirEncryptedFs.mkdirs(new Path(cmrootdirEncrypted));
      cmrootdirEncryptedFs.mkdirs(new Path(cmRootFallbackEncrypted));
      encryptedHiveConf.set(HiveConf.ConfVars.REPL_CM_DIR.varname, cmrootdirEncrypted);
      encryptedHiveConf.set(HiveConf.ConfVars.REPL_CM_FALLBACK_NONENCRYPTED_DIR.varname, cmRootFallbackEncrypted);

      //Create cm in encrypted zone
      EncryptionZoneUtils.createEncryptionZone(new Path(cmrootdirEncrypted), "test_key_db", conf);
      EncryptionZoneUtils.createEncryptionZone(new Path(cmRootFallbackEncrypted), "test_key_db", conf);
      ReplChangeManager.resetReplChangeManagerInstance();
      boolean exceptionThrown = false;
      try {
        new Warehouse(encryptedHiveConf);
      } catch (MetaException e) {
        exceptionThrown = true;
        assertTrue(e.getMessage().contains("should not be encrypted"));
      }
      assertTrue(exceptionThrown);
    } finally {
      cmrootdirEncryptedFs.delete(new Path(cmrootdirEncrypted), true);
      cmrootdirEncryptedFs.delete(new Path(cmRootFallbackEncrypted), true);
      ReplChangeManager.resetReplChangeManagerInstance();
      initReplChangeManager();
    }
  }

  @Test
  public void testCmrootFallbackRelative() throws Exception {
    HiveConf encryptedHiveConf = new HiveConf(TestReplChangeManager.class);
    encryptedHiveConf.setBoolean(HiveConf.ConfVars.REPL_CM_ENABLED.varname, true);
    encryptedHiveConf.setInt(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 60);
    encryptedHiveConf.set(HiveConf.ConfVars.METASTORE_WAREHOUSE.varname,
            "hdfs://" + miniDFSCluster.getNameNode().getHostAndPort()
                    + HiveConf.ConfVars.METASTORE_WAREHOUSE.defaultStrVal);
    String cmrootdirEncrypted = "hdfs://" + miniDFSCluster.getNameNode().getHostAndPort() + "/cmrootIsEncrypted";
    String cmRootFallbackEncrypted = "cmrootFallbackEncrypted";
    FileSystem cmrootdirEncryptedFs = new Path(cmrootdirEncrypted).getFileSystem(encryptedHiveConf);
    try {
      cmrootdirEncryptedFs.mkdirs(new Path(cmrootdirEncrypted));
      cmrootdirEncryptedFs.mkdirs(new Path(cmRootFallbackEncrypted));
      encryptedHiveConf.set(HiveConf.ConfVars.REPL_CM_DIR.varname, cmrootdirEncrypted);
      encryptedHiveConf.set(HiveConf.ConfVars.REPL_CM_FALLBACK_NONENCRYPTED_DIR.varname, cmRootFallbackEncrypted);

      //Create cm in encrypted zone
      EncryptionZoneUtils.createEncryptionZone(new Path(cmrootdirEncrypted), "test_key_db", conf);

      ReplChangeManager.resetReplChangeManagerInstance();
      boolean exceptionThrown = false;
      try {
        new Warehouse(encryptedHiveConf);
      } catch (MetaException e) {
        exceptionThrown = true;
        assertTrue(e.getMessage().contains("should be absolute"));
      }
      assertTrue(exceptionThrown);
    } finally {
      cmrootdirEncryptedFs.delete(new Path(cmrootdirEncrypted), true);
      cmrootdirEncryptedFs.delete(new Path(cmRootFallbackEncrypted), true);
      ReplChangeManager.resetReplChangeManagerInstance();
      initReplChangeManager();
    }
  }



  private void createFile(Path path, String content) throws IOException {
    FSDataOutputStream output = path.getFileSystem(hiveConf).create(path);
    output.writeChars(content);
    output.close();
  }

  private void silentDropDatabase(String dbName) throws TException {
    try {
      for (String tableName : client.getTables(dbName, "*")) {
        client.dropTable(dbName, tableName);
      }
      client.dropDatabase(dbName);
    } catch (NoSuchObjectException|InvalidOperationException|MetaException e) {
      // NOP
    }
  }
}
