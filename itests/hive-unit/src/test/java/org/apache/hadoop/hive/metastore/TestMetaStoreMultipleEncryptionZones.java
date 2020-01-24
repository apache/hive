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
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ReplChangeManager.RecycleType;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

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
  private static HadoopShims.HdfsEncryptionShim shimCm;
  private static String cmrootEncrypted;
  private static String jksFile = System.getProperty("java.io.tmpdir") + "/test.jks";

  @BeforeClass
  public static void setUp() throws Exception {
    //Create secure cluster
    conf = new Configuration();
    conf.set("hadoop.security.key.provider.path", "jceks://file" + jksFile);
    miniDFSCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    DFSTestUtil.createKey("test_key_cm", miniDFSCluster, conf);
    DFSTestUtil.createKey("test_key_db", miniDFSCluster, conf);
    hiveConf = new HiveConf(TestReplChangeManager.class);
    hiveConf.setBoolean(HiveConf.ConfVars.REPLCMENABLED.varname, true);
    hiveConf.setInt(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 60);
    hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname,
            "hdfs://" + miniDFSCluster.getNameNode().getHostAndPort()
                    + HiveConf.ConfVars.METASTOREWAREHOUSE.defaultStrVal);

    cmroot = "hdfs://" + miniDFSCluster.getNameNode().getHostAndPort() + "/cmroot";
    cmrootEncrypted = "/cmrootEncrypted/";
    hiveConf.set(HiveConf.ConfVars.REPLCMDIR.varname, cmroot);
    hiveConf.set(HiveConf.ConfVars.REPLCMENCRYPTEDDIR.varname, cmrootEncrypted);
    warehouse = new Warehouse(hiveConf);
    warehouseFs = warehouse.getWhRoot().getFileSystem(hiveConf);
    fs = new Path(cmroot).getFileSystem(hiveConf);
    fs.mkdirs(warehouse.getWhRoot());

    //Create cm in encrypted zone
    shimCm = ShimLoader.getHadoopShims().createHdfsEncryptionShim(fs, conf);

    try {
      client = new HiveMetaStoreClient(hiveConf);
    } catch (Throwable e) {
      System.err.println("Unable to open the metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw e;
    }
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
    String dbName = "encrdb1";
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
    shimCm.createEncryptionZone(dirTbl1, "test_key_db");
    Path part11 = new Path(dirTbl1, "part1");
    createFile(part11, "testClearer11");

    Path dirTbl2 = new Path(dirDb, tblName2);
    warehouseFs.mkdirs(dirTbl2);
    shimCm.createEncryptionZone(dirTbl2, "test_key_cm");
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
  public void dropTableWithDifferentEncryptionZones() throws Throwable {
    String dbName = "encrdb2";
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
    shimCm.createEncryptionZone(dirTbl1, "test_key_db");
    Path part11 = new Path(dirTbl1, "part1");
    createFile(part11, "testClearer11");

    Path dirTbl2 = new Path(dirDb, tblName2);
    warehouseFs.mkdirs(dirTbl2);
    shimCm.createEncryptionZone(dirTbl2, "test_key_db");
    Path part12 = new Path(dirTbl2, "part2");
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
    shimCm.createEncryptionZone(dirDb, "test_key_db");
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
    shimCm.createEncryptionZone(dirDb, "test_key_db");
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
    shimCm.createEncryptionZone(dirTbl1, "test_key_db");
    Path part11 = new Path(dirTbl1, "part1");
    createFile(part11, "testClearer11");

    Path dirTbl2 = new Path(dirDb, tblName2);
    warehouseFs.mkdirs(dirTbl2);
    shimCm.createEncryptionZone(dirTbl2, "test_key_db");
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
    shimCm.createEncryptionZone(dirTbl1, "test_key_db");
    Path part11 = new Path(dirTbl1, "part1");
    createFile(part11, "testClearer11");

    Path dirTbl2 = new Path(dirDb, tblName2);
    warehouseFs.mkdirs(dirTbl2);
    shimCm.createEncryptionZone(dirTbl2, "test_key_cm");
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
    String dbName = "encrdb7";
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
    warehouseFs.mkdirs(dirDb);
    Path dirTbl1 = new Path(dirDb, tblName1);
    warehouseFs.mkdirs(dirTbl1);
    shimCm.createEncryptionZone(dirTbl1, "test_key_db");
    Path part11 = new Path(dirTbl1, "part1");
    createFile(part11, "testClearer11");

    Path dirTbl2 = new Path(dirDb, tblName2);
    warehouseFs.mkdirs(dirTbl2);
    shimCm.createEncryptionZone(dirTbl2, "test_key_db");
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
  public void truncateTableWithDifferentEncryptionZonesDifferentKey() throws Throwable {
    String dbName = "encrdb8";
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
    warehouseFs.mkdirs(dirDb);
    Path dirTbl1 = new Path(dirDb, tblName1);
    warehouseFs.mkdirs(dirTbl1);
    shimCm.createEncryptionZone(dirTbl1, "test_key_db");
    Path part11 = new Path(dirTbl1, "part1");
    createFile(part11, "testClearer11");

    Path dirTbl2 = new Path(dirDb, tblName2);
    warehouseFs.mkdirs(dirTbl2);
    shimCm.createEncryptionZone(dirTbl2, "test_key_cm");
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
    shimCm.createEncryptionZone(dirDb, "test_key_db");
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

    Path dirDb = new Path(warehouse.getWhRoot(), "db3");
    warehouseFs.mkdirs(dirDb);
    Path dirTbl1 = new Path(dirDb, "tbl1");
    warehouseFs.mkdirs(dirTbl1);
    shimCm.createEncryptionZone(dirTbl1, "test_key_db");
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
    } catch (NoSuchObjectException|InvalidOperationException e) {
      // NOP
    }
  }
}
