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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ReplChangeManager.RecycleType;
import static org.apache.hadoop.hive.metastore.ReplChangeManager.SOURCE_OF_REPLICATION;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
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

import com.google.common.collect.ImmutableMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * TestMetaStoreAuthorization.
 */
public class TestMetaStoreMultipleEncryptionZones {
  private static HiveMetaStoreClient client;
  private static HiveMetaStoreClient clientEncrypted;
  private static HiveConf hiveConf;
  private static HiveConf hiveConfEncrypted;
  private static Configuration conf;
  private static Warehouse warehouse;
  private static FileSystem warehouseFs;
  private static Warehouse warehouseEncrypted;
  private static FileSystem warehouseFsEncrypted;
  private static MiniDFSCluster m_dfs;
  private static String cmroot;
  private static FileSystem fs;
  private static String cmrootEncrypted;
  private static FileSystem fsEncrypted;
  private static String jksFile = System.getProperty("java.io.tmpdir") + "/test.jks";

  @BeforeClass
  public static void setUp() throws Exception {
    //Create secure cluster
    conf = new Configuration();
    conf.set("hadoop.security.key.provider.path", "jceks://file" + jksFile);
    m_dfs = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    DFSTestUtil.createKey("test_key_cm", m_dfs, conf);
    DFSTestUtil.createKey("test_key_warehouse", m_dfs, conf);
    hiveConf = new HiveConf(TestReplChangeManager.class);
    hiveConf.setBoolean(HiveConf.ConfVars.REPLCMENABLED.varname, true);
    hiveConf.setInt(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 60);
    hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname,
            "hdfs://" + m_dfs.getNameNode().getHostAndPort() + HiveConf.ConfVars.METASTOREWAREHOUSE.defaultStrVal);

    cmroot = "hdfs://" + m_dfs.getNameNode().getHostAndPort() + "/cmroot";
    hiveConf.set(HiveConf.ConfVars.REPLCMDIR.varname, cmroot);
    warehouse = new Warehouse(hiveConf);
    warehouseFs = warehouse.getWhRoot().getFileSystem(hiveConf);
    fs = new Path(cmroot).getFileSystem(hiveConf);
    fs.mkdirs(warehouse.getWhRoot());

    hiveConfEncrypted = new HiveConf(TestReplChangeManager.class);
    hiveConfEncrypted.setBoolean(HiveConf.ConfVars.REPLCMENABLED.varname, true);
    hiveConfEncrypted.setInt(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 60);
    cmrootEncrypted = "hdfs://" + m_dfs.getNameNode().getHostAndPort() + "/cmrootEncrypted";
    hiveConfEncrypted.set(HiveConf.ConfVars.REPLCMDIR.varname, cmrootEncrypted);
    hiveConfEncrypted.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname,
            "hdfs://" + m_dfs.getNameNode().getHostAndPort() + "/user/hive/warehouseEncrypted");
    warehouseEncrypted = new Warehouse(hiveConfEncrypted);
    warehouseFsEncrypted = warehouseEncrypted.getWhRoot().getFileSystem(hiveConfEncrypted);
    fsEncrypted = new Path(cmrootEncrypted).getFileSystem(hiveConfEncrypted);
    fsEncrypted.mkdirs(warehouseEncrypted.getWhRoot());
    fsEncrypted.mkdirs(new Path(cmrootEncrypted));
    //Create cm in encrypted zone
    HadoopShims.HdfsEncryptionShim shimCm
            = ShimLoader.getHadoopShims().createHdfsEncryptionShim(fsEncrypted, conf);
    shimCm.createEncryptionZone(new Path(cmrootEncrypted), "test_key_cm");
    //Create warehouse in different encrypted zone
    HadoopShims.HdfsEncryptionShim shimWarehouse
            = ShimLoader.getHadoopShims().createHdfsEncryptionShim(warehouseFsEncrypted, conf);
    shimWarehouse.createEncryptionZone(warehouseEncrypted.getWhRoot(), "test_key_warehouse");

    try {
      client = new HiveMetaStoreClient(hiveConf);
    } catch (Throwable e) {
      System.err.println("Unable to open the metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw e;
    }
    try {
      clientEncrypted = new HiveMetaStoreClient(hiveConfEncrypted);
    } catch (Throwable e) {
      System.err.println("Unable to open the metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw e;
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    try {
      m_dfs.shutdown();
      client.close();
      clientEncrypted.close();
    } catch (Throwable e) {
      System.err.println("Unable to close metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw e;
    }
  }

  @Test
  public void dropTableFailureWithDifferentEncryptionZonesForCm() throws Throwable {
    String dbName = "simpdb";
    String tblName = "simptbl";
    String typeName = "Person";
    clientEncrypted.dropTable(dbName, tblName);
    silentDropDatabase(dbName, clientEncrypted);

    new DatabaseBuilder()
            .setName(dbName)
            .addParam("repl.source.for", "1, 2, 3")
            .create(clientEncrypted, hiveConfEncrypted);

    clientEncrypted.dropType(typeName);
    Type typ1 = new Type();
    typ1.setName(typeName);
    typ1.setFields(new ArrayList<>(2));
    typ1.getFields().add(
            new FieldSchema("name", ColumnType.STRING_TYPE_NAME, ""));
    typ1.getFields().add(
            new FieldSchema("income", ColumnType.INT_TYPE_NAME, ""));
    clientEncrypted.createType(typ1);

    Table tbl = new TableBuilder()
            .setDbName(dbName)
            .setTableName(tblName)
            .setCols(typ1.getFields())
            .setNumBuckets(1)
            .addBucketCol("name")
            .addStorageDescriptorParam("test_param_1", "Use this for comments etc")
            .create(clientEncrypted, hiveConfEncrypted);

    Table tbl2 = clientEncrypted.getTable(dbName, tblName);
    Assert.assertNotNull(tbl2);

    Path dirDb = new Path(warehouseEncrypted.getWhRoot(), dbName +".db");
    warehouseFsEncrypted.mkdirs(dirDb);
    Path dirTbl1 = new Path(dirDb, tblName);
    warehouseFsEncrypted.mkdirs(dirTbl1);
    Path part11 = new Path(dirTbl1, "part1");
    createFile(part11, "testClearer11", hiveConfEncrypted);

    boolean exceptionThrown = false;
    try {
      clientEncrypted.dropTable(dbName, tblName);
    } catch (MetaException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("can't be moved from encryption zone"));
    }
    assertFalse(exceptionThrown);
    assertFalse(warehouseFsEncrypted.exists(part11));
    try {
      clientEncrypted.getTable(dbName, tblName);
    } catch (NoSuchObjectException e) {
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);
  }

  @Test
  public void dropTableWithoutEncryptionZonesForCm() throws Throwable {
    String dbName = "simpdb";
    String tblName = "simptbl";
    String typeName = "Person";
    client.dropTable(dbName, tblName);
    silentDropDatabase(dbName, client);

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

    Table tbl = new TableBuilder()
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
    createFile(part11, "testClearer11", hiveConf);

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
  public void dropExternalTableWithDifferentEncryptionZonesForCm() throws Throwable {
    String dbName = "simpdb";
    String tblName = "simptbl";
    String typeName = "Person";
    clientEncrypted.dropTable(dbName, tblName);
    silentDropDatabase(dbName, clientEncrypted);

    new DatabaseBuilder()
            .setName(dbName)
            .addParam("repl.source.for", "1, 2, 3")
            .create(clientEncrypted, hiveConfEncrypted);

    clientEncrypted.dropType(typeName);
    Type typ1 = new Type();
    typ1.setName(typeName);
    typ1.setFields(new ArrayList<>(2));
    typ1.getFields().add(
            new FieldSchema("name", ColumnType.STRING_TYPE_NAME, ""));
    typ1.getFields().add(
            new FieldSchema("income", ColumnType.INT_TYPE_NAME, ""));
    clientEncrypted.createType(typ1);

    Table tbl = new TableBuilder()
            .setDbName(dbName)
            .setTableName(tblName)
            .setCols(typ1.getFields())
            .setNumBuckets(1)
            .addBucketCol("name")
            .addTableParam("EXTERNAL", "true")
            .addTableParam("external.table.purge", "true")
            .addStorageDescriptorParam("test_param_1", "Use this for comments etc")
            .create(clientEncrypted, hiveConfEncrypted);

    Table tbl2 = clientEncrypted.getTable(dbName, tblName);
    Assert.assertNotNull(tbl2);

    Path dirDb = new Path(warehouseEncrypted.getWhRoot(), dbName +".db");
    warehouseFsEncrypted.mkdirs(dirDb);
    Path dirTbl1 = new Path(dirDb, tblName);
    warehouseFsEncrypted.mkdirs(dirTbl1);
    Path part11 = new Path(dirTbl1, "part1");
    createFile(part11, "testClearer11", hiveConfEncrypted);

    boolean exceptionThrown = false;
    try {
      clientEncrypted.dropTable(dbName, tblName);
    } catch (MetaException e) {
      exceptionThrown = true;
    }
    assertFalse(exceptionThrown);
    assertFalse(warehouseFsEncrypted.exists(part11));
    try {
      clientEncrypted.getTable(dbName, tblName);
    } catch (NoSuchObjectException e) {
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);
  }

  @Test
  public void dropExternalTableWithoutEncryptionZonesForCm() throws Throwable {
    String dbName = "simpdb";
    String tblName = "simptbl";
    String typeName = "Person";
    client.dropTable(dbName, tblName);
    silentDropDatabase(dbName, client);

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

    Table tbl = new TableBuilder()
            .setDbName(dbName)
            .setTableName(tblName)
            .setCols(typ1.getFields())
            .setNumBuckets(1)
            .addBucketCol("name")
            .addTableParam("EXTERNAL", "true")
            .addTableParam("external.table.purge", "true")
            .addStorageDescriptorParam("test_param_1", "Use this for comments etc")
            .create(client, hiveConf);

    Table tbl2 = client.getTable(dbName, tblName);
    Assert.assertNotNull(tbl2);

    Path dirDb = new Path(warehouse.getWhRoot(), dbName +".db");
    warehouseFs.mkdirs(dirDb);
    Path dirTbl1 = new Path(dirDb, tblName);
    warehouseFs.mkdirs(dirTbl1);
    Path part11 = new Path(dirTbl1, "part1");
    createFile(part11, "testClearer11", hiveConf);

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
  public void truncateTableFailureWithDifferentEncryptionZonesForCm() throws Throwable {
    String dbName = "simpdb";
    String tblName = "simptbl";
    String typeName = "Person";
    clientEncrypted.dropTable(dbName, tblName);
    silentDropDatabase(dbName, clientEncrypted);

    new DatabaseBuilder()
            .setName(dbName)
            .addParam("repl.source.for", "1, 2, 3")
            .create(clientEncrypted, hiveConfEncrypted);

    clientEncrypted.dropType(typeName);
    Type typ1 = new Type();
    typ1.setName(typeName);
    typ1.setFields(new ArrayList<>(2));
    typ1.getFields().add(
            new FieldSchema("name", ColumnType.STRING_TYPE_NAME, ""));
    typ1.getFields().add(
            new FieldSchema("income", ColumnType.INT_TYPE_NAME, ""));
    clientEncrypted.createType(typ1);

    Table tbl = new TableBuilder()
            .setDbName(dbName)
            .setTableName(tblName)
            .setCols(typ1.getFields())
            .setNumBuckets(1)
            .addBucketCol("name")
            .addStorageDescriptorParam("test_param_1", "Use this for comments etc")
            .create(clientEncrypted, hiveConfEncrypted);

    Table tbl2 = clientEncrypted.getTable(dbName, tblName);
    Assert.assertNotNull(tbl2);

    Path dirDb = new Path(warehouseEncrypted.getWhRoot(), dbName +".db");
    warehouseFsEncrypted.mkdirs(dirDb);
    Path dirTbl1 = new Path(dirDb, tblName);
    warehouseFsEncrypted.mkdirs(dirTbl1);
    Path part11 = new Path(dirTbl1, "part1");
    createFile(part11, "testClearer11", hiveConfEncrypted);

    boolean exceptionThrown = false;
    try {
      clientEncrypted.truncateTable(dbName, tblName, null);
    } catch (MetaException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("can't be moved from encryption zone"));
    }
    assertFalse(exceptionThrown);
    assertFalse(warehouseFsEncrypted.exists(part11));
    Assert.assertNotNull(clientEncrypted.getTable(dbName, tblName));
  }

  @Test
  public void truncateTableWithoutEncryptionZonesForCm() throws Throwable {
    String dbName = "simpdb";
    String tblName = "simptbl";
    String typeName = "Person";
    client.dropTable(dbName, tblName);
    silentDropDatabase(dbName, client);

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

    Table tbl = new TableBuilder()
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
    createFile(part11, "testClearer11", hiveConf);

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

    long now = System.currentTimeMillis();
    Path dirDb = new Path(warehouseEncrypted.getWhRoot(), "db3");
    warehouseFsEncrypted.mkdirs(dirDb);
    Path dirTbl1 = new Path(dirDb, "tbl1");
    warehouseFsEncrypted.mkdirs(dirTbl1);
    Path part11 = new Path(dirTbl1, "part1");
    createFile(part11, "testClearer11", hiveConfEncrypted);
    String fileChksum11 = ReplChangeManager.checksumFor(part11, warehouseFsEncrypted);
    boolean exceptionThrown = false;
    try {
      ReplChangeManager.getInstance(hiveConfEncrypted).recycle(dirTbl1, RecycleType.MOVE, false);
    } catch (RemoteException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("can't be moved from encryption zone"));
    }
    assertFalse(exceptionThrown);
  }




  private static void silentDropDatabase(String dbName, HiveMetaStoreClient client) throws MetaException, TException{
    try {
      for (String tableName : client.getTables(dbName, "*")) {
        client.dropTable(dbName, tableName);
      }
      client.dropDatabase(dbName);
    } catch (NoSuchObjectException|InvalidOperationException e) {
      // NOP
    }
  }

  private void createFile(Path path, String content, HiveConf hiveConf) throws IOException {
    FSDataOutputStream output = path.getFileSystem(hiveConf).create(path);
    output.writeChars(content);
    output.close();
  }
}
