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

package org.apache.hadoop.hive.metastore.client;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.fb.FbFileStatus;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for listing files.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestGetFileList extends MetaStoreClientTest {
  private AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;
  private FileSystem fs;

  protected static final String DB_NAME = "test_getfile_db";
  protected static final String TABLE_NAME = "test_getfile_table";
  protected static final String FILE_NAME = "sample";
  protected static final String PARTITION1 = "partcol1";
  protected static final String PARTITION2 = "partcol2";
  protected static final int NUM_PARTITIONS = 2;
  protected static final int NUM_FILES_IN_PARTITION  = 5;


  public TestGetFileList(String name, AbstractMetaStoreService metaStore) {
    this.metaStore = metaStore;
  }

  @BeforeClass
  public static void startMetaStores() {
    Map<ConfVars, String> msConf = new HashMap<>();
    Map<String, String> extraConf = new HashMap<>();
    extraConf.put(ConfVars.HIVE_IN_TEST.getVarname(), "true");
    startMetaStores(msConf, extraConf);
  }

  @Before
  public void setUp() throws Exception {
    // Get new client
    client = metaStore.getClient();

    // Clean up the database
    client.dropDatabase(DB_NAME, true, true, true);
    metaStore.cleanWarehouseDirs();

    //Create files in local filesystem
    fs = new LocalFileSystem();
    java.nio.file.Path tempDirWithPrefix = Files.createTempDirectory("GetFileTest_");
    fs.initialize(tempDirWithPrefix.toUri(), metaStore.getConf());
  }

  @After
  public void tearDown() throws Exception {
    try {
        client.close();
    } finally {
      client = null;
    }
  }

  protected AbstractMetaStoreService getMetaStore() {
    return metaStore;
  }

  protected IMetaStoreClient getClient() {
    return client;
  }

  protected void setClient(IMetaStoreClient client) {
    this.client = client;
  }

  @Test
  public void testGetFileListResultCountOnePartition() throws Exception {
    createDatabaseOnePartition();
    List<String> partVals = new ArrayList<>();
    partVals.add("a1");
    GetFileListResponse resp = getClient().getFileList(null, DB_NAME, TABLE_NAME, partVals,  "");
    Assert.assertEquals(NUM_FILES_IN_PARTITION, resp.getFileListDataSize());
  }

  @Test
  public void testGetFileListResultOnePartition() throws Exception {
    createDatabaseOnePartition();
    List<String> partVals = new ArrayList<>();
    partVals.add("a1");
    GetFileListResponse resp = getClient().getFileList(null, DB_NAME, TABLE_NAME, partVals,  "");
    List<String> expectedPaths = new ArrayList<>();
    for (int i = 0; i < NUM_FILES_IN_PARTITION; i++) {
      expectedPaths.add("/" + FILE_NAME + i);
    }

    List<String> actualPaths = new ArrayList<>();
    for (ByteBuffer buffer: resp.getFileListData()) {
      FbFileStatus fileStatus = FbFileStatus.getRootAsFbFileStatus(buffer);
      actualPaths.add(fileStatus.relativePath());
    }
    Collections.sort(actualPaths);
    Assert.assertArrayEquals(expectedPaths.toArray(), actualPaths.toArray());
  }

  @Test
  public void testGetFileListResultMultiPartition() throws Exception {
    createDatabaseMultiPartition();
    List<String> partVals = new ArrayList<>();
    partVals.add("a1");
    partVals.add("1");
    GetFileListResponse resp = getClient().getFileList(null, DB_NAME, TABLE_NAME, partVals,  "");
    List<String> expectedPaths = new ArrayList<>();
    for (int i = 0; i < NUM_FILES_IN_PARTITION; i++) {
      expectedPaths.add("/" + FILE_NAME + i);
    }

    List<String> actualPaths = new ArrayList<>();
    for (ByteBuffer buffer: resp.getFileListData()) {
      FbFileStatus fileStatus = FbFileStatus.getRootAsFbFileStatus(buffer);
      actualPaths.add(fileStatus.relativePath());
    }
    Collections.sort(actualPaths);
    Assert.assertArrayEquals(expectedPaths.toArray(), actualPaths.toArray());
  }

  @Test
  public void testGetFileListNoResult() throws Exception {
    createDatabaseOnePartition();
    List<String> partVals = new ArrayList<>();
    partVals.add("a2");
    Assert.assertThrows(MetaException.class,
            () -> getClient().getFileList(null, DB_NAME, TABLE_NAME, partVals,  ""));
  }



  private void createDatabaseOnePartition() throws Exception {

    Database db = new DatabaseBuilder().
            setName(DB_NAME).
            setLocation(metaStore.getWarehouseRoot().toString()).
            create(client, metaStore.getConf());

    Table table = new TableBuilder()
            .inDb(db)
            .setTableName(TABLE_NAME)
            .addCol("id", "int")
            .addCol("name", "string")
            .addPartCol(PARTITION1, "string")
            .create(client, metaStore.getConf());

    for (int i = 0; i < NUM_PARTITIONS; i++) {
      new PartitionBuilder()
              .inTable(table)
              .addValue("a" + i)
              .addToTable(client, metaStore.getConf());
    }

    // Create files
    for (int i = 0; i < NUM_PARTITIONS; i++) {
      for (int j = 0; j < NUM_FILES_IN_PARTITION; j++) {
        String fileName = db.getLocationUri() + "/" + db.getName() + ".db/" + TABLE_NAME + "/" + PARTITION1 + "=a" + i + "/" + FILE_NAME + j;
        fs.createNewFile(new Path(fileName));
      }
    }
  }

  private void createDatabaseMultiPartition() throws Exception {

    Database db = new DatabaseBuilder().
            setName(DB_NAME).
            setLocation(metaStore.getWarehouseRoot().toString()).
            create(client, metaStore.getConf());

    Table table = new TableBuilder()
            .inDb(db)
            .setTableName(TABLE_NAME)
            .addCol("id", "int")
            .addCol("name", "string")
            .addPartCol(PARTITION1, "string")
            .addPartCol(PARTITION2, "int")
            .create(client, metaStore.getConf());

    for (int i = 0; i < NUM_PARTITIONS; i++) {
      new PartitionBuilder()
              .inTable(table)
              .addValue("a" + i)
              .addValue(Integer.toString(i))
              .addToTable(client, metaStore.getConf());
    }

    // Create files
    for (int i = 0; i < NUM_PARTITIONS; i++) {
      for (int j = 0; j < NUM_FILES_IN_PARTITION; j++) {
        String fileName = db.getLocationUri() + "/" + db.getName() + ".db/" + TABLE_NAME + "/" + PARTITION1 + "=a" + i + "/"
                + PARTITION2 + "=" + i + "/" + FILE_NAME + j;
        fs.createNewFile(new Path(fileName));
      }
    }
  }

}
