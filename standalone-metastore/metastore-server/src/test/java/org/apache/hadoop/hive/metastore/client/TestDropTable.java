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

import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.handler.AbstractRequestHandler;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.AsyncOperationResp;
import org.apache.hadoop.hive.metastore.api.DropTableRequest;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionsRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestDropTable extends MetaStoreClientTest {
  protected static final String DB_NAME = "testdroptable";
  protected static final String TABLE_NAME = "test_drop_table";
  private final String testTempDir = Paths.get(System.getProperty("java.io.tmpdir"), "testDropTable").toString();
  private AbstractMetaStoreService metaStore;
  private HiveMetaStoreClient client;
  private ThriftHiveMetastore.Iface iface;

  public TestDropTable(String name, AbstractMetaStoreService metaStore) {
    this.metaStore = metaStore;
  }

  @BeforeClass
  public static void startMetaStores() {
    Map<MetastoreConf.ConfVars, String> msConf = new HashMap<>();
    // Enable trash, so it can be tested
    Map<String, String> extraConf = new HashMap<>();
    extraConf.put("metastore.limit.partition.request", "1500");
    extraConf.put("metastore.direct.sql.batch.size", "100");
    extraConf.put("metastore.rawstore.batch.size", "500");
    extraConf.put("hive.in.test", "true");
    startMetaStores(msConf, extraConf);
  }

  @Before
  public void setUp() throws Exception {
    // Get new client
    client = metaStore.getClient();
    Field field = ThriftHiveMetaStoreClient.class.getDeclaredField("client");
    field.setAccessible(true);
    iface = (ThriftHiveMetastore.Iface) field.get(client.getThriftClient());
    // Clean up the database
    cleanDB();
    createDB(DB_NAME);
  }

  private Table createTable(
      boolean external,
      List<FieldSchema> partCols,
      String location) throws Exception {
    new TableBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .addCol("test_id", "int", "test col id")
        .addCol("test_value", "string", "test col value")
        .addTableParam("param_key", "param_value")
        .setType(external ? TableType.EXTERNAL_TABLE.name()
            : TableType.MANAGED_TABLE.name())
        .addTableParam(external ? "EXTERNAL" : "param_key_2", "TRUE")
        .setPartCols(partCols)
        .addStorageDescriptorParam("sd_param_key", "sd_param_value")
        .setSerdeName(TABLE_NAME)
        .setStoredAsSubDirectories(false)
        .addSerdeParam("serde_param_key", "serde_param_value")
        .setLocation(location)
        .create(client, metaStore.getConf());
    return client.getTable(DB_NAME, TABLE_NAME);
  }

  private void addPartitions(Table table, int partSize,
      String outsideTabLocation) throws Exception {
    List<Partition> partitions = new ArrayList<>(partSize);
    for (int i = 0; i < partSize / 2; i++) {
      partitions.add(new PartitionBuilder().inTable(table)
          .addValue("part_" + i)
          .addPartParam("part_param_key", "part_param_value")
          .build(metaStore.getConf()));
    }
    for (int i = partSize / 2; i < partSize; i++) {
      partitions.add(new PartitionBuilder().inTable(table)
          .addValue("part_" + i)
          .addPartParam("part_param_key", "part_param_value")
          .setLocation(outsideTabLocation + "/part_" + i)
          .build(metaStore.getConf()));
    }
    client.add_partitions(partitions);
  }

  @Test
  public void testDropNonPartitionedTbl() throws Exception {
    GetTableRequest getTableRequest = new GetTableRequest(DB_NAME, TABLE_NAME);
    getTableRequest.setGetColumnStats(true);
    for (int i = 0; i < 2; i++) {
      boolean external = i % 2 == 0;
      String location = external ?
          (metaStore.getExternalWarehouseRoot() + "/" + TABLE_NAME) : (metaStore.getWarehouseRoot() + "/" + TABLE_NAME);
      String tableName = DB_NAME + "." + TABLE_NAME;
      createTable(external, null, location);
      client.dropTable(DB_NAME, TABLE_NAME);
      try {
        client.getTable(getTableRequest);
        fail("Get table should fail as it must be dropped");
      } catch (NoSuchObjectException e) {
        assertTrue(e.getMessage().contains(tableName + " table not found"));
      }
      Path tblPath = new Path(location);
      assertEquals(external, metaStore.isPathExists(tblPath));
      if (external) {
        tblPath.getFileSystem(metaStore.getConf()).delete(tblPath, true);
      }
    }
  }

  @Test
  public void testDropPartitionedTbl() throws Exception {
    GetTableRequest getTableRequest = new GetTableRequest(DB_NAME, TABLE_NAME);
    getTableRequest.setGetColumnStats(true);
    for (int i = 0; i < 2; i++) {
      boolean external = i % 2 == 0;
      String location = external ?
          (metaStore.getExternalWarehouseRoot() + "/" + TABLE_NAME) : (metaStore.getWarehouseRoot() + "/" + TABLE_NAME);
      String tableName = DB_NAME + "." + TABLE_NAME;
      Table table =
          createTable(external, Arrays.asList(new FieldSchema("part_col1", "string", "partition column")), location);
      Path testTempPath = new Path(testTempDir + "/" + location);
      int partSize = 100;
      addPartitions(table, partSize, testTempPath.toString());
      assertTrue(metaStore.isPathExists(testTempPath));
      FileStatus[] dirs = testTempPath.getFileSystem(metaStore.getConf()).listStatus(testTempPath, name -> true);
      assertEquals(partSize / 2, dirs.length);

      client.dropTable(DB_NAME, TABLE_NAME);
      try {
        client.getTable(getTableRequest);
        fail("Get table should fail as it must be dropped");
      } catch (NoSuchObjectException e) {
        assertTrue(e.getMessage().contains(tableName + " table not found"));
      }

      Path tblPath = new Path(location);
      assertEquals(external, metaStore.isPathExists(tblPath));
      dirs = testTempPath.getFileSystem(metaStore.getConf()).listStatus(testTempPath, name -> true);
      assertEquals(dirs.length, (external ? partSize / 2 : 0));
      if (external) {
        tblPath.getFileSystem(metaStore.getConf()).delete(tblPath, true);
        testTempPath.getFileSystem(metaStore.getConf()).delete(testTempPath, true);
      }
    }
  }

  @Test
  public void testDropProgress() throws Exception {
    String location = metaStore.getWarehouseRoot() + "/" + TABLE_NAME;
    String tableName = DB_NAME + "." + TABLE_NAME;
    Table table =
        createTable(true, Arrays.asList(new FieldSchema("part_col1", "string", "partition column")), location);
    Path testTempPath = new Path(testTempDir + "/" + location);
    int partSize = 100;
    addPartitions(table, partSize, testTempPath.toString());
    assertTrue(metaStore.isPathExists(testTempPath));

    DropTableRequest dropTableReq = new DropTableRequest(DB_NAME, TABLE_NAME);
    dropTableReq.setDeleteData(true);
    dropTableReq.setDropPartitions(true);
    dropTableReq.setEnvContext(null);
    dropTableReq.setAsyncDrop(true);
    AsyncOperationResp resp = iface.drop_table_req(dropTableReq);
    assertNotNull(resp.getMessage());
    dropTableReq.setId(resp.getId());
    while (!resp.isFinished()) {
      assertTrue(AbstractRequestHandler.containsRequest(dropTableReq.getId()));
      resp = iface.drop_table_req(dropTableReq);
      assertNotNull(resp.getMessage());
    }

    assertTrue(resp.isFinished());
    try {
      GetTableRequest getTableRequest = new GetTableRequest(DB_NAME, TABLE_NAME);
      getTableRequest.setGetColumnStats(true);
      client.getTable(getTableRequest);
      fail("Get table should fail as it must be dropped");
    } catch (NoSuchObjectException e) {
      assertTrue(e.getMessage().contains(tableName + " table not found"));
    }
  }

  @Test
  public void cancelDropTable() throws Exception {
    String location = metaStore.getWarehouseRoot() + "/" + TABLE_NAME;
    String tableName = DB_NAME + "." + TABLE_NAME;
    Table table =
        createTable(true, Arrays.asList(new FieldSchema("part_col1", "string", "partition column")), location);
    Path testTempPath = new Path(testTempDir + "/" + location);
    int partSize = 1000;
    addPartitions(table, partSize, testTempPath.toString());
    assertTrue(metaStore.isPathExists(testTempPath));

    DropTableRequest dropTableReq = new DropTableRequest(DB_NAME, TABLE_NAME);
    dropTableReq.setDeleteData(true);
    dropTableReq.setDropPartitions(true);
    dropTableReq.setEnvContext(null);
    dropTableReq.setAsyncDrop(true);
    AsyncOperationResp resp = iface.drop_table_req(dropTableReq);

    // cancel the request
    dropTableReq.setId(resp.getId());
    dropTableReq.setCancel(true);
    assertTrue(AbstractRequestHandler.containsRequest(dropTableReq.getId()));
    resp = iface.drop_table_req(dropTableReq);
    assertTrue(resp.isFinished());
    assertTrue(resp.getMessage().contains("table " + table.getCatName() + "."+ tableName + ": Canceled"));

    PartitionsRequest req = new PartitionsRequest();
    req.setDbName(DB_NAME);
    req.setTblName(TABLE_NAME);
    req.setMaxParts((short)-1);
    List<Partition> partitions = client.getPartitionsRequest(req).getPartitions();
    assertEquals(partSize, partitions.size());

    client.dropTable(DB_NAME, TABLE_NAME);
    try {
      GetTableRequest getTableRequest = new GetTableRequest(DB_NAME, TABLE_NAME);
      getTableRequest.setGetColumnStats(true);
      client.getTable(getTableRequest);
      fail("Get table should fail as it must be dropped");
    } catch (NoSuchObjectException e) {
      assertTrue(e.getMessage().contains(tableName + " table not found"));
    }
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (client != null) {
        try {
          client.close();
        } catch (Exception e) {
          // HIVE-19729: Shallow the exceptions based on the discussion in the Jira
        }
      }
    } finally {
      client = null;
    }

    Path path = new Path(testTempDir);
    path.getFileSystem(metaStore.getConf()).delete(path, true);
  }

  protected void cleanDB() throws Exception{
    client.dropDatabase(DB_NAME, true, true, true);
    metaStore.cleanWarehouseDirs();
  }

  protected void createDB(String dbName) throws TException {
    new DatabaseBuilder().
        setName(dbName).
        create(client, metaStore.getConf());
  }
}
