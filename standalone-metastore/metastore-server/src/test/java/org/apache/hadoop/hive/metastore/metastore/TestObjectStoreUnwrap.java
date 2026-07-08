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

package org.apache.hadoop.hive.metastore.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.Deadline;
import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.GetPartitionsArgs;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.metastore.iface.TableStore;
import org.apache.hadoop.hive.metastore.utils.DirectSqlConfigurator;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;

@Category(MetastoreUnitTest.class)
public class TestObjectStoreUnwrap {
  private static final String DB = "unwrap_proxy_db";
  private static final String TABLE = "unwrap_proxy_tbl";

  private ObjectStore objectStore;
  private Configuration conf;
  private TableName tableName;

  @Before
  public void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, ConfVars.HIVE_IN_TEST, true);
    MetaStoreTestUtils.setConfForStandloneMode(conf);

    String currentUrl = MetastoreConf.getVar(conf, ConfVars.CONNECT_URL_KEY);
    currentUrl = currentUrl.replace(MetaStoreServerUtils.JUNIT_DATABASE_PREFIX,
        String.format("%s_%s", MetaStoreServerUtils.JUNIT_DATABASE_PREFIX, UUID.randomUUID()));
    MetastoreConf.setVar(conf, ConfVars.CONNECT_URL_KEY, currentUrl);

    objectStore = new ObjectStore();
    objectStore.setConf(conf);
    HMSHandler.createDefaultCatalog(objectStore, new Warehouse(conf));
    tableName = new TableName(DEFAULT_CATALOG_NAME, DB, TABLE);
    createPartitionedTable();
  }

  @Test
  public void testQueryClosedAfterRead() throws Exception {
    TableStore tableStore = objectStore.unwrap(TableStore.class);
    TransactionHandler<?> handler = getHandler(tableStore);
    GetPartitionsArgs args = new GetPartitionsArgs.GetPartitionsArgsBuilder().max(10).build();

    try (AutoCloseable ignored = deadline();
         DirectSqlConfigurator directSql = new DirectSqlConfigurator(conf, false)) {
      List<Partition> partitions = tableStore.getPartitions(tableName, args);
      Assert.assertEquals(3, partitions.size());
    }

    Assert.assertEquals(0, handler.getOpenQueryCount());
    Assert.assertFalse(objectStore.isActiveTransaction());
  }

  @Test
  public void testQueryClosedAfterDelete() throws Exception {
    TableStore tableStore = objectStore.unwrap(TableStore.class);
    TransactionHandler<?> handler = getHandler(tableStore);

    try (AutoCloseable ignored = deadline();
         DirectSqlConfigurator directSql = new DirectSqlConfigurator(conf, false)) {
      tableStore.dropPartitions(tableName, Arrays.asList("test_part_col=a0", "test_part_col=a1"));
    }

    Assert.assertEquals(0, handler.getOpenQueryCount());
    Assert.assertFalse(objectStore.isActiveTransaction());

    try (AutoCloseable ignored = deadline();
         DirectSqlConfigurator directSql = new DirectSqlConfigurator(conf, false)) {
      Assert.assertEquals(1, tableStore.getPartitions(tableName,
          new GetPartitionsArgs.GetPartitionsArgsBuilder().max(10).build()).size());
    }
    Assert.assertEquals(0, handler.getOpenQueryCount());
  }

  @Test
  public void testRepeatedUnwrapNoLeaks() throws Exception {
    GetPartitionsArgs args = new GetPartitionsArgs.GetPartitionsArgsBuilder().max(10).build();
    try (AutoCloseable ignored = deadline();
         DirectSqlConfigurator directSql = new DirectSqlConfigurator(conf, false)) {
      for (int i = 0; i < 5; i++) {
        TableStore tableStore = objectStore.unwrap(TableStore.class);
        TransactionHandler<?> handler = getHandler(tableStore);
        Assert.assertEquals(3, tableStore.getPartitions(tableName, args).size());
        Assert.assertEquals("queries leaked on iteration " + i, 0, handler.getOpenQueryCount());
      }
    }
    Assert.assertFalse(objectStore.isActiveTransaction());
  }

  @Test
  public void testProxiesReadThenDelete() throws Exception {
    TableStore readProxy = objectStore.unwrap(TableStore.class);
    TableStore deleteProxy = objectStore.unwrap(TableStore.class);
    Assert.assertNotSame(readProxy, deleteProxy);

    TransactionHandler<?> readHandler = getHandler(readProxy);
    TransactionHandler<?> deleteHandler = getHandler(deleteProxy);
    GetPartitionsArgs args = new GetPartitionsArgs.GetPartitionsArgsBuilder().max(10).build();

    try (AutoCloseable ignored = deadline();
         DirectSqlConfigurator directSql = new DirectSqlConfigurator(conf, false)) {
      Assert.assertEquals(3, readProxy.getPartitions(tableName, args).size());
      Assert.assertEquals(0, readHandler.getOpenQueryCount());
      Assert.assertEquals(0, deleteHandler.getOpenQueryCount());

      Assert.assertEquals(3, deleteProxy.getPartitions(tableName, args).size());
      Assert.assertEquals(0, deleteHandler.getOpenQueryCount());
      Assert.assertEquals(0, readHandler.getOpenQueryCount());

      deleteProxy.dropPartitions(tableName, Arrays.asList("test_part_col=a0", "test_part_col=a1"));
      Assert.assertEquals(0, deleteHandler.getOpenQueryCount());

      Assert.assertEquals(1, readProxy.getPartitions(tableName, args).size());
      Assert.assertEquals(0, readHandler.getOpenQueryCount());
    }
    Assert.assertFalse(objectStore.isActiveTransaction());
  }

  @Test
  public void testProxiesInterleavedOps() throws Exception {
    TableStore proxyA = objectStore.unwrap(TableStore.class);
    TableStore proxyB = objectStore.unwrap(TableStore.class);
    TransactionHandler<?> handlerA = getHandler(proxyA);
    TransactionHandler<?> handlerB = getHandler(proxyB);
    GetPartitionsArgs args = new GetPartitionsArgs.GetPartitionsArgsBuilder().max(10).build();

    try (AutoCloseable ignored = deadline();
         DirectSqlConfigurator directSql = new DirectSqlConfigurator(conf, false)) {
      Assert.assertEquals(3, proxyA.getPartitions(tableName, args).size());
      Assert.assertEquals(0, handlerA.getOpenQueryCount());

      Assert.assertEquals(3, proxyB.getPartitions(tableName, args).size());
      Assert.assertEquals(0, handlerB.getOpenQueryCount());

      proxyA.dropPartitions(tableName, Collections.singletonList("test_part_col=a0"));
      Assert.assertEquals(0, handlerA.getOpenQueryCount());

      Assert.assertEquals(2, proxyB.getPartitions(tableName, args).size());
      Assert.assertEquals(0, handlerB.getOpenQueryCount());

      proxyB.dropPartitions(tableName, Collections.singletonList("test_part_col=a1"));
      Assert.assertEquals(0, handlerB.getOpenQueryCount());

      Assert.assertEquals(1, proxyA.getPartitions(tableName, args).size());
      Assert.assertEquals(0, handlerA.getOpenQueryCount());
    }
    Assert.assertFalse(objectStore.isActiveTransaction());
  }

  @Test
  public void testProxiesVisibleInTxn() throws Exception {
    TableStore proxyA = objectStore.unwrap(TableStore.class);
    TableStore proxyB = objectStore.unwrap(TableStore.class);
    Assert.assertNotSame(proxyA, proxyB);

    TransactionHandler<?> handlerA = getHandler(proxyA);
    TransactionHandler<?> handlerB = getHandler(proxyB);
    GetPartitionsArgs args = new GetPartitionsArgs.GetPartitionsArgsBuilder().max(10).build();

    try (AutoCloseable ignored = deadline();
         DirectSqlConfigurator directSql = new DirectSqlConfigurator(conf, false)) {
      objectStore.openTransaction();
      try {
        Assert.assertTrue(objectStore.isActiveTransaction());

        Assert.assertEquals(3, proxyA.getPartitions(tableName, args).size());
        Assert.assertEquals(0, handlerA.getOpenQueryCount());

        proxyA.dropPartitions(tableName, Arrays.asList("test_part_col=a0", "test_part_col=a1"));
        Assert.assertEquals(0, handlerA.getOpenQueryCount());
        Assert.assertTrue(objectStore.isActiveTransaction());

        // Uncommitted deletes from proxyA are visible to proxyB within the same transaction.
        Assert.assertEquals(1, proxyB.getPartitions(tableName, args).size());
        Assert.assertEquals(0, handlerB.getOpenQueryCount());

        proxyB.dropPartitions(tableName, Collections.singletonList("test_part_col=a2"));
        Assert.assertEquals(0, handlerB.getOpenQueryCount());

        // Uncommitted deletes from proxyB are visible to proxyA within the same transaction.
        Assert.assertEquals(0, proxyA.getPartitions(tableName, args).size());
        Assert.assertEquals(0, handlerA.getOpenQueryCount());

        objectStore.commitTransaction();
      } catch (Exception e) {
        objectStore.rollbackTransaction();
        throw e;
      }
    }
    Assert.assertFalse(objectStore.isActiveTransaction());

    try (AutoCloseable ignored = deadline();
         DirectSqlConfigurator directSql = new DirectSqlConfigurator(conf, false)) {
      Assert.assertEquals(0, proxyA.getPartitions(tableName, args).size());
      Assert.assertEquals(0, proxyB.getPartitions(tableName, args).size());
    }
  }

  @Test
  public void testProxiesRollbackInTxn() throws Exception {
    TableStore proxyA = objectStore.unwrap(TableStore.class);
    TableStore proxyB = objectStore.unwrap(TableStore.class);
    GetPartitionsArgs args = new GetPartitionsArgs.GetPartitionsArgsBuilder().max(10).build();

    try (AutoCloseable ignored = deadline();
         DirectSqlConfigurator directSql = new DirectSqlConfigurator(conf, false)) {
      objectStore.openTransaction();
      try {
        proxyA.dropPartitions(tableName, Arrays.asList("test_part_col=a0", "test_part_col=a1"));
        Assert.assertEquals(1, proxyB.getPartitions(tableName, args).size());
        objectStore.rollbackTransaction();
      } catch (Exception e) {
        objectStore.rollbackTransaction();
        throw e;
      }
    }
    Assert.assertFalse(objectStore.isActiveTransaction());

    try (AutoCloseable ignored = deadline();
         DirectSqlConfigurator directSql = new DirectSqlConfigurator(conf, false)) {
      Assert.assertEquals(3, proxyA.getPartitions(tableName, args).size());
      Assert.assertEquals(3, proxyB.getPartitions(tableName, args).size());
    }
  }

  @Test
  public void testCreateTableSharesPm() throws Exception {
    String otherDb = "unwrap_other_db";
    String otherTable = "unwrap_other_tbl";
    objectStore.createDatabase(new DatabaseBuilder()
        .setName(otherDb)
        .setDescription("description")
        .setLocation("locationurl")
        .build(conf));

    Table table = new TableBuilder()
        .setDbName(otherDb)
        .setTableName(otherTable)
        .addCol("c", "int")
        .build(conf);

    TableStore tableStore = objectStore.unwrap(TableStore.class);
    tableStore.createTable(table);

    Table fetched = tableStore.getTable(
        new TableName(DEFAULT_CATALOG_NAME, otherDb, otherTable), null, -1);
    Assert.assertNotNull(fetched);
    Assert.assertEquals(0, getHandler(tableStore).getOpenQueryCount());
    Assert.assertFalse(objectStore.isActiveTransaction());
  }

  private void createPartitionedTable() throws Exception {
    objectStore.createDatabase(new DatabaseBuilder()
        .setName(DB)
        .setDescription("description")
        .setLocation("locationurl")
        .build(conf));

    Table table = new TableBuilder()
        .setDbName(DB)
        .setTableName(TABLE)
        .addCol("test_col1", "int")
        .addPartCol("test_part_col", "int")
        .build(conf);
    objectStore.createTable(table);

    for (int i = 0; i < 3; i++) {
      Partition part = new PartitionBuilder()
          .inTable(table)
          .addValue("a" + i)
          .build(conf);
      objectStore.addPartition(part);
    }
  }

  private static TransactionHandler<?> getHandler(Object storeProxy) {
    return (TransactionHandler<?>) Proxy.getInvocationHandler(storeProxy);
  }

  private AutoCloseable deadline() throws Exception {
    Deadline.registerIfNot(100_000);
    Deadline.startTimer("TestObjectStoreUnwrap");
    return Deadline::stopTimer;
  }
}
