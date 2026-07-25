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
import org.apache.hadoop.hive.metastore.ExecutionContextTestUtils;
import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.columnstats.ColStatsBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.model.MNotificationLog;
import org.apache.hadoop.hive.metastore.model.MPartition;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnStatistics;
import org.apache.hadoop.hive.metastore.model.MStorageDescriptor;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.utils.DirectSqlConfigurator;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;

/**
 * Verifies batched metastore operations evict loaded JDO entities from the persistence context.
 * Without eviction, long-lived RawStore instances (for example the DB notification cleaner thread)
 * accumulate deleted entities in the L1 cache and can OOM.
 */
@Category(MetastoreUnitTest.class)
public class TestPersistenceContextEviction {
  private static final int BATCH_SIZE = 3;
  private static final int NUM_EVENTS = 12;
  private static final int NUM_PARTITIONS = 12;
  private static final String DB = "ec_evict_db";
  private static final String TABLE = "ec_evict_tbl";
  private static final String ENGINE = "hive";

  private ObjectStore objectStore;
  private Configuration conf;
  private PersistenceManager pm;

  @Before
  public void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.EVENT_CLEAN_MAX_EVENTS, BATCH_SIZE);
    MetastoreConf.setLongVar(conf, ConfVars.RAWSTORE_PARTITION_BATCH_SIZE, BATCH_SIZE);
    MetaStoreTestUtils.setConfForStandloneMode(conf);

    String currentUrl = MetastoreConf.getVar(conf, ConfVars.CONNECT_URL_KEY);
    currentUrl = currentUrl.replace(MetaStoreServerUtils.JUNIT_DATABASE_PREFIX,
        String.format("%s_%s", MetaStoreServerUtils.JUNIT_DATABASE_PREFIX, UUID.randomUUID()));
    MetastoreConf.setVar(conf, ConfVars.CONNECT_URL_KEY, currentUrl);

    objectStore = new ObjectStore();
    objectStore.setConf(conf);
    HMSHandler.createDefaultCatalog(objectStore, new Warehouse(conf));
    pm = objectStore.createRawStoreBundle().getPersistentManager();
  }

  @Test
  public void testExecutionContextCountsLoadedNotificationEvents() throws MetaException {
    insertNotificationEvents(5, "payload");

    objectStore.openTransaction();
    try {
      Query query = pm.newQuery(MNotificationLog.class);
      List<MNotificationLog> events = (List<MNotificationLog>) query.execute();
      pm.retrieveAll(events);
      Assert.assertTrue("expected loaded events to remain in the persistence context",
          ExecutionContextTestUtils.countCachedInstances(pm, MNotificationLog.class) >= 5);
    } finally {
      objectStore.rollbackTransaction();
    }

    Assert.assertEquals(0, ExecutionContextTestUtils.countCachedInstances(pm, MNotificationLog.class));
  }

  @Test
  public void testCleanNotificationEventsEvictsCachedEntities() throws MetaException {
    insertNotificationEvents(NUM_EVENTS, "x".repeat(50));

    objectStore.openTransaction();
    try {
      objectStore.cleanNotificationEvents(0);
      Assert.assertEquals("batched notification cleanup retains deleted events in the L1 cache", NUM_EVENTS,
          ExecutionContextTestUtils.countCachedInstances(pm, MNotificationLog.class));
    } finally {
      objectStore.commitTransaction();
    }

    Assert.assertEquals("batched notification cleanup must not retain deleted events in the L1 cache", 0,
        ExecutionContextTestUtils.countCachedInstances(pm, MNotificationLog.class));
    NotificationEventResponse response = objectStore.getNextNotification(new NotificationEventRequest());
    Assert.assertEquals(0, response.getEventsSize());
  }

  @Test
  public void testDeletePartitionColumnStatisticsEvictsCachedStats() throws Exception {
    List<String> partNames;
    try (DirectSqlConfigurator ignored = new DirectSqlConfigurator(conf, false)) {
      partNames = createTableWithPartitionStats(NUM_PARTITIONS);
      objectStore.openTransaction();
      try {
        objectStore.deletePartitionColumnStatistics(DEFAULT_CATALOG_NAME, DB, TABLE, partNames, null, ENGINE);
        Assert.assertEquals("batched partition column stats delete must not retain stats in the L1 cache", 0,
            ExecutionContextTestUtils.countCachedInstances(pm, MPartitionColumnStatistics.class));
        Assert.assertEquals("batched partition update must not retain stats in the L1 cache", 0,
            ExecutionContextTestUtils.countCachedInstances(pm, MPartition.class));
      } finally {
        objectStore.commitTransaction();
      }
    }
  }

  @Test
  public void testDropPartitionsEvictsCachedEntities() throws Exception {
    List<String> partNames;
    try (DirectSqlConfigurator ignored = new DirectSqlConfigurator(conf, false)) {
      partNames = createPartitionedTable(NUM_PARTITIONS);
      objectStore.openTransaction();
      try {
        objectStore.dropPartitions(DEFAULT_CATALOG_NAME, DB, TABLE, partNames);
        Assert.assertEquals("batched partition drop must not retain partitions in the L1 cache", 0,
            ExecutionContextTestUtils.countCachedInstances(pm, MPartition.class));
        Assert.assertEquals("batched partition drop must not retain storage descriptors in the L1 cache", 0,
            ExecutionContextTestUtils.countCachedInstances(pm, MStorageDescriptor.class));
      } finally {
        objectStore.commitTransaction();
      }
    }
  }

  private void insertNotificationEvents(int count, String message) throws MetaException {
    for (int i = 0; i < count; i++) {
      NotificationEvent event = new NotificationEvent(0, 0,
          EventMessage.EventType.CREATE_DATABASE.toString(), message);
      objectStore.addNotificationEvent(event);
    }
  }

  private List<String> createPartitionedTable(int partitionCount) throws Exception {
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

    List<String> partNames = new ArrayList<>(partitionCount);
    for (int i = 0; i < partitionCount; i++) {
      Partition partition = new PartitionBuilder()
          .inTable(table)
          .addValue("a" + i)
          .build(conf);
      objectStore.addPartition(partition);
      partNames.add(Warehouse.makePartName(table.getPartitionKeys(), partition.getValues()));
    }
    return partNames;
  }

  private List<String> createTableWithPartitionStats(int partitionCount) throws Exception {
    List<String> partNames = createPartitionedTable(partitionCount);
    Table table = objectStore.getTable(DEFAULT_CATALOG_NAME, DB, TABLE);
    MTable mTable = objectStore.ensureGetMTable(DEFAULT_CATALOG_NAME, DB, TABLE);
    for (int i = 0; i < partitionCount; i++) {
      ColumnStatistics stats = new ColumnStatistics();
      ColumnStatisticsDesc desc = new ColumnStatisticsDesc();
      desc.setCatName(DEFAULT_CATALOG_NAME);
      desc.setDbName(DB);
      desc.setTableName(TABLE);
      desc.setPartName(partNames.get(i));
      stats.setStatsDesc(desc);
      stats.setEngine(ENGINE);

      ColumnStatisticsData data = new ColStatsBuilder<>(long.class)
          .numNulls(1).numDVs(2).low(3L).high(4L).build();
      stats.setStatsObj(List.of(new ColumnStatisticsObj("test_part_col", "int", data)));

      objectStore.updatePartitionColumnStatistics(table, mTable, stats,
          Warehouse.getPartValuesFromPartName(partNames.get(i)), null, -1);
    }
    return partNames;
  }
}
