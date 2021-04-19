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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.ConfigChangeEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.metastore.events.LoadPartitionDoneEvent;
import org.apache.hadoop.hive.metastore.events.PreAddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterTableEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateTableEvent;
import org.apache.hadoop.hive.metastore.events.PreDropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreDropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreDropTableEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.metastore.events.PreLoadPartitionDoneEvent;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;

import org.junit.experimental.categories.Category;

/**
 * TestMetaStoreEventListener. Test case for
 * {@link org.apache.hadoop.hive.metastore.MetaStoreEventListener} and
 * {@link org.apache.hadoop.hive.metastore.MetaStorePreEventListener}
 */
@Category(MetastoreUnitTest.class)
public class TestMetaStoreEventListener {
  private Configuration conf;
  private HiveMetaStoreClient msc;

  private static final String dbName = "hive2038";
  private static final String tblName = "tmptbl";
  private static final String renamed = "tmptbl2";
  private static final String metaConfKey = "metastore.partition.name.whitelist.pattern";
  private static final String metaConfVal = "";

  @Before
  public void setUp() throws Exception {
    System.setProperty("hive.metastore.event.listeners",
        DummyListener.class.getName());
    System.setProperty("hive.metastore.pre.event.listeners",
        DummyPreListener.class.getName());

    conf = MetastoreConf.newMetastoreConf();

    MetastoreConf.setVar(conf, ConfVars.PARTITION_NAME_WHITELIST_PATTERN, metaConfVal);
    MetastoreConf.setLongVar(conf, ConfVars.THRIFT_CONNECTION_RETRIES, 3);
    MetastoreConf.setBoolVar(conf, ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf);

    HiveMetaStoreClient.setProcessorIdentifier("test@TestMetaStoreEventListener");
    HiveMetaStoreClient.setProcessorCapabilities(new String[] {
        "HIVEFULLACIDREAD",
        "EXTWRITE",
        "EXTREAD",
        "HIVEBUCKET2"
    });

    msc = new HiveMetaStoreClient(conf);

    msc.dropDatabase(dbName, true, true, true);
    DummyListener.notifyList.clear();
    DummyPreListener.notifyList.clear();
  }

  private void validateCreateDb(Database expectedDb, Database actualDb) {
    assertEquals(expectedDb.getName(), actualDb.getName());
    assertEquals(expectedDb.getLocationUri(), actualDb.getLocationUri());
  }

  private void validateTable(Table expectedTable, Table actualTable) {
    assertEquals(expectedTable.getTableName(), actualTable.getTableName());
    assertEquals(expectedTable.getDbName(), actualTable.getDbName());
    assertEquals(expectedTable.getSd().getLocation(), actualTable.getSd().getLocation());
  }

  private void validateCreateTable(Table expectedTable, Table actualTable) {
    validateTable(expectedTable, actualTable);
  }

  private void validateAddPartition(Partition expectedPartition, Partition actualPartition) {
    assertEquals(expectedPartition, actualPartition);
  }

  private void validateTableInAddPartition(Table expectedTable, Table actualTable) {
    // AccessType is not set on the table object from the event. We want to compare everything else.
    actualTable.setAccessType(expectedTable.getAccessType());
    assertEquals(expectedTable, actualTable);
  }

  private void validatePartition(Partition expectedPartition, Partition actualPartition) {
    assertEquals(expectedPartition.getValues(), actualPartition.getValues());
    assertEquals(expectedPartition.getDbName(), actualPartition.getDbName());
    assertEquals(expectedPartition.getTableName(), actualPartition.getTableName());
  }

  private void validateAlterPartition(Partition expectedOldPartition,
      Partition expectedNewPartition, String actualOldPartitionDbName,
      String actualOldPartitionTblName,List<String> actualOldPartitionValues,
      Partition actualNewPartition) {
    assertEquals(expectedOldPartition.getValues(), actualOldPartitionValues);
    assertEquals(expectedOldPartition.getDbName(), actualOldPartitionDbName);
    assertEquals(expectedOldPartition.getTableName(), actualOldPartitionTblName);

    validatePartition(expectedNewPartition, actualNewPartition);
  }

  private void validateAlterTable(Table expectedOldTable, Table expectedNewTable,
      Table actualOldTable, Table actualNewTable) {
    validateTable(expectedOldTable, actualOldTable);
    validateTable(expectedNewTable, actualNewTable);
  }

  private void validateAlterTableColumns(Table expectedOldTable, Table expectedNewTable,
      Table actualOldTable, Table actualNewTable) {
    validateAlterTable(expectedOldTable, expectedNewTable, actualOldTable, actualNewTable);

    assertEquals(expectedOldTable.getSd().getCols(), actualOldTable.getSd().getCols());
    assertEquals(expectedNewTable.getSd().getCols(), actualNewTable.getSd().getCols());
  }

  private void validateLoadPartitionDone(String expectedTableName,
      Map<String,String> expectedPartitionName, String actualTableName,
      Map<String,String> actualPartitionName) {
    assertEquals(expectedPartitionName, actualPartitionName);
    assertEquals(expectedTableName, actualTableName);
  }

  private void validateDropPartition(Iterator<Partition> expectedPartitions, Iterator<Partition> actualPartitions) {
    while (expectedPartitions.hasNext()){
      assertTrue(actualPartitions.hasNext());
      validatePartition(expectedPartitions.next(), actualPartitions.next());
    }
    assertFalse(actualPartitions.hasNext());
  }

  private void validateTableInDropPartition(Table expectedTable, Table actualTable) {
    validateTable(expectedTable, actualTable);
  }

  private void validateDropTable(Table expectedTable, Table actualTable) {
    validateTable(expectedTable, actualTable);
  }

  private void validateDropDb(Database expectedDb, Database actualDb) {
    assertEquals(expectedDb, actualDb);
  }

  @Test
  public void testListener() throws Exception {
    int listSize = 0;

    List<ListenerEvent> notifyList = DummyListener.notifyList;
    List<PreEventContext> preNotifyList = DummyPreListener.notifyList;
    assertEquals(notifyList.size(), listSize);
    assertEquals(preNotifyList.size(), listSize);

    new DatabaseBuilder()
        .setName(dbName)
        .create(msc, conf);
    listSize++;
    PreCreateDatabaseEvent preDbEvent = (PreCreateDatabaseEvent)(preNotifyList.get(preNotifyList.size() - 1));
    Database db = msc.getDatabase(dbName);
    assertEquals(listSize, notifyList.size());
    assertEquals(listSize + 1, preNotifyList.size());
    validateCreateDb(db, preDbEvent.getDatabase());

    CreateDatabaseEvent dbEvent = (CreateDatabaseEvent)(notifyList.get(listSize - 1));
    Assert.assertTrue(dbEvent.getStatus());
    validateCreateDb(db, dbEvent.getDatabase());

    Table table = new TableBuilder()
        .inDb(db)
        .setTableName(tblName)
        .addCol("a", "string")
        .addPartCol("b", "string")
        .create(msc, conf);
    PreCreateTableEvent preTblEvent = (PreCreateTableEvent) (preNotifyList.get(preNotifyList.size() - 1));
    listSize++;
    Table tbl = msc.getTable(dbName, tblName);
    validateCreateTable(tbl, preTblEvent.getTable());
    assertEquals(notifyList.size(), listSize);

    CreateTableEvent tblEvent = (CreateTableEvent)(notifyList.get(listSize - 1));
    Assert.assertTrue(tblEvent.getStatus());
    validateCreateTable(tbl, tblEvent.getTable());


    new PartitionBuilder()
        .inTable(table)
        .addValue("2011")
        .addToTable(msc, conf);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    PreAddPartitionEvent prePartEvent = (PreAddPartitionEvent)(preNotifyList.get(preNotifyList.size() - 1));

    AddPartitionEvent partEvent = (AddPartitionEvent)(notifyList.get(listSize-1));
    Assert.assertTrue(partEvent.getStatus());
    Partition part = msc.getPartition("hive2038", "tmptbl", "b=2011");
    Partition partAdded = partEvent.getPartitionIterator().next();
    partAdded.setWriteId(part.getWriteId());
    validateAddPartition(part, partAdded);
    validateTableInAddPartition(tbl, partEvent.getTable());
    validateAddPartition(part, prePartEvent.getPartitions().get(0));

    // Test adding multiple partitions in a single partition-set, atomically.
    int currentTime = (int)System.currentTimeMillis();
    HiveMetaStoreClient hmsClient = new HiveMetaStoreClient(conf);
    table = hmsClient.getTable(dbName, "tmptbl");
    Partition partition1 = new Partition(Arrays.asList("20110101"), dbName, "tmptbl", currentTime,
                                        currentTime, table.getSd(), table.getParameters());
    Partition partition2 = new Partition(Arrays.asList("20110102"), dbName, "tmptbl", currentTime,
                                        currentTime, table.getSd(), table.getParameters());
    Partition partition3 = new Partition(Arrays.asList("20110103"), dbName, "tmptbl", currentTime,
                                        currentTime, table.getSd(), table.getParameters());
    hmsClient.add_partitions(Arrays.asList(partition1, partition2, partition3));
    ++listSize;
    AddPartitionEvent multiplePartitionEvent = (AddPartitionEvent)(notifyList.get(listSize-1));
    validateTableInAddPartition(table, multiplePartitionEvent.getTable());
    List<Partition> multiParts = Lists.newArrayList(multiplePartitionEvent.getPartitionIterator());
    assertEquals("Unexpected number of partitions in event!", 3, multiParts.size());
    assertEquals("Unexpected partition value.", partition1.getValues(), multiParts.get(0).getValues());
    assertEquals("Unexpected partition value.", partition2.getValues(), multiParts.get(1).getValues());
    assertEquals("Unexpected partition value.", partition3.getValues(), multiParts.get(2).getValues());

    part.setLastAccessTime((int)(System.currentTimeMillis()/1000));
    msc.alter_partition(dbName, tblName, part);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    PreAlterPartitionEvent preAlterPartEvent =
        (PreAlterPartitionEvent)preNotifyList.get(preNotifyList.size() - 1);

    //the partition did not change,
    // so the new partition should be similar to the original partition
    Partition origP = msc.getPartition(dbName, tblName, "b=2011");

    AlterPartitionEvent alterPartEvent = (AlterPartitionEvent)notifyList.get(listSize - 1);
    Assert.assertTrue(alterPartEvent.getStatus());
    validateAlterPartition(origP, origP, alterPartEvent.getOldPartition().getDbName(),
        alterPartEvent.getOldPartition().getTableName(),
        alterPartEvent.getOldPartition().getValues(), alterPartEvent.getNewPartition());


    validateAlterPartition(origP, origP, preAlterPartEvent.getDbName(),
        preAlterPartEvent.getTableName(), preAlterPartEvent.getNewPartition().getValues(),
        preAlterPartEvent.getNewPartition());

    List<String> part_vals = new ArrayList<>();
    part_vals.add("c=2012");
    int preEventListSize;
    preEventListSize = preNotifyList.size() + 1;
    Partition newPart = msc.appendPartition(dbName, tblName, part_vals);

    listSize++;
    assertEquals(notifyList.size(), listSize);
    assertEquals(preNotifyList.size(), preEventListSize);

    AddPartitionEvent appendPartEvent =
        (AddPartitionEvent)(notifyList.get(listSize-1));
    Partition partAppended = appendPartEvent.getPartitionIterator().next();
    validateAddPartition(newPart, partAppended);

    PreAddPartitionEvent preAppendPartEvent =
        (PreAddPartitionEvent)(preNotifyList.get(preNotifyList.size() - 1));
    validateAddPartition(newPart, preAppendPartEvent.getPartitions().get(0));

    Table renamedTable = new Table(table);
    renamedTable.setTableName(renamed);
    msc.alter_table(dbName, tblName, renamedTable);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    PreAlterTableEvent preAlterTableE = (PreAlterTableEvent) preNotifyList.get(preNotifyList.size() - 1);

    renamedTable = msc.getTable(dbName, renamed);

    AlterTableEvent alterTableE = (AlterTableEvent) notifyList.get(listSize-1);
    Assert.assertTrue(alterTableE.getStatus());
    validateAlterTable(tbl, renamedTable, alterTableE.getOldTable(), alterTableE.getNewTable());
    validateAlterTable(tbl, renamedTable, preAlterTableE.getOldTable(),
        preAlterTableE.getNewTable());

    //change the table name back
    table = new Table(renamedTable);
    table.setTableName(tblName);
    msc.alter_table(dbName, renamed, table);
    listSize++;
    assertEquals(notifyList.size(), listSize);

    table = msc.getTable(dbName, tblName);
    table.getSd().addToCols(new FieldSchema("c", "int", ""));
    msc.alter_table(dbName, tblName, table);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    preAlterTableE = (PreAlterTableEvent) preNotifyList.get(preNotifyList.size() - 1);

    Table altTable = msc.getTable(dbName, tblName);

    alterTableE = (AlterTableEvent) notifyList.get(listSize-1);
    Assert.assertTrue(alterTableE.getStatus());
    validateAlterTableColumns(tbl, altTable, alterTableE.getOldTable(), alterTableE.getNewTable());
    validateAlterTableColumns(tbl, altTable, preAlterTableE.getOldTable(),
        preAlterTableE.getNewTable());

    Map<String,String> kvs = new HashMap<>(1);
    kvs.put("b", "2011");
    msc.markPartitionForEvent("hive2038", "tmptbl", kvs, PartitionEventType.LOAD_DONE);
    listSize++;
    assertEquals(notifyList.size(), listSize);

    LoadPartitionDoneEvent partMarkEvent = (LoadPartitionDoneEvent)notifyList.get(listSize - 1);
    Assert.assertTrue(partMarkEvent.getStatus());
    validateLoadPartitionDone("tmptbl", kvs, partMarkEvent.getTable().getTableName(),
        partMarkEvent.getPartitionName());

    PreLoadPartitionDoneEvent prePartMarkEvent =
        (PreLoadPartitionDoneEvent)preNotifyList.get(preNotifyList.size() - 1);
    validateLoadPartitionDone("tmptbl", kvs, prePartMarkEvent.getTableName(),
        prePartMarkEvent.getPartitionName());

    msc.dropPartition(dbName, tblName, Collections.singletonList("2011"));
    listSize++;
    assertEquals(notifyList.size(), listSize);
    PreDropPartitionEvent preDropPart = (PreDropPartitionEvent) preNotifyList.get(preNotifyList
        .size() - 1);

    DropPartitionEvent dropPart = (DropPartitionEvent)notifyList.get(listSize - 1);
    Assert.assertTrue(dropPart.getStatus());
    validateDropPartition(Collections.singletonList(part).iterator(), dropPart.getPartitionIterator());
    validateTableInDropPartition(tbl, dropPart.getTable());

    validateDropPartition(Collections.singletonList(part).iterator(), preDropPart.getPartitionIterator());
    validateTableInDropPartition(tbl, preDropPart.getTable());

    msc.dropTable(dbName, tblName);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    PreDropTableEvent preDropTbl = (PreDropTableEvent)preNotifyList.get(preNotifyList.size() - 1);

    DropTableEvent dropTbl = (DropTableEvent)notifyList.get(listSize-1);
    Assert.assertTrue(dropTbl.getStatus());
    validateDropTable(tbl, dropTbl.getTable());
    validateDropTable(tbl, preDropTbl.getTable());

    msc.dropDatabase(dbName);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    PreDropDatabaseEvent preDropDB = (PreDropDatabaseEvent)preNotifyList.get(preNotifyList.size() - 1);

    DropDatabaseEvent dropDB = (DropDatabaseEvent)notifyList.get(listSize-1);
    Assert.assertTrue(dropDB.getStatus());
    validateDropDb(db, dropDB.getDatabase());
    validateDropDb(db, preDropDB.getDatabase());

    msc.setMetaConf("metastore.try.direct.sql", "false");
    ConfigChangeEvent event = (ConfigChangeEvent) notifyList.get(notifyList.size() - 1);
    assertEquals("metastore.try.direct.sql", event.getKey());
    assertEquals("true", event.getOldValue());
    assertEquals("false", event.getNewValue());
  }

  @Test
  public void testMetaConfNotifyListenersClosingClient() throws Exception {
    HiveMetaStoreClient closingClient = new HiveMetaStoreClient(conf, null);
    closingClient.setMetaConf(metaConfKey, "[test pattern modified]");
    ConfigChangeEvent event = (ConfigChangeEvent) DummyListener.getLastEvent();
    assertEquals(event.getOldValue(), metaConfVal);
    assertEquals(event.getNewValue(), "[test pattern modified]");
    closingClient.close();

    Thread.sleep(2 * 1000);

    event = (ConfigChangeEvent) DummyListener.getLastEvent();
    assertEquals(event.getOldValue(), "[test pattern modified]");
    assertEquals(event.getNewValue(), metaConfVal);
  }

  @Test
  public void testMetaConfNotifyListenersNonClosingClient() throws Exception {
    HiveMetaStoreClient nonClosingClient = new HiveMetaStoreClient(conf, null);
    nonClosingClient.setMetaConf(metaConfKey, "[test pattern modified]");
    ConfigChangeEvent event = (ConfigChangeEvent) DummyListener.getLastEvent();
    assertEquals(event.getOldValue(), metaConfVal);
    assertEquals(event.getNewValue(), "[test pattern modified]");
    // This should also trigger meta listener notification via TServerEventHandler#deleteContext
    nonClosingClient.getTTransport().close();

    Thread.sleep(2 * 1000);

    event = (ConfigChangeEvent) DummyListener.getLastEvent();
    assertEquals(event.getOldValue(), "[test pattern modified]");
    assertEquals(event.getNewValue(), metaConfVal);
  }

  @Test
  public void testMetaConfDuplicateNotification() throws Exception {
    HiveMetaStoreClient closingClient = new HiveMetaStoreClient(conf, null);
    closingClient.setMetaConf(metaConfKey, metaConfVal);
    int beforeCloseNotificationEventCounts = DummyListener.notifyList.size();
    closingClient.close();

    Thread.sleep(2 * 1000);

    int afterCloseNotificationEventCounts = DummyListener.notifyList.size();
    // Setting key to same value, should not trigger configChange event during shutdown
    assertEquals(beforeCloseNotificationEventCounts, afterCloseNotificationEventCounts);
  }

  @Test
  public void testMetaConfSameHandler() throws Exception {
    HiveMetaStoreClient closingClient = new HiveMetaStoreClient(conf, null);
    closingClient.setMetaConf(metaConfKey, "[test pattern modified]");
    ConfigChangeEvent event = (ConfigChangeEvent) DummyListener.getLastEvent();
    int beforeCloseNotificationEventCounts = DummyListener.notifyList.size();
    IHMSHandler beforeHandler = event.getIHMSHandler();
    closingClient.close();

    Thread.sleep(2 * 1000);
    event = (ConfigChangeEvent) DummyListener.getLastEvent();
    int afterCloseNotificationEventCounts = DummyListener.notifyList.size();
    IHMSHandler afterHandler = event.getIHMSHandler();
    // Meta-conf cleanup should trigger an event to listener
    assertNotSame(beforeCloseNotificationEventCounts, afterCloseNotificationEventCounts);
    // Both the handlers should be same
    assertEquals(beforeHandler, afterHandler);
  }
}
