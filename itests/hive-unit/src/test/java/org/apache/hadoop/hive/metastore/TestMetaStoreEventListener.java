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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddIndexEvent;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterIndexEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.ConfigChangeEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropIndexEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.metastore.events.LoadPartitionDoneEvent;
import org.apache.hadoop.hive.metastore.events.PreAddIndexEvent;
import org.apache.hadoop.hive.metastore.events.PreAddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterIndexEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterTableEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateTableEvent;
import org.apache.hadoop.hive.metastore.events.PreDropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreDropIndexEvent;
import org.apache.hadoop.hive.metastore.events.PreDropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreDropTableEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.metastore.events.PreLoadPartitionDoneEvent;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.SetProcessor;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.ShimLoader;

/**
 * TestMetaStoreEventListener. Test case for
 * {@link org.apache.hadoop.hive.metastore.MetaStoreEventListener} and
 * {@link org.apache.hadoop.hive.metastore.MetaStorePreEventListener}
 */
public class TestMetaStoreEventListener extends TestCase {
  private HiveConf hiveConf;
  private HiveMetaStoreClient msc;
  private Driver driver;

  private static final String dbName = "hive2038";
  private static final String tblName = "tmptbl";
  private static final String renamed = "tmptbl2";

  @Override
  protected void setUp() throws Exception {

    super.setUp();

    System.setProperty("hive.metastore.event.listeners",
        DummyListener.class.getName());
    System.setProperty("hive.metastore.pre.event.listeners",
        DummyPreListener.class.getName());

    int port = MetaStoreUtils.findFreePort();
    MetaStoreUtils.startMetaStore(port, ShimLoader.getHadoopThriftAuthBridge());

    hiveConf = new HiveConf(this.getClass());
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + port);
    hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    SessionState.start(new CliSessionState(hiveConf));
    msc = new HiveMetaStoreClient(hiveConf, null);
    driver = new Driver(hiveConf);

    driver.run("drop database if exists " + dbName + " cascade");

    DummyListener.notifyList.clear();
    DummyPreListener.notifyList.clear();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
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

  private void validateDropPartition(Partition expectedPartition, Partition actualPartition) {
    validatePartition(expectedPartition, actualPartition);
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

  private void validateIndex(Index expectedIndex, Index actualIndex) {
    assertEquals(expectedIndex.getDbName(), actualIndex.getDbName());
    assertEquals(expectedIndex.getIndexName(), actualIndex.getIndexName());
    assertEquals(expectedIndex.getIndexHandlerClass(), actualIndex.getIndexHandlerClass());
    assertEquals(expectedIndex.getOrigTableName(), actualIndex.getOrigTableName());
    assertEquals(expectedIndex.getIndexTableName(), actualIndex.getIndexTableName());
    assertEquals(expectedIndex.getSd().getLocation(), actualIndex.getSd().getLocation());
  }

  private void validateAddIndex(Index expectedIndex, Index actualIndex) {
    validateIndex(expectedIndex, actualIndex);
  }

  private void validateAlterIndex(Index expectedOldIndex, Index actualOldIndex,
      Index expectedNewIndex, Index actualNewIndex) {
    validateIndex(expectedOldIndex, actualOldIndex);
    validateIndex(expectedNewIndex, actualNewIndex);
  }

  private void validateDropIndex(Index expectedIndex, Index actualIndex) {
    validateIndex(expectedIndex, actualIndex);
  }

  public void testListener() throws Exception {
    int listSize = 0;

    List<ListenerEvent> notifyList = DummyListener.notifyList;
    List<PreEventContext> preNotifyList = DummyPreListener.notifyList;
    assertEquals(notifyList.size(), listSize);
    assertEquals(preNotifyList.size(), listSize);

    driver.run("create database " + dbName);
    listSize++;
    PreCreateDatabaseEvent preDbEvent = (PreCreateDatabaseEvent)(preNotifyList.get(preNotifyList.size() - 1));
    Database db = msc.getDatabase(dbName);
    assertEquals(listSize, notifyList.size());
    assertEquals(listSize + 1, preNotifyList.size());
    validateCreateDb(db, preDbEvent.getDatabase());

    CreateDatabaseEvent dbEvent = (CreateDatabaseEvent)(notifyList.get(listSize - 1));
    assert dbEvent.getStatus();
    validateCreateDb(db, dbEvent.getDatabase());


    driver.run("use " + dbName);
    driver.run(String.format("create table %s (a string) partitioned by (b string)", tblName));
    PreCreateTableEvent preTblEvent = (PreCreateTableEvent)(preNotifyList.get(preNotifyList.size() - 1));
    listSize++;
    Table tbl = msc.getTable(dbName, tblName);
    validateCreateTable(tbl, preTblEvent.getTable());
    assertEquals(notifyList.size(), listSize);

    CreateTableEvent tblEvent = (CreateTableEvent)(notifyList.get(listSize - 1));
    assert tblEvent.getStatus();
    validateCreateTable(tbl, tblEvent.getTable());

    driver.run("create index tmptbl_i on table tmptbl(a) as 'compact' " +
        "WITH DEFERRED REBUILD IDXPROPERTIES ('prop1'='val1', 'prop2'='val2')");
    listSize += 2;  // creates index table internally
    assertEquals(notifyList.size(), listSize);

    AddIndexEvent addIndexEvent = (AddIndexEvent)notifyList.get(listSize - 1);
    assert addIndexEvent.getStatus();
    PreAddIndexEvent preAddIndexEvent = (PreAddIndexEvent)(preNotifyList.get(preNotifyList.size() - 3));

    Index oldIndex = msc.getIndex(dbName, "tmptbl", "tmptbl_i");

    validateAddIndex(oldIndex, addIndexEvent.getIndex());

    validateAddIndex(oldIndex, preAddIndexEvent.getIndex());

    driver.run("alter index tmptbl_i on tmptbl set IDXPROPERTIES " +
        "('prop1'='val1_new', 'prop3'='val3')");
    listSize++;
    assertEquals(notifyList.size(), listSize);

    Index newIndex = msc.getIndex(dbName, "tmptbl", "tmptbl_i");

    AlterIndexEvent alterIndexEvent = (AlterIndexEvent) notifyList.get(listSize - 1);
    assert alterIndexEvent.getStatus();
    validateAlterIndex(oldIndex, alterIndexEvent.getOldIndex(),
        newIndex, alterIndexEvent.getNewIndex());

    PreAlterIndexEvent preAlterIndexEvent = (PreAlterIndexEvent) (preNotifyList.get(preNotifyList.size() - 1));
    validateAlterIndex(oldIndex, preAlterIndexEvent.getOldIndex(),
        newIndex, preAlterIndexEvent.getNewIndex());

    driver.run("drop index tmptbl_i on tmptbl");
    listSize++;
    assertEquals(notifyList.size(), listSize);

    DropIndexEvent dropIndexEvent = (DropIndexEvent) notifyList.get(listSize - 1);
    assert dropIndexEvent.getStatus();
    validateDropIndex(newIndex, dropIndexEvent.getIndex());

    PreDropIndexEvent preDropIndexEvent = (PreDropIndexEvent) (preNotifyList.get(preNotifyList.size() - 1));
    validateDropIndex(newIndex, preDropIndexEvent.getIndex());

    driver.run("alter table tmptbl add partition (b='2011')");
    listSize++;
    assertEquals(notifyList.size(), listSize);
    PreAddPartitionEvent prePartEvent = (PreAddPartitionEvent)(preNotifyList.get(preNotifyList.size() - 1));

    AddPartitionEvent partEvent = (AddPartitionEvent)(notifyList.get(listSize-1));
    assert partEvent.getStatus();
    Partition part = msc.getPartition("hive2038", "tmptbl", "b=2011");
    validateAddPartition(part, partEvent.getPartitions().get(0));
    validateTableInAddPartition(tbl, partEvent.getTable());
    validateAddPartition(part, prePartEvent.getPartitions().get(0));

    // Test adding multiple partitions in a single partition-set, atomically.
    int currentTime = (int)System.currentTimeMillis();
    HiveMetaStoreClient hmsClient = new HiveMetaStoreClient(hiveConf);
    Table table = hmsClient.getTable(dbName, "tmptbl");
    Partition partition1 = new Partition(Arrays.asList("20110101"), dbName, "tmptbl", currentTime,
                                        currentTime, table.getSd(), table.getParameters());
    Partition partition2 = new Partition(Arrays.asList("20110102"), dbName, "tmptbl", currentTime,
                                        currentTime, table.getSd(), table.getParameters());
    Partition partition3 = new Partition(Arrays.asList("20110103"), dbName, "tmptbl", currentTime,
                                        currentTime, table.getSd(), table.getParameters());
    hmsClient.add_partitions(Arrays.asList(partition1, partition2, partition3));
    ++listSize;
    AddPartitionEvent multiplePartitionEvent = (AddPartitionEvent)(notifyList.get(listSize-1));
    assertEquals("Unexpected number of partitions in event!", 3, multiplePartitionEvent.getPartitions().size());
    assertEquals("Unexpected table value.", table, multiplePartitionEvent.getTable());
    assertEquals("Unexpected partition value.", partition1.getValues(), multiplePartitionEvent.getPartitions().get(0).getValues());
    assertEquals("Unexpected partition value.", partition2.getValues(), multiplePartitionEvent.getPartitions().get(1).getValues());
    assertEquals("Unexpected partition value.", partition3.getValues(), multiplePartitionEvent.getPartitions().get(2).getValues());

    driver.run(String.format("alter table %s touch partition (%s)", tblName, "b='2011'"));
    listSize++;
    assertEquals(notifyList.size(), listSize);
    PreAlterPartitionEvent preAlterPartEvent =
        (PreAlterPartitionEvent)preNotifyList.get(preNotifyList.size() - 1);

    //the partition did not change,
    // so the new partition should be similar to the original partition
    Partition origP = msc.getPartition(dbName, tblName, "b=2011");

    AlterPartitionEvent alterPartEvent = (AlterPartitionEvent)notifyList.get(listSize - 1);
    assert alterPartEvent.getStatus();
    validateAlterPartition(origP, origP, alterPartEvent.getOldPartition().getDbName(),
        alterPartEvent.getOldPartition().getTableName(),
        alterPartEvent.getOldPartition().getValues(), alterPartEvent.getNewPartition());


    validateAlterPartition(origP, origP, preAlterPartEvent.getDbName(),
        preAlterPartEvent.getTableName(), preAlterPartEvent.getNewPartition().getValues(),
        preAlterPartEvent.getNewPartition());

    List<String> part_vals = new ArrayList<String>();
    part_vals.add("c=2012");
    int preEventListSize;
    preEventListSize = preNotifyList.size() + 1;
    Partition newPart = msc.appendPartition(dbName, tblName, part_vals);

    listSize++;
    assertEquals(notifyList.size(), listSize);
    assertEquals(preNotifyList.size(), preEventListSize);

    AddPartitionEvent appendPartEvent =
        (AddPartitionEvent)(notifyList.get(listSize-1));
    validateAddPartition(newPart, appendPartEvent.getPartitions().get(0));

    PreAddPartitionEvent preAppendPartEvent =
        (PreAddPartitionEvent)(preNotifyList.get(preNotifyList.size() - 1));
    validateAddPartition(newPart, preAppendPartEvent.getPartitions().get(0));

    driver.run(String.format("alter table %s rename to %s", tblName, renamed));
    listSize++;
    assertEquals(notifyList.size(), listSize);
    PreAlterTableEvent preAlterTableE = (PreAlterTableEvent) preNotifyList.get(preNotifyList.size() - 1);

    Table renamedTable = msc.getTable(dbName, renamed);

    AlterTableEvent alterTableE = (AlterTableEvent) notifyList.get(listSize-1);
    assert alterTableE.getStatus();
    validateAlterTable(tbl, renamedTable, alterTableE.getOldTable(), alterTableE.getNewTable());
    validateAlterTable(tbl, renamedTable, preAlterTableE.getOldTable(),
        preAlterTableE.getNewTable());

    //change the table name back
    driver.run(String.format("alter table %s rename to %s", renamed, tblName));
    listSize++;
    assertEquals(notifyList.size(), listSize);

    driver.run(String.format("alter table %s ADD COLUMNS (c int)", tblName));
    listSize++;
    assertEquals(notifyList.size(), listSize);
    preAlterTableE = (PreAlterTableEvent) preNotifyList.get(preNotifyList.size() - 1);

    Table altTable = msc.getTable(dbName, tblName);

    alterTableE = (AlterTableEvent) notifyList.get(listSize-1);
    assert alterTableE.getStatus();
    validateAlterTableColumns(tbl, altTable, alterTableE.getOldTable(), alterTableE.getNewTable());
    validateAlterTableColumns(tbl, altTable, preAlterTableE.getOldTable(),
        preAlterTableE.getNewTable());

    Map<String,String> kvs = new HashMap<String, String>(1);
    kvs.put("b", "2011");
    msc.markPartitionForEvent("hive2038", "tmptbl", kvs, PartitionEventType.LOAD_DONE);
    listSize++;
    assertEquals(notifyList.size(), listSize);

    LoadPartitionDoneEvent partMarkEvent = (LoadPartitionDoneEvent)notifyList.get(listSize - 1);
    assert partMarkEvent.getStatus();
    validateLoadPartitionDone("tmptbl", kvs, partMarkEvent.getTable().getTableName(),
        partMarkEvent.getPartitionName());

    PreLoadPartitionDoneEvent prePartMarkEvent =
        (PreLoadPartitionDoneEvent)preNotifyList.get(preNotifyList.size() - 1);
    validateLoadPartitionDone("tmptbl", kvs, prePartMarkEvent.getTableName(),
        prePartMarkEvent.getPartitionName());

    driver.run(String.format("alter table %s drop partition (b='2011')", tblName));
    listSize++;
    assertEquals(notifyList.size(), listSize);
    PreDropPartitionEvent preDropPart = (PreDropPartitionEvent) preNotifyList.get(preNotifyList
        .size() - 1);

    DropPartitionEvent dropPart = (DropPartitionEvent)notifyList.get(listSize - 1);
    assert dropPart.getStatus();
    validateDropPartition(part, dropPart.getPartition());
    validateTableInDropPartition(tbl, dropPart.getTable());

    validateDropPartition(part, preDropPart.getPartition());
    validateTableInDropPartition(tbl, preDropPart.getTable());

    driver.run("drop table " + tblName);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    PreDropTableEvent preDropTbl = (PreDropTableEvent)preNotifyList.get(preNotifyList.size() - 1);

    DropTableEvent dropTbl = (DropTableEvent)notifyList.get(listSize-1);
    assert dropTbl.getStatus();
    validateDropTable(tbl, dropTbl.getTable());
    validateDropTable(tbl, preDropTbl.getTable());

    driver.run("drop database " + dbName);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    PreDropDatabaseEvent preDropDB = (PreDropDatabaseEvent)preNotifyList.get(preNotifyList.size() - 1);

    DropDatabaseEvent dropDB = (DropDatabaseEvent)notifyList.get(listSize-1);
    assert dropDB.getStatus();
    validateDropDb(db, dropDB.getDatabase());
    validateDropDb(db, preDropDB.getDatabase());

    SetProcessor.setVariable("metaconf:hive.metastore.try.direct.sql", "false");
    ConfigChangeEvent event = (ConfigChangeEvent) notifyList.get(notifyList.size() - 1);
    assertEquals("hive.metastore.try.direct.sql", event.getKey());
    assertEquals("true", event.getOldValue());
    assertEquals("false", event.getNewValue());
  }

}
