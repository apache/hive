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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
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
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * TestMetaStoreEventListener. Test case for
 * {@link org.apache.hadoop.hive.metastore.MetaStoreEventListener} and
 * {@link org.apache.hadoop.hive.metastore.MetaStorePreEventListener}
 */
public class TestMetaStoreEventListener extends TestCase {
  private static final String msPort = "20001";
  private HiveConf hiveConf;
  private HiveMetaStoreClient msc;
  private Driver driver;

  private static class RunMS implements Runnable {

    @Override
    public void run() {
      try {
        HiveMetaStore.main(new String[]{msPort});
      } catch (Throwable e) {
        e.printStackTrace(System.err);
        assert false;
      }
    }
  }

  @Override
  protected void setUp() throws Exception {

    super.setUp();
    System.setProperty(ConfVars.METASTORE_EVENT_LISTENERS.varname,
        DummyListener.class.getName());
    System.setProperty(ConfVars.METASTORE_PRE_EVENT_LISTENERS.varname,
        DummyPreListener.class.getName());
    Thread t = new Thread(new RunMS());
    t.start();
    Thread.sleep(40000);
    hiveConf = new HiveConf(this.getClass());
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + msPort);
    hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTRETRIES, 3);
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    SessionState.start(new CliSessionState(hiveConf));
    msc = new HiveMetaStoreClient(hiveConf, null);
    driver = new Driver(hiveConf);
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

  private void validateDropTable(Table expectedTable, Table actualTable) {
    validateTable(expectedTable, actualTable);
  }

  private void validateDropDb(Database expectedDb, Database actualDb) {
    assertEquals(expectedDb, actualDb);
  }

  public void testListener() throws Exception {
    String dbName = "tmpdb";
    String tblName = "tmptbl";
    String renamed = "tmptbl2";
    int listSize = 0;

    List<ListenerEvent> notifyList = DummyListener.notifyList;
    assertEquals(notifyList.size(), listSize);
    List<PreEventContext> preNotifyList = DummyPreListener.notifyList;
    assertEquals(preNotifyList.size(), listSize);

    driver.run("create database " + dbName);
    listSize++;
    Database db = msc.getDatabase(dbName);
    assertEquals(listSize, notifyList.size());
    assertEquals(listSize, preNotifyList.size());

    CreateDatabaseEvent dbEvent = (CreateDatabaseEvent)(notifyList.get(listSize - 1));
    assert dbEvent.getStatus();
    validateCreateDb(db, dbEvent.getDatabase());

    PreCreateDatabaseEvent preDbEvent = (PreCreateDatabaseEvent)(preNotifyList.get(listSize - 1));
    validateCreateDb(db, preDbEvent.getDatabase());

    driver.run("use " + dbName);
    driver.run(String.format("create table %s (a string) partitioned by (b string)", tblName));
    listSize++;
    Table tbl = msc.getTable(dbName, tblName);
    assertEquals(notifyList.size(), listSize);
    assertEquals(preNotifyList.size(), listSize);

    CreateTableEvent tblEvent = (CreateTableEvent)(notifyList.get(listSize - 1));
    assert tblEvent.getStatus();
    validateCreateTable(tbl, tblEvent.getTable());

    PreCreateTableEvent preTblEvent = (PreCreateTableEvent)(preNotifyList.get(listSize - 1));
    validateCreateTable(tbl, preTblEvent.getTable());

    driver.run("alter table tmptbl add partition (b='2011')");
    listSize++;
    Partition part = msc.getPartition("tmpdb", "tmptbl", "b=2011");
    assertEquals(notifyList.size(), listSize);
    assertEquals(preNotifyList.size(), listSize);

    AddPartitionEvent partEvent = (AddPartitionEvent)(notifyList.get(listSize-1));
    assert partEvent.getStatus();
    validateAddPartition(part, partEvent.getPartition());

    PreAddPartitionEvent prePartEvent = (PreAddPartitionEvent)(preNotifyList.get(listSize-1));
    validateAddPartition(part, prePartEvent.getPartition());

    driver.run(String.format("alter table %s touch partition (%s)", tblName, "b='2011'"));
    listSize++;
    assertEquals(notifyList.size(), listSize);
    assertEquals(preNotifyList.size(), listSize);

    //the partition did not change,
    // so the new partition should be similar to the original partition
    Partition origP = msc.getPartition(dbName, tblName, "b=2011");

    AlterPartitionEvent alterPartEvent = (AlterPartitionEvent)notifyList.get(listSize - 1);
    assert alterPartEvent.getStatus();
    validateAlterPartition(origP, origP, alterPartEvent.getOldPartition().getDbName(),
        alterPartEvent.getOldPartition().getTableName(),
        alterPartEvent.getOldPartition().getValues(), alterPartEvent.getNewPartition());

    PreAlterPartitionEvent preAlterPartEvent =
        (PreAlterPartitionEvent)preNotifyList.get(listSize - 1);
    validateAlterPartition(origP, origP, preAlterPartEvent.getDbName(),
        preAlterPartEvent.getTableName(), preAlterPartEvent.getNewPartition().getValues(),
        preAlterPartEvent.getNewPartition());

    driver.run(String.format("alter table %s rename to %s", tblName, renamed));
    listSize++;
    assertEquals(notifyList.size(), listSize);
    assertEquals(preNotifyList.size(), listSize);

    Table renamedTable = msc.getTable(dbName, renamed);

    AlterTableEvent alterTableE = (AlterTableEvent) notifyList.get(listSize-1);
    assert alterTableE.getStatus();
    validateAlterTable(tbl, renamedTable, alterTableE.getOldTable(), alterTableE.getNewTable());

    PreAlterTableEvent preAlterTableE = (PreAlterTableEvent) preNotifyList.get(listSize-1);
    validateAlterTable(tbl, renamedTable, preAlterTableE.getOldTable(),
        preAlterTableE.getNewTable());

    //change the table name back
    driver.run(String.format("alter table %s rename to %s", renamed, tblName));
    listSize++;
    assertEquals(notifyList.size(), listSize);
    assertEquals(preNotifyList.size(), listSize);

    driver.run(String.format("alter table %s ADD COLUMNS (c int)", tblName));
    listSize++;
    assertEquals(notifyList.size(), listSize);
    assertEquals(preNotifyList.size(), listSize);

    Table altTable = msc.getTable(dbName, tblName);

    alterTableE = (AlterTableEvent) notifyList.get(listSize-1);
    assert alterTableE.getStatus();
    validateAlterTableColumns(tbl, altTable, alterTableE.getOldTable(), alterTableE.getNewTable());

    preAlterTableE = (PreAlterTableEvent) preNotifyList.get(listSize-1);
    validateAlterTableColumns(tbl, altTable, preAlterTableE.getOldTable(),
        preAlterTableE.getNewTable());

    Map<String,String> kvs = new HashMap<String, String>(1);
    kvs.put("b", "2011");
    msc.markPartitionForEvent("tmpdb", "tmptbl", kvs, PartitionEventType.LOAD_DONE);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    assertEquals(preNotifyList.size(), listSize);

    LoadPartitionDoneEvent partMarkEvent = (LoadPartitionDoneEvent)notifyList.get(listSize - 1);
    assert partMarkEvent.getStatus();
    validateLoadPartitionDone("tmptbl", kvs, partMarkEvent.getTable().getTableName(),
        partMarkEvent.getPartitionName());

    PreLoadPartitionDoneEvent prePartMarkEvent =
        (PreLoadPartitionDoneEvent)preNotifyList.get(listSize - 1);
    validateLoadPartitionDone("tmptbl", kvs, prePartMarkEvent.getTableName(),
        prePartMarkEvent.getPartitionName());

    driver.run(String.format("alter table %s drop partition (b='2011')", tblName));
    listSize++;
    assertEquals(notifyList.size(), listSize);
    assertEquals(preNotifyList.size(), listSize);

    DropPartitionEvent dropPart = (DropPartitionEvent)notifyList.get(listSize - 1);
    assert dropPart.getStatus();
    validateDropPartition(part, dropPart.getPartition());

    PreDropPartitionEvent preDropPart = (PreDropPartitionEvent)preNotifyList.get(listSize - 1);
    validateDropPartition(part, preDropPart.getPartition());

    driver.run("drop table " + tblName);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    assertEquals(preNotifyList.size(), listSize);

    DropTableEvent dropTbl = (DropTableEvent)notifyList.get(listSize-1);
    assert dropTbl.getStatus();
    validateDropTable(tbl, dropTbl.getTable());

    PreDropTableEvent preDropTbl = (PreDropTableEvent)preNotifyList.get(listSize-1);
    validateDropTable(tbl, preDropTbl.getTable());

    driver.run("drop database " + dbName);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    assertEquals(preNotifyList.size(), listSize);

    DropDatabaseEvent dropDB = (DropDatabaseEvent)notifyList.get(listSize-1);
    assert dropDB.getStatus();
    validateDropDb(db, dropDB.getDatabase());

    PreDropDatabaseEvent preDropDB = (PreDropDatabaseEvent)preNotifyList.get(listSize-1);
    assert dropDB.getStatus();
    validateDropDb(db, preDropDB.getDatabase());
  }
}
