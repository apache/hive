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
import org.apache.hadoop.hive.metastore.api.FieldSchema;
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
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * TestMetaStoreEventListener. Test case for
 * {@link org.apache.hadoop.hive.metastore.hadooorg.apache.hadoop.hive.metastore.MetaStoreEventListener}
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
    Thread t = new Thread(new RunMS());
    t.start();
    Thread.sleep(40000);
    hiveConf = new HiveConf(this.getClass());
    hiveConf.setBoolVar(ConfVars.METASTORE_MODE, false);
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

  public void testListener() throws Exception {
    String dbName = "tmpdb";
    String tblName = "tmptbl";
    String renamed = "tmptbl2";
    int listSize = 0;

    List<ListenerEvent> notifyList = DummyListener.notifyList;
    assertEquals(notifyList.size(), listSize);

    driver.run("create database " + dbName);
    listSize++;
    Database db = msc.getDatabase(dbName);
    assertEquals(listSize, notifyList.size());
    CreateDatabaseEvent dbEvent = (CreateDatabaseEvent)(notifyList.get(listSize - 1));
    assert dbEvent.getStatus();
    assertEquals(db.getName(), dbEvent.getDatabase().getName());
    assertEquals(db.getLocationUri(), dbEvent.getDatabase().getLocationUri());

    driver.run("use " + dbName);
    driver.run(String.format("create table %s (a string) partitioned by (b string)", tblName));
    listSize++;
    Table tbl = msc.getTable(dbName, tblName);
    assertEquals(notifyList.size(), listSize);
    CreateTableEvent tblEvent = (CreateTableEvent)(notifyList.get(listSize - 1));
    assert tblEvent.getStatus();
    assertEquals(tbl.getTableName(), tblEvent.getTable().getTableName());
    assertEquals(tbl.getDbName(), tblEvent.getTable().getDbName());
    assertEquals(tbl.getSd().getLocation(), tblEvent.getTable().getSd().getLocation());

    driver.run("alter table tmptbl add partition (b='2011')");
    listSize++;
    Partition part = msc.getPartition("tmpdb", "tmptbl", "b=2011");
    assertEquals(notifyList.size(), listSize);
    AddPartitionEvent partEvent = (AddPartitionEvent)(notifyList.get(listSize-1));
    assert partEvent.getStatus();
    assertEquals(part, partEvent.getPartition());

    driver.run(String.format("alter table %s touch partition (%s)", tblName, "b='2011'"));
    listSize++;
    assertEquals(notifyList.size(), listSize);
    AlterPartitionEvent alterPartEvent = (AlterPartitionEvent)notifyList.get(listSize - 1);
    assert alterPartEvent.getStatus();
    Partition origP = msc.getPartition(dbName, tblName, "b=2011");
    assertEquals(origP.getValues(), alterPartEvent.getOldPartition().getValues());
    assertEquals(origP.getDbName(), alterPartEvent.getOldPartition().getDbName());
    assertEquals(origP.getTableName(), alterPartEvent.getOldPartition().getTableName());
    //the partition did not change,
    // so the new partition should be similar to the original partition
    assertEquals(origP.getValues(), alterPartEvent.getNewPartition().getValues());
    assertEquals(origP.getDbName(), alterPartEvent.getNewPartition().getDbName());
    assertEquals(origP.getTableName(), alterPartEvent.getNewPartition().getTableName());

    driver.run(String.format("alter table %s rename to %s", tblName, renamed));
    listSize++;
    assertEquals(notifyList.size(), listSize);
    Table renamedTable = msc.getTable(dbName, renamed);
    AlterTableEvent alterTableE = (AlterTableEvent) notifyList.get(listSize-1);
    assert alterTableE.getStatus();
    Table oldTable = alterTableE.getOldTable();
    Table newTable = alterTableE.getNewTable();
    assertEquals(tbl.getDbName(), oldTable.getDbName());
    assertEquals(tbl.getTableName(), oldTable.getTableName());
    assertEquals(tbl.getSd().getLocation(), oldTable.getSd().getLocation());
    assertEquals(renamedTable.getDbName(), newTable.getDbName());
    assertEquals(renamedTable.getTableName(), newTable.getTableName());
    assertEquals(renamedTable.getSd().getLocation(), newTable.getSd().getLocation());

    //change the table name back
    driver.run(String.format("alter table %s rename to %s", renamed, tblName));
    listSize++;
    assertEquals(notifyList.size(), listSize);

    driver.run(String.format("alter table %s ADD COLUMNS (c int)", tblName));
    listSize++;
    assertEquals(notifyList.size(), listSize);
    Table altTable = msc.getTable(dbName, tblName);
    alterTableE = (AlterTableEvent) notifyList.get(listSize-1);
    assert alterTableE.getStatus();
    oldTable = alterTableE.getOldTable();
    List<FieldSchema> origCols = tbl.getSd().getCols();
    List<FieldSchema> oldCols = oldTable.getSd().getCols();
    List<FieldSchema> altCols = altTable.getSd().getCols();
    List<FieldSchema> newCols = altTable.getSd().getCols();

    assertEquals(origCols, oldCols);
    assertEquals(altCols, newCols);

    newTable = alterTableE.getNewTable();
    assertEquals(tbl.getDbName(), oldTable.getDbName());
    assertEquals(tbl.getTableName(), oldTable.getTableName());
    assertEquals(tbl.getSd().getLocation(), oldTable.getSd().getLocation());
    assertEquals(altTable.getDbName(), newTable.getDbName());
    assertEquals(altTable.getTableName(), newTable.getTableName());
    assertEquals(altTable.getSd().getLocation(), newTable.getSd().getLocation());

    Map<String,String> kvs = new HashMap<String, String>(1);
    kvs.put("b", "2011");
    msc.markPartitionForEvent("tmpdb", "tmptbl", kvs, PartitionEventType.LOAD_DONE);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    LoadPartitionDoneEvent partMarkEvent = (LoadPartitionDoneEvent)notifyList.get(listSize - 1);
    assert partMarkEvent.getStatus();
    assertEquals(partMarkEvent.getPartitionName(), kvs);
    assertEquals(partMarkEvent.getTable().getTableName(), "tmptbl");

    driver.run(String.format("alter table %s drop partition (b='2011')", tblName));
    listSize++;
    assertEquals(notifyList.size(), listSize);
    DropPartitionEvent dropPart = (DropPartitionEvent)notifyList.get(listSize - 1);
    assert dropPart.getStatus();
    assertEquals(part.getValues(), dropPart.getPartition().getValues());
    assertEquals(part.getDbName(), dropPart.getPartition().getDbName());
    assertEquals(part.getTableName(), dropPart.getPartition().getTableName());

    driver.run("drop table " + tblName);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    DropTableEvent dropTbl = (DropTableEvent)notifyList.get(listSize-1);
    assert dropTbl.getStatus();
    assertEquals(tbl.getTableName(), dropTbl.getTable().getTableName());
    assertEquals(tbl.getDbName(), dropTbl.getTable().getDbName());
    assertEquals(tbl.getSd().getLocation(), dropTbl.getTable().getSd().getLocation());

    driver.run("drop database " + dbName);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    DropDatabaseEvent dropDB = (DropDatabaseEvent)notifyList.get(listSize-1);
    assert dropDB.getStatus();
    assertEquals(db, dropDB.getDatabase());
  }
}
