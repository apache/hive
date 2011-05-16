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

import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
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
    hiveConf.set("hive.metastore.local", "false");
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

    List<ListenerEvent> notifyList = DummyListener.notifyList;
    assertEquals(notifyList.size(), 0);

    driver.run("create database tmpdb");
    Database db = msc.getDatabase("tmpdb");
    assertEquals(1, notifyList.size());
    CreateDatabaseEvent dbEvent = (CreateDatabaseEvent)(notifyList.get(0));
    assert dbEvent.getStatus();
    assertEquals(db.getName(), dbEvent.getDatabase().getName());
    assertEquals(db.getLocationUri(), dbEvent.getDatabase().getLocationUri());

    driver.run("use tmpdb");
    driver.run("create table tmptbl (a string) partitioned by (b string)");
    Table tbl = msc.getTable("tmpdb", "tmptbl");
    assertEquals(notifyList.size(), 2);
    CreateTableEvent tblEvent = (CreateTableEvent)(notifyList.get(1));
    assert tblEvent.getStatus();
    assertEquals(tbl.getTableName(), tblEvent.getTable().getTableName());
    assertEquals(tbl.getDbName(), tblEvent.getTable().getDbName());
    assertEquals(tbl.getSd().getLocation(), tblEvent.getTable().getSd().getLocation());

    driver.run("alter table tmptbl add partition (b='2011')");
    Partition part = msc.getPartition("tmpdb", "tmptbl", "b=2011");
    assertEquals(notifyList.size(), 3);
    AddPartitionEvent partEvent = (AddPartitionEvent)(notifyList.get(2));
    assert partEvent.getStatus();
    assertEquals(part, partEvent.getPartition());

    driver.run("alter table tmptbl drop partition (b='2011')");
    assertEquals(notifyList.size(), 4);
    DropPartitionEvent dropPart = (DropPartitionEvent)notifyList.get(3);
    assert dropPart.getStatus();
    assertEquals(part.getValues(), dropPart.getPartition().getValues());
    assertEquals(part.getDbName(), dropPart.getPartition().getDbName());
    assertEquals(part.getTableName(), dropPart.getPartition().getTableName());

    driver.run("drop table tmptbl");
    assertEquals(notifyList.size(), 5);
    DropTableEvent dropTbl = (DropTableEvent)notifyList.get(4);
    assert dropTbl.getStatus();
    assertEquals(tbl.getTableName(), dropTbl.getTable().getTableName());
    assertEquals(tbl.getDbName(), dropTbl.getTable().getDbName());
    assertEquals(tbl.getSd().getLocation(), dropTbl.getTable().getSd().getLocation());


    driver.run("drop database tmpdb");
    assertEquals(notifyList.size(), 6);
    DropDatabaseEvent dropDB = (DropDatabaseEvent)notifyList.get(5);
    assert dropDB.getStatus();
    assertEquals(db, dropDB.getDatabase());
  }
}
