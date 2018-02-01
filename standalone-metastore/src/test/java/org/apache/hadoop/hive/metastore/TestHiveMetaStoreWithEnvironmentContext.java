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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

/**
 * TestHiveMetaStoreWithEnvironmentContext. Test case for _with_environment_context
 * calls in {@link org.apache.hadoop.hive.metastore.HiveMetaStore}
 */
@Category(MetastoreUnitTest.class)
public class TestHiveMetaStoreWithEnvironmentContext {

  private Configuration conf;
  private HiveMetaStoreClient msc;
  private EnvironmentContext envContext;
  private final Database db = new Database();
  private Table table;
  private Partition partition;

  private static final String dbName = "hive3252";
  private static final String tblName = "tmptbl";
  private static final String renamed = "tmptbl2";

  @Before
  public void setUp() throws Exception {
    System.setProperty("hive.metastore.event.listeners",
        DummyListener.class.getName());

    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setLongVar(conf, ConfVars.THRIFT_CONNECTION_RETRIES, 3);
    MetastoreConf.setBoolVar(conf, ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    int port = MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf);
    MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, "thrift://localhost:" + port);
    msc = new HiveMetaStoreClient(conf);

    msc.dropDatabase(dbName, true, true);

    Map<String, String> envProperties = new HashMap<>();
    envProperties.put("hadoop.job.ugi", "test_user");
    envContext = new EnvironmentContext(envProperties);

    db.setName(dbName);

    table = new TableBuilder()
        .setDbName(dbName)
        .setTableName(tblName)
        .addTableParam("a", "string")
        .addPartCol("b", "string")
        .addCol("a", "string")
        .addCol("b", "string")
        .build();


    partition = new PartitionBuilder()
        .fromTable(table)
        .addValue("2011")
        .build();

    DummyListener.notifyList.clear();
  }

  @Test
  public void testEnvironmentContext() throws Exception {
    int listSize = 0;

    List<ListenerEvent> notifyList = DummyListener.notifyList;
    assertEquals(notifyList.size(), listSize);
    msc.createDatabase(db);
    listSize++;
    assertEquals(listSize, notifyList.size());
    CreateDatabaseEvent dbEvent = (CreateDatabaseEvent)(notifyList.get(listSize - 1));
    assert dbEvent.getStatus();

    msc.createTable(table, envContext);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    CreateTableEvent tblEvent = (CreateTableEvent)(notifyList.get(listSize - 1));
    assert tblEvent.getStatus();
    assertEquals(envContext, tblEvent.getEnvironmentContext());

    table = msc.getTable(dbName, tblName);

    partition.getSd().setLocation(table.getSd().getLocation() + "/part1");
    msc.add_partition(partition, envContext);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    AddPartitionEvent partEvent = (AddPartitionEvent)(notifyList.get(listSize-1));
    assert partEvent.getStatus();
    assertEquals(envContext, partEvent.getEnvironmentContext());

    List<String> partVals = new ArrayList<>();
    partVals.add("2012");
    msc.appendPartition(dbName, tblName, partVals, envContext);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    AddPartitionEvent appendPartEvent = (AddPartitionEvent)(notifyList.get(listSize-1));
    assert appendPartEvent.getStatus();
    assertEquals(envContext, appendPartEvent.getEnvironmentContext());

    table.setTableName(renamed);
    msc.alter_table_with_environmentContext(dbName, tblName, table, envContext);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    AlterTableEvent alterTableEvent = (AlterTableEvent) notifyList.get(listSize-1);
    assert alterTableEvent.getStatus();
    assertEquals(envContext, alterTableEvent.getEnvironmentContext());

    table.setTableName(tblName);
    msc.alter_table_with_environmentContext(dbName, renamed, table, envContext);
    listSize++;
    assertEquals(notifyList.size(), listSize);

    List<String> dropPartVals = new ArrayList<>();
    dropPartVals.add("2011");
    msc.dropPartition(dbName, tblName, dropPartVals, envContext);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    DropPartitionEvent dropPartEvent = (DropPartitionEvent)notifyList.get(listSize - 1);
    assert dropPartEvent.getStatus();
    assertEquals(envContext, dropPartEvent.getEnvironmentContext());

    msc.dropPartition(dbName, tblName, "b=2012", true, envContext);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    DropPartitionEvent dropPartByNameEvent = (DropPartitionEvent)notifyList.get(listSize - 1);
    assert dropPartByNameEvent.getStatus();
    assertEquals(envContext, dropPartByNameEvent.getEnvironmentContext());

    msc.dropTable(dbName, tblName, true, false, envContext);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    DropTableEvent dropTblEvent = (DropTableEvent)notifyList.get(listSize-1);
    assert dropTblEvent.getStatus();
    assertEquals(envContext, dropTblEvent.getEnvironmentContext());

    msc.dropDatabase(dbName);
    listSize++;
    assertEquals(notifyList.size(), listSize);

    DropDatabaseEvent dropDB = (DropDatabaseEvent)notifyList.get(listSize-1);
    assert dropDB.getStatus();
  }

}
