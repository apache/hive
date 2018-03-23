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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import junit.framework.TestCase;
import org.junit.experimental.categories.Category;

/**
 * Ensure that the status of MetaStore events depend on the RawStore's commit status.
 */
@Category(MetastoreUnitTest.class)
public class TestMetaStoreEventListenerOnlyOnCommit {

  private Configuration conf;
  private HiveMetaStoreClient msc;

  @Before
  public void setUp() throws Exception {
    DummyRawStoreControlledCommit.setCommitSucceed(true);

    System.setProperty(ConfVars.EVENT_LISTENERS.toString(), DummyListener.class.getName());
    System.setProperty(ConfVars.RAW_STORE_IMPL.toString(),
            DummyRawStoreControlledCommit.class.getName());

    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setLongVar(conf, ConfVars.THRIFT_CONNECTION_RETRIES, 3);
    MetastoreConf.setBoolVar(conf, ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    int port = MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf);
    MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, "thrift://localhost:" + port);
    msc = new HiveMetaStoreClient(conf);

    DummyListener.notifyList.clear();
  }

  @Test
  public void testEventStatus() throws Exception {
    int listSize = 0;
    List<ListenerEvent> notifyList = DummyListener.notifyList;
    assertEquals(notifyList.size(), listSize);

    String dbName = "tmpDb";
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .build();
    msc.createDatabase(db);

    listSize += 1;
    notifyList = DummyListener.notifyList;
    assertEquals(notifyList.size(), listSize);
    assertTrue(DummyListener.getLastEvent().getStatus());

    String tableName = "unittest_TestMetaStoreEventListenerOnlyOnCommit";
    Table table = new TableBuilder()
        .setDbName(db)
        .setTableName(tableName)
        .addCol("id", "int")
        .addPartCol("ds", "string")
        .build();
    msc.createTable(table);
    listSize += 1;
    notifyList = DummyListener.notifyList;
    assertEquals(notifyList.size(), listSize);
    assertTrue(DummyListener.getLastEvent().getStatus());

    Partition part = new PartitionBuilder()
        .fromTable(table)
        .addValue("foo1")
        .build();
    msc.add_partition(part);
    listSize += 1;
    notifyList = DummyListener.notifyList;
    assertEquals(notifyList.size(), listSize);
    assertTrue(DummyListener.getLastEvent().getStatus());

    DummyRawStoreControlledCommit.setCommitSucceed(false);

    part = new PartitionBuilder()
        .fromTable(table)
        .addValue("foo2")
        .build();
    msc.add_partition(part);
    listSize += 1;
    notifyList = DummyListener.notifyList;
    assertEquals(notifyList.size(), listSize);
    assertFalse(DummyListener.getLastEvent().getStatus());

  }
}
