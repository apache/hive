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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.api;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.listener.DbNotificationListener;
import org.apache.hive.hcatalog.messaging.HCatEventMessage;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This can't use TestHCatClient because it has to have control over certain conf variables when
 * the metastore is started.  Plus, we don't need a metastore running in another thread.  The
 * local one is fine.
 */
public class TestHCatClientNotification {

  private static final Log LOG = LogFactory.getLog(TestHCatClientNotification.class.getName());
  private static HCatClient hCatClient;
  private int startTime;
  private long firstEventId;

  @BeforeClass
  public static void setupClient() throws Exception {
    HiveConf conf = new HiveConf(); conf.setVar(HiveConf.ConfVars.METASTORE_EVENT_LISTENERS,
        DbNotificationListener.class.getName());
    hCatClient = HCatClient.create(conf);
  }

  @Before
  public void setup() throws Exception {
    long now = System.currentTimeMillis() / 1000;
    startTime = 0;
    if (now > Integer.MAX_VALUE) fail("Bummer, time has fallen over the edge");
    else startTime = (int)now;
    firstEventId = hCatClient.getCurrentNotificationEventId();
  }

  @Test
  public void createDatabase() throws Exception {
    hCatClient.createDatabase(HCatCreateDBDesc.create("myhcatdb").build());
    List<HCatNotificationEvent> events = hCatClient.getNextNotification(firstEventId, 0, null);
    assertEquals(1, events.size());

    HCatNotificationEvent event = events.get(0);
    assertEquals(firstEventId + 1, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_CREATE_DATABASE_EVENT, event.getEventType());
    assertEquals("myhcatdb", event.getDbName());
    assertNull(event.getTableName());
    assertTrue(event.getMessage().matches("\\{\"eventType\":\"CREATE_DATABASE\",\"server\":\"\"," +
        "\"servicePrincipal\":\"\",\"db\":\"myhcatdb\",\"timestamp\":[0-9]+}"));
  }

  @Test
  public void dropDatabase() throws Exception {
    String dbname = "hcatdropdb";
    hCatClient.createDatabase(HCatCreateDBDesc.create(dbname).build());
    hCatClient.dropDatabase(dbname, false, HCatClient.DropDBMode.RESTRICT);

    List<HCatNotificationEvent> events = hCatClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, events.size());

    HCatNotificationEvent event = events.get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_DROP_DATABASE_EVENT, event.getEventType());
    assertEquals(dbname, event.getDbName());
    assertNull(event.getTableName());
    assertTrue(event.getMessage().matches("\\{\"eventType\":\"DROP_DATABASE\",\"server\":\"\"," +
        "\"servicePrincipal\":\"\",\"db\":\"hcatdropdb\",\"timestamp\":[0-9]+}"));
  }

  @Test
  public void createTable() throws Exception {
    String dbName = "default";
    String tableName = "hcatcreatetable";
    HCatTable table = new HCatTable(dbName, tableName);
    table.cols(Arrays.asList(new HCatFieldSchema("onecol", TypeInfoFactory.stringTypeInfo, "")));
    hCatClient.createTable(HCatCreateTableDesc.create(table).build());

    List<HCatNotificationEvent> events = hCatClient.getNextNotification(firstEventId, 0, null);
    assertEquals(1, events.size());

    HCatNotificationEvent event = events.get(0);
    assertEquals(firstEventId + 1, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_CREATE_TABLE_EVENT, event.getEventType());
    assertEquals(dbName, event.getDbName());
    assertEquals("hcatcreatetable", event.getTableName());
    assertTrue(event.getMessage().matches("\\{\"eventType\":\"CREATE_TABLE\",\"server\":\"\"," +
        "\"servicePrincipal\":\"\",\"db\":\"default\",\"table\":\"hcatcreatetable\",\"timestamp\":[0-9]+}"));
  }

  // TODO - Currently no way to test alter table, as this interface doesn't support alter table

  @Test
  public void dropTable() throws Exception {
    String dbName = "default";
    String tableName = "hcatdroptable";
    HCatTable table = new HCatTable(dbName, tableName);
    table.cols(Arrays.asList(new HCatFieldSchema("onecol", TypeInfoFactory.stringTypeInfo, "")));
    hCatClient.createTable(HCatCreateTableDesc.create(table).build());
    hCatClient.dropTable(dbName, tableName, false);

    List<HCatNotificationEvent> events = hCatClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, events.size());

    HCatNotificationEvent event = events.get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_DROP_TABLE_EVENT, event.getEventType());
    assertEquals(dbName, event.getDbName());
    assertEquals(tableName, event.getTableName());
    assertTrue(event.getMessage().matches("\\{\"eventType\":\"DROP_TABLE\",\"server\":\"\"," +
        "\"servicePrincipal\":\"\",\"db\":\"default\",\"table\":" +
        "\"hcatdroptable\",\"timestamp\":[0-9]+}"));
  }

  @Test
  public void addPartition() throws Exception {
    String dbName = "default";
    String tableName = "hcataddparttable";
    String partColName = "pc";
    HCatTable table = new HCatTable(dbName, tableName);
    table.partCol(new HCatFieldSchema(partColName, TypeInfoFactory.stringTypeInfo, ""));
    table.cols(Arrays.asList(new HCatFieldSchema("onecol", TypeInfoFactory.stringTypeInfo, "")));
    hCatClient.createTable(HCatCreateTableDesc.create(table).build());
    String partName = "testpart";
    Map<String, String> partSpec = new HashMap<String, String>(1);
    partSpec.put(partColName, partName);
    hCatClient.addPartition(
        HCatAddPartitionDesc.create(
            new HCatPartition(table, partSpec, null)
        ).build()
    );

    List<HCatNotificationEvent> events = hCatClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, events.size());

    HCatNotificationEvent event = events.get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_ADD_PARTITION_EVENT, event.getEventType());
    assertEquals("default", event.getDbName());
    assertEquals(tableName, event.getTableName());
    assertTrue(event.getMessage().matches("\\{\"eventType\":\"ADD_PARTITION\",\"server\":\"\"," +
        "\"servicePrincipal\":\"\",\"db\":\"default\",\"table\":" +
        "\"hcataddparttable\",\"timestamp\":[0-9]+,\"partitions\":\\[\\{\"pc\":\"testpart\"}]}"));
  }

  // TODO - currently no way to test alter partition, as HCatClient doesn't support it.
  @Test
  public void dropPartition() throws Exception {
    String dbName = "default";
    String tableName = "hcatdropparttable";
    String partColName = "pc";
    HCatTable table = new HCatTable(dbName, tableName);
    table.partCol(new HCatFieldSchema(partColName, TypeInfoFactory.stringTypeInfo, ""));
    table.cols(Arrays.asList(new HCatFieldSchema("onecol", TypeInfoFactory.stringTypeInfo, "")));
    hCatClient.createTable(HCatCreateTableDesc.create(table).build());
    String partName = "testpart";
    Map<String, String> partSpec = new HashMap<String, String>(1);
    partSpec.put(partColName, partName);
    hCatClient.addPartition(
        HCatAddPartitionDesc.create(
            new HCatPartition(table, partSpec, null)
        ).build()
    );
    hCatClient.dropPartitions(dbName, tableName, partSpec, false);

    List<HCatNotificationEvent> events = hCatClient.getNextNotification(firstEventId, 0, null);
    assertEquals(3, events.size());

    HCatNotificationEvent event = events.get(2);
    assertEquals(firstEventId + 3, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_DROP_PARTITION_EVENT, event.getEventType());
    assertEquals("default", event.getDbName());
    assertEquals(tableName, event.getTableName());
    assertTrue(event.getMessage().matches("\\{\"eventType\":\"DROP_PARTITION\",\"server\":\"\"," +
        "\"servicePrincipal\":\"\",\"db\":\"default\",\"table\":" +
        "\"hcatdropparttable\",\"timestamp\":[0-9]+,\"partitions\":\\[\\{\"pc\":\"testpart\"}]}"));
  }

  @Test
  public void getOnlyMaxEvents() throws Exception {
    hCatClient.createDatabase(HCatCreateDBDesc.create("hcatdb1").build());
    hCatClient.createDatabase(HCatCreateDBDesc.create("hcatdb2").build());
    hCatClient.createDatabase(HCatCreateDBDesc.create("hcatdb3").build());

    List<HCatNotificationEvent> events = hCatClient.getNextNotification(firstEventId, 2, null);
    assertEquals(2, events.size());
    assertEquals(firstEventId + 1, events.get(0).getEventId());
    assertEquals(firstEventId + 2, events.get(1).getEventId());
  }

  @Test
  public void filter() throws Exception {
    hCatClient.createDatabase(HCatCreateDBDesc.create("hcatf1").build());
    hCatClient.createDatabase(HCatCreateDBDesc.create("hcatf2").build());
    hCatClient.dropDatabase("hcatf2", false, HCatClient.DropDBMode.RESTRICT);

    IMetaStoreClient.NotificationFilter filter = new IMetaStoreClient.NotificationFilter() {
      @Override
      public boolean accept(NotificationEvent event) {
        return event.getEventType().equals(HCatConstants.HCAT_DROP_DATABASE_EVENT);
      }
    };
    List<HCatNotificationEvent> events = hCatClient.getNextNotification(firstEventId, 0, filter);
    assertEquals(1, events.size());
    assertEquals(firstEventId + 3, events.get(0).getEventId());
  }

  @Test
  public void filterWithMax() throws Exception {
    hCatClient.createDatabase(HCatCreateDBDesc.create("hcatm1").build());
    hCatClient.createDatabase(HCatCreateDBDesc.create("hcatm2").build());
    hCatClient.dropDatabase("hcatm2", false, HCatClient.DropDBMode.RESTRICT);

    IMetaStoreClient.NotificationFilter filter = new IMetaStoreClient.NotificationFilter() {
      @Override
      public boolean accept(NotificationEvent event) {
        return event.getEventType().equals(HCatConstants.HCAT_CREATE_DATABASE_EVENT);
      }
    };
    List<HCatNotificationEvent> events = hCatClient.getNextNotification(firstEventId, 1, filter);
    assertEquals(1, events.size());
    assertEquals(firstEventId + 1, events.get(0).getEventId());
  }
}
