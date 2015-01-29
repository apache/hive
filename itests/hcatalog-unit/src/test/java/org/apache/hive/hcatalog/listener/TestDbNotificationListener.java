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
package org.apache.hive.hcatalog.listener;

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
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
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

public class TestDbNotificationListener {

  private static final Log LOG = LogFactory.getLog(TestDbNotificationListener.class.getName());
  private static Map<String, String> emptyParameters = new HashMap<String, String>();
  private static IMetaStoreClient msClient;
  private int startTime;
  private long firstEventId;

  @BeforeClass
  public static void connectToMetastore() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.METASTORE_EVENT_LISTENERS,
        DbNotificationListener.class.getName());
    msClient = new HiveMetaStoreClient(conf);
  }

  @Before
  public void setup() throws Exception {
    long now = System.currentTimeMillis() / 1000;
    startTime = 0;
    if (now > Integer.MAX_VALUE) fail("Bummer, time has fallen over the edge");
    else startTime = (int) now;
    firstEventId = msClient.getCurrentNotificationEventId().getEventId();
  }

  @Test
  public void createDatabase() throws Exception {
    Database db = new Database("mydb", "no description", "file:/tmp", emptyParameters);
    msClient.createDatabase(db);

    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(1, rsp.getEventsSize());

    NotificationEvent event = rsp.getEvents().get(0);
    assertEquals(firstEventId + 1, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_CREATE_DATABASE_EVENT, event.getEventType());
    assertEquals("mydb", event.getDbName());
    assertNull(event.getTableName());
    assertTrue(event.getMessage().matches("\\{\"eventType\":\"CREATE_DATABASE\",\"server\":\"\"," +
        "\"servicePrincipal\":\"\",\"db\":\"mydb\",\"timestamp\":[0-9]+}"));
  }

  @Test
  public void dropDatabase() throws Exception {
    Database db = new Database("dropdb", "no description", "file:/tmp", emptyParameters);
    msClient.createDatabase(db);
    msClient.dropDatabase("dropdb");

    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());

    NotificationEvent event = rsp.getEvents().get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_DROP_DATABASE_EVENT, event.getEventType());
    assertEquals("dropdb", event.getDbName());
    assertNull(event.getTableName());
    assertTrue(event.getMessage().matches("\\{\"eventType\":\"DROP_DATABASE\",\"server\":\"\"," +
        "\"servicePrincipal\":\"\",\"db\":\"dropdb\",\"timestamp\":[0-9]+}"));
  }

  @Test
  public void createTable() throws Exception {
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, emptyParameters);
    Table table = new Table("mytable", "default", "me", startTime, startTime, 0, sd, null,
        emptyParameters, null, null, null);
    msClient.createTable(table);

    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(1, rsp.getEventsSize());

    NotificationEvent event = rsp.getEvents().get(0);
    assertEquals(firstEventId + 1, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_CREATE_TABLE_EVENT, event.getEventType());
    assertEquals("default", event.getDbName());
    assertEquals("mytable", event.getTableName());
    assertTrue(event.getMessage().matches("\\{\"eventType\":\"CREATE_TABLE\",\"server\":\"\"," +
        "\"servicePrincipal\":\"\",\"db\":\"default\",\"table\":\"mytable\",\"timestamp\":[0-9]+}"));
  }

  @Test
  public void alterTable() throws Exception {
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, emptyParameters);
    Table table = new Table("alttable", "default", "me", startTime, startTime, 0, sd,
        new ArrayList<FieldSchema>(), emptyParameters, null, null, null);
    msClient.createTable(table);

    table = new Table("alttable", "default", "me", startTime, startTime + 1, 0, sd,
        new ArrayList<FieldSchema>(), emptyParameters, null, null, null);
    msClient.alter_table("default", "alttable", table);

    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());

    NotificationEvent event = rsp.getEvents().get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_ALTER_TABLE_EVENT, event.getEventType());
    assertEquals("default", event.getDbName());
    assertEquals("alttable", event.getTableName());
    assertTrue(event.getMessage().matches("\\{\"eventType\":\"ALTER_TABLE\",\"server\":\"\"," +
        "\"servicePrincipal\":\"\",\"db\":\"default\",\"table\":\"alttable\"," +
        "\"timestamp\":[0-9]+}"));
  }

  @Test
  public void dropTable() throws Exception {
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, emptyParameters);
    Table table = new Table("droptable", "default", "me", startTime, startTime, 0, sd, null,
        emptyParameters, null, null, null);
    msClient.createTable(table);
    msClient.dropTable("default", "droptable");

    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());

    NotificationEvent event = rsp.getEvents().get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_DROP_TABLE_EVENT, event.getEventType());
    assertEquals("default", event.getDbName());
    assertEquals("droptable", event.getTableName());
    assertTrue(event.getMessage().matches("\\{\"eventType\":\"DROP_TABLE\",\"server\":\"\"," +
        "\"servicePrincipal\":\"\",\"db\":\"default\",\"table\":" +
        "\"droptable\",\"timestamp\":[0-9]+}"));
  }

  @Test
  public void addPartition() throws Exception {
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("ds", "string", ""));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, emptyParameters);
    Table table = new Table("addPartTable", "default", "me", startTime, startTime, 0, sd, partCols,
        emptyParameters, null, null, null);
    msClient.createTable(table);

    Partition partition = new Partition(Arrays.asList("today"), "default", "addPartTable",
        startTime, startTime, sd, emptyParameters);
    msClient.add_partition(partition);

    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());

    NotificationEvent event = rsp.getEvents().get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_ADD_PARTITION_EVENT, event.getEventType());
    assertEquals("default", event.getDbName());
    assertEquals("addparttable", event.getTableName());
    assertTrue(event.getMessage().matches("\\{\"eventType\":\"ADD_PARTITION\",\"server\":\"\"," +
        "\"servicePrincipal\":\"\",\"db\":\"default\",\"table\":" +
        "\"addparttable\",\"timestamp\":[0-9]+,\"partitions\":\\[\\{\"ds\":\"today\"}]}"));
  }

  @Test
  public void alterPartition() throws Exception {
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("ds", "string", ""));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, emptyParameters);
    Table table = new Table("alterparttable", "default", "me", startTime, startTime, 0, sd,
        partCols, emptyParameters, null, null, null);
    msClient.createTable(table);

    Partition partition = new Partition(Arrays.asList("today"), "default", "alterparttable",
        startTime, startTime, sd, emptyParameters);
    msClient.add_partition(partition);

    Partition newPart = new Partition(Arrays.asList("today"), "default", "alterparttable",
        startTime, startTime + 1, sd, emptyParameters);
    msClient.alter_partition("default", "alterparttable", newPart);

    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(3, rsp.getEventsSize());

    NotificationEvent event = rsp.getEvents().get(2);
    assertEquals(firstEventId + 3, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_ALTER_PARTITION_EVENT, event.getEventType());
    assertEquals("default", event.getDbName());
    assertEquals("alterparttable", event.getTableName());
    assertTrue(event.getMessage().matches( "\\{\"eventType\":\"ALTER_PARTITION\",\"server\":\"\"," +
        "\"servicePrincipal\":\"\",\"db\":\"default\",\"table\":\"alterparttable\"," +
        "\"timestamp\":[0-9]+,\"values\":\\[\"today\"]}"));
  }

  @Test
  public void dropPartition() throws Exception {
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("ds", "string", ""));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 0,
        serde, null, null, emptyParameters);
    Table table = new Table("dropPartTable", "default", "me", startTime, startTime, 0, sd, partCols,
        emptyParameters, null, null, null);
    msClient.createTable(table);

    Partition partition = new Partition(Arrays.asList("today"), "default", "dropPartTable",
        startTime, startTime, sd, emptyParameters);
    msClient.add_partition(partition);

    msClient.dropPartition("default", "dropparttable", Arrays.asList("today"), false);

    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(3, rsp.getEventsSize());

    NotificationEvent event = rsp.getEvents().get(2);
    assertEquals(firstEventId + 3, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(HCatConstants.HCAT_DROP_PARTITION_EVENT, event.getEventType());
    assertEquals("default", event.getDbName());
    assertEquals("dropparttable", event.getTableName());
    assertTrue(event.getMessage().matches("\\{\"eventType\":\"DROP_PARTITION\",\"server\":\"\"," +
        "\"servicePrincipal\":\"\",\"db\":\"default\",\"table\":" +
        "\"dropparttable\",\"timestamp\":[0-9]+,\"partitions\":\\[\\{\"ds\":\"today\"}]}"));
  }

  @Test
  public void getOnlyMaxEvents() throws Exception {
    Database db = new Database("db1", "no description", "file:/tmp", emptyParameters);
    msClient.createDatabase(db);
    db = new Database("db2", "no description", "file:/tmp", emptyParameters);
    msClient.createDatabase(db);
    db = new Database("db3", "no description", "file:/tmp", emptyParameters);
    msClient.createDatabase(db);

    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 2, null);
    assertEquals(2, rsp.getEventsSize());
    assertEquals(firstEventId + 1, rsp.getEvents().get(0).getEventId());
    assertEquals(firstEventId + 2, rsp.getEvents().get(1).getEventId());
  }

  @Test
  public void filter() throws Exception {
    Database db = new Database("f1", "no description", "file:/tmp", emptyParameters);
    msClient.createDatabase(db);
    db = new Database("f2", "no description", "file:/tmp", emptyParameters);
    msClient.createDatabase(db);
    msClient.dropDatabase("f2");

    IMetaStoreClient.NotificationFilter filter = new IMetaStoreClient.NotificationFilter() {
      @Override
      public boolean accept(NotificationEvent event) {
        return event.getEventType().equals(HCatConstants.HCAT_DROP_DATABASE_EVENT);
      }
    };

    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, filter);
    assertEquals(1, rsp.getEventsSize());
    assertEquals(firstEventId + 3, rsp.getEvents().get(0).getEventId());
  }

  @Test
  public void filterWithMax() throws Exception {
    Database db = new Database("f10", "no description", "file:/tmp", emptyParameters);
    msClient.createDatabase(db);
    db = new Database("f11", "no description", "file:/tmp", emptyParameters);
    msClient.createDatabase(db);
    msClient.dropDatabase("f11");

    IMetaStoreClient.NotificationFilter filter = new IMetaStoreClient.NotificationFilter() {
      @Override
      public boolean accept(NotificationEvent event) {
        return event.getEventType().equals(HCatConstants.HCAT_CREATE_DATABASE_EVENT);
      }
    };

    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 1, filter);
    assertEquals(1, rsp.getEventsSize());
    assertEquals(firstEventId + 1, rsp.getEvents().get(0).getEventId());
  }
}