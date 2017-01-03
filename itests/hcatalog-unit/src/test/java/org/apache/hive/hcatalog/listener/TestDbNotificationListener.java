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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventRequestData;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InsertEventRequestData;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType;
import org.apache.hadoop.hive.metastore.messaging.json.JSONMessageFactory;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests DbNotificationListener when used as a transactional event listener
 * (hive.metastore.transactional.event.listeners)
 */
public class TestDbNotificationListener {
  private static final Logger LOG = LoggerFactory.getLogger(TestDbNotificationListener.class
      .getName());
  private static final int EVENTS_TTL = 30;
  private static final int CLEANUP_SLEEP_TIME = 10;
  private static Map<String, String> emptyParameters = new HashMap<String, String>();
  private static IMetaStoreClient msClient;
  private static Driver driver;
  private int startTime;
  private long firstEventId;

  @SuppressWarnings("rawtypes")
  @BeforeClass
  public static void connectToMetastore() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.METASTORE_TRANSACTIONAL_EVENT_LISTENERS,
        DbNotificationListener.class.getName());
    conf.setVar(HiveConf.ConfVars.METASTORE_EVENT_DB_LISTENER_TTL, String.valueOf(EVENTS_TTL) + "s");
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setBoolVar(HiveConf.ConfVars.FIRE_EVENTS_FOR_DML, true);
    conf.setVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
    conf.setVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL, DummyRawStoreFailEvent.class.getName());
    Class dbNotificationListener =
        Class.forName("org.apache.hive.hcatalog.listener.DbNotificationListener");
    Class[] classes = dbNotificationListener.getDeclaredClasses();
    for (Class c : classes) {
      if (c.getName().endsWith("CleanerThread")) {
        Field sleepTimeField = c.getDeclaredField("sleepTime");
        sleepTimeField.setAccessible(true);
        sleepTimeField.set(null, CLEANUP_SLEEP_TIME * 1000);
      }
    }
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    SessionState.start(new CliSessionState(conf));
    msClient = new HiveMetaStoreClient(conf);
    driver = new Driver(conf);
  }

  @Before
  public void setup() throws Exception {
    long now = System.currentTimeMillis() / 1000;
    startTime = 0;
    if (now > Integer.MAX_VALUE) {
      fail("Bummer, time has fallen over the edge");
    } else {
      startTime = (int) now;
    }
    firstEventId = msClient.getCurrentNotificationEventId().getEventId();
    DummyRawStoreFailEvent.setEventSucceed(true);
  }

  @Test
  public void createDatabase() throws Exception {
    String dbName = "createdb";
    String dbName2 = "createdb2";
    String dbLocationUri = "file:/tmp";
    String dbDescription = "no description";
    Database db = new Database(dbName, dbDescription, dbLocationUri, emptyParameters);
    msClient.createDatabase(db);

    // Read notification from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(1, rsp.getEventsSize());

    // Read event from notification
    NotificationEvent event = rsp.getEvents().get(0);
    assertEquals(firstEventId + 1, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(EventType.CREATE_DATABASE.toString(), event.getEventType());
    assertEquals(dbName, event.getDbName());
    assertNull(event.getTableName());

    // Parse the message field
    ObjectNode jsonTree = JSONMessageFactory.getJsonTree(event);
    assertEquals(EventType.CREATE_DATABASE.toString(), jsonTree.get("eventType").asText());
    assertEquals(dbName, jsonTree.get("db").asText());

    // When hive.metastore.transactional.event.listeners is set,
    // a failed event should not create a new notification
    DummyRawStoreFailEvent.setEventSucceed(false);
    db = new Database(dbName2, dbDescription, dbLocationUri, emptyParameters);
    try {
      msClient.createDatabase(db);
      fail("Error: create database should've failed");
    } catch (Exception ex) {
      // expected
    }
    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(1, rsp.getEventsSize());
  }

  @Test
  public void dropDatabase() throws Exception {
    String dbName = "dropdb";
    String dbName2 = "dropdb2";
    String dbLocationUri = "file:/tmp";
    String dbDescription = "no description";
    Database db = new Database(dbName, dbDescription, dbLocationUri, emptyParameters);
    msClient.createDatabase(db);
    msClient.dropDatabase(dbName);

    // Read notification from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);

    // Two events: one for create db and other for drop db
    assertEquals(2, rsp.getEventsSize());

    // Read event from notification
    NotificationEvent event = rsp.getEvents().get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(EventType.DROP_DATABASE.toString(), event.getEventType());
    assertEquals(dbName, event.getDbName());
    assertNull(event.getTableName());

    // Parse the message field
    ObjectNode jsonTree = JSONMessageFactory.getJsonTree(event);
    assertEquals(EventType.DROP_DATABASE.toString(), jsonTree.get("eventType").asText());
    assertEquals(dbName, jsonTree.get("db").asText());

    // When hive.metastore.transactional.event.listeners is set,
    // a failed event should not create a new notification
    db = new Database(dbName2, dbDescription, dbLocationUri, emptyParameters);
    msClient.createDatabase(db);
    DummyRawStoreFailEvent.setEventSucceed(false);
    try {
      msClient.dropDatabase(dbName2);
      fail("Error: drop database should've failed");
    } catch (Exception ex) {
      // expected
    }
    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(3, rsp.getEventsSize());
  }

  @Test
  public void createTable() throws Exception {
    String defaultDbName = "default";
    String tblName = "createtable";
    String tblName2 = "createtable2";
    String tblOwner = "me";
    String serdeLocation = "file:/tmp";
    FieldSchema col1 = new FieldSchema("col1", "int", "no comment");
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(col1);
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd =
        new StorageDescriptor(cols, serdeLocation, "input", "output", false, 0, serde, null, null,
            emptyParameters);
    Table table =
        new Table(tblName, defaultDbName, tblOwner, startTime, startTime, 0, sd, null,
            emptyParameters, null, null, null);
    msClient.createTable(table);

    // Get notifications from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(1, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(0);
    assertEquals(firstEventId + 1, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(EventType.CREATE_TABLE.toString(), event.getEventType());
    assertEquals(defaultDbName, event.getDbName());
    assertEquals(tblName, event.getTableName());

    // Parse the message field
    ObjectNode jsonTree = JSONMessageFactory.getJsonTree(event);
    assertEquals(EventType.CREATE_TABLE.toString(), jsonTree.get("eventType").asText());
    assertEquals(defaultDbName, jsonTree.get("db").asText());
    assertEquals(tblName, jsonTree.get("table").asText());
    Table tableObj = JSONMessageFactory.getTableObj(jsonTree);
    assertEquals(table, tableObj);

    // When hive.metastore.transactional.event.listeners is set,
    // a failed event should not create a new notification
    table =
        new Table(tblName2, defaultDbName, tblOwner, startTime, startTime, 0, sd, null,
            emptyParameters, null, null, null);
    DummyRawStoreFailEvent.setEventSucceed(false);
    try {
      msClient.createTable(table);
      fail("Error: create table should've failed");
    } catch (Exception ex) {
      // expected
    }
    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(1, rsp.getEventsSize());
  }

  @Test
  public void alterTable() throws Exception {
    String defaultDbName = "default";
    String tblName = "altertabletbl";
    String tblOwner = "me";
    String serdeLocation = "file:/tmp";
    FieldSchema col1 = new FieldSchema("col1", "int", "no comment");
    FieldSchema col2 = new FieldSchema("col2", "int", "no comment");
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(col1);
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd =
        new StorageDescriptor(cols, serdeLocation, "input", "output", false, 0, serde, null, null,
            emptyParameters);
    Table table =
        new Table(tblName, defaultDbName, tblOwner, startTime, startTime, 0, sd,
            new ArrayList<FieldSchema>(), emptyParameters, null, null, null);

    // Event 1
    msClient.createTable(table);
    cols.add(col2);
    table =
        new Table(tblName, defaultDbName, tblOwner, startTime, startTime, 0, sd,
            new ArrayList<FieldSchema>(), emptyParameters, null, null, null);
    // Event 2
    msClient.alter_table(defaultDbName, tblName, table);

    // Get notifications from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(EventType.ALTER_TABLE.toString(), event.getEventType());
    assertEquals(defaultDbName, event.getDbName());
    assertEquals(tblName, event.getTableName());

    AlterTableMessage alterTableMessage =
        JSONMessageFactory.getInstance().getDeserializer().getAlterTableMessage(event.getMessage());
    assertEquals(table, alterTableMessage.getTableObjAfter());

    // When hive.metastore.transactional.event.listeners is set,
    // a failed event should not create a new notification
    DummyRawStoreFailEvent.setEventSucceed(false);
    try {
      msClient.alter_table(defaultDbName, tblName, table);
      fail("Error: alter table should've failed");
    } catch (Exception ex) {
      // expected
    }
    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());
  }

  @Test
  public void dropTable() throws Exception {
    String defaultDbName = "default";
    String tblName = "droptbl";
    String tblName2 = "droptbl2";
    String tblOwner = "me";
    String serdeLocation = "file:/tmp";
    FieldSchema col1 = new FieldSchema("col1", "int", "no comment");
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(col1);
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd =
        new StorageDescriptor(cols, serdeLocation, "input", "output", false, 0, serde, null, null,
            emptyParameters);
    Table table =
        new Table(tblName, defaultDbName, tblOwner, startTime, startTime, 0, sd, null,
            emptyParameters, null, null, null);

    // Event 1
    msClient.createTable(table);
    // Event 2
    msClient.dropTable(defaultDbName, tblName);

    // Get notifications from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(EventType.DROP_TABLE.toString(), event.getEventType());
    assertEquals(defaultDbName, event.getDbName());
    assertEquals(tblName, event.getTableName());

    // Parse the message field
    ObjectNode jsonTree = JSONMessageFactory.getJsonTree(event);
    assertEquals(EventType.DROP_TABLE.toString(), jsonTree.get("eventType").asText());
    assertEquals(defaultDbName, jsonTree.get("db").asText());
    assertEquals(tblName, jsonTree.get("table").asText());

    // When hive.metastore.transactional.event.listeners is set,
    // a failed event should not create a new notification
    table =
        new Table(tblName2, defaultDbName, tblOwner, startTime, startTime, 0, sd, null,
            emptyParameters, null, null, null);
    msClient.createTable(table);
    DummyRawStoreFailEvent.setEventSucceed(false);
    try {
      msClient.dropTable(defaultDbName, tblName2);
      fail("Error: drop table should've failed");
    } catch (Exception ex) {
      // expected
    }
    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(3, rsp.getEventsSize());
  }

  @Test
  public void addPartition() throws Exception {
    String defaultDbName = "default";
    String tblName = "addptn";
    String tblName2 = "addptn2";
    String tblOwner = "me";
    String serdeLocation = "file:/tmp";
    FieldSchema col1 = new FieldSchema("col1", "int", "no comment");
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(col1);
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd =
        new StorageDescriptor(cols, serdeLocation, "input", "output", false, 0, serde, null, null,
            emptyParameters);
    FieldSchema partCol1 = new FieldSchema("ds", "string", "no comment");
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> partCol1Vals = Arrays.asList("today");
    partCols.add(partCol1);
    Table table =
        new Table(tblName, defaultDbName, tblOwner, startTime, startTime, 0, sd, partCols,
            emptyParameters, null, null, null);

    // Event 1
    msClient.createTable(table);
    Partition partition =
        new Partition(partCol1Vals, defaultDbName, tblName, startTime, startTime, sd,
            emptyParameters);
    // Event 2
    msClient.add_partition(partition);

    // Get notifications from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(EventType.ADD_PARTITION.toString(), event.getEventType());
    assertEquals(defaultDbName, event.getDbName());
    assertEquals(tblName, event.getTableName());

    // Parse the message field
    ObjectNode jsonTree = JSONMessageFactory.getJsonTree(event);
    assertEquals(EventType.ADD_PARTITION.toString(), jsonTree.get("eventType").asText());
    assertEquals(defaultDbName, jsonTree.get("db").asText());
    assertEquals(tblName, jsonTree.get("table").asText());
    List<Partition> partitionObjList = JSONMessageFactory.getPartitionObjList(jsonTree);
    assertEquals(partition, partitionObjList.get(0));

    // When hive.metastore.transactional.event.listeners is set,
    // a failed event should not create a new notification
    partition =
        new Partition(Arrays.asList("tomorrow"), defaultDbName, tblName2, startTime, startTime, sd,
            emptyParameters);
    DummyRawStoreFailEvent.setEventSucceed(false);
    try {
      msClient.add_partition(partition);
      fail("Error: add partition should've failed");
    } catch (Exception ex) {
      // expected
    }
    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());
  }

  @Test
  public void alterPartition() throws Exception {
    String defaultDbName = "default";
    String tblName = "alterptn";
    String tblOwner = "me";
    String serdeLocation = "file:/tmp";
    FieldSchema col1 = new FieldSchema("col1", "int", "no comment");
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(col1);
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd =
        new StorageDescriptor(cols, serdeLocation, "input", "output", false, 0, serde, null, null,
            emptyParameters);
    FieldSchema partCol1 = new FieldSchema("ds", "string", "no comment");
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> partCol1Vals = Arrays.asList("today");
    partCols.add(partCol1);
    Table table =
        new Table(tblName, defaultDbName, tblOwner, startTime, startTime, 0, sd, partCols,
            emptyParameters, null, null, null);

    // Event 1
    msClient.createTable(table);
    Partition partition =
        new Partition(partCol1Vals, defaultDbName, tblName, startTime, startTime, sd,
            emptyParameters);
    // Event 2
    msClient.add_partition(partition);
    Partition newPart =
        new Partition(Arrays.asList("today"), defaultDbName, tblName, startTime, startTime + 1, sd,
            emptyParameters);
    // Event 3
    msClient.alter_partition(defaultDbName, tblName, newPart, null);

    // Get notifications from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(3, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(2);
    assertEquals(firstEventId + 3, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(EventType.ALTER_PARTITION.toString(), event.getEventType());
    assertEquals(defaultDbName, event.getDbName());
    assertEquals(tblName, event.getTableName());

    // Parse the message field
    ObjectNode jsonTree = JSONMessageFactory.getJsonTree(event);
    assertEquals(EventType.ALTER_PARTITION.toString(), jsonTree.get("eventType").asText());
    assertEquals(defaultDbName, jsonTree.get("db").asText());
    assertEquals(tblName, jsonTree.get("table").asText());

    AlterPartitionMessage alterPartitionMessage =
        JSONMessageFactory.getInstance().getDeserializer().getAlterPartitionMessage(event.getMessage());
    assertEquals(newPart, alterPartitionMessage.getPtnObjAfter());

    // When hive.metastore.transactional.event.listeners is set,
    // a failed event should not create a new notification
    DummyRawStoreFailEvent.setEventSucceed(false);
    try {
      msClient.alter_partition(defaultDbName, tblName, newPart, null);
      fail("Error: alter partition should've failed");
    } catch (Exception ex) {
      // expected
    }
    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(3, rsp.getEventsSize());
  }

  @Test
  public void dropPartition() throws Exception {
    String defaultDbName = "default";
    String tblName = "dropptn";
    String tblOwner = "me";
    String serdeLocation = "file:/tmp";
    FieldSchema col1 = new FieldSchema("col1", "int", "no comment");
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(col1);
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd =
        new StorageDescriptor(cols, serdeLocation, "input", "output", false, 0, serde, null, null,
            emptyParameters);
    FieldSchema partCol1 = new FieldSchema("ds", "string", "no comment");
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> partCol1Vals = Arrays.asList("today");
    partCols.add(partCol1);
    Table table =
        new Table(tblName, defaultDbName, tblOwner, startTime, startTime, 0, sd, partCols,
            emptyParameters, null, null, null);

    // Event 1
    msClient.createTable(table);
    Partition partition =
        new Partition(partCol1Vals, defaultDbName, tblName, startTime, startTime, sd,
            emptyParameters);
    // Event 2
    msClient.add_partition(partition);
    // Event 3
    msClient.dropPartition(defaultDbName, tblName, partCol1Vals, false);

    // Get notifications from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(3, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(2);
    assertEquals(firstEventId + 3, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(EventType.DROP_PARTITION.toString(), event.getEventType());
    assertEquals(defaultDbName, event.getDbName());
    assertEquals(tblName, event.getTableName());

    // Parse the message field
    ObjectNode jsonTree = JSONMessageFactory.getJsonTree(event);
    assertEquals(EventType.DROP_PARTITION.toString(), jsonTree.get("eventType").asText());
    assertEquals(defaultDbName, jsonTree.get("db").asText());
    assertEquals(tblName, jsonTree.get("table").asText());

    // When hive.metastore.transactional.event.listeners is set,
    // a failed event should not create a new notification
    List<String> newpartCol1Vals = Arrays.asList("tomorrow");
    partition =
        new Partition(newpartCol1Vals, defaultDbName, tblName, startTime, startTime, sd,
            emptyParameters);
    msClient.add_partition(partition);
    DummyRawStoreFailEvent.setEventSucceed(false);
    try {
      msClient.dropPartition(defaultDbName, tblName, newpartCol1Vals, false);
      fail("Error: drop partition should've failed");
    } catch (Exception ex) {
      // expected
    }
    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(4, rsp.getEventsSize());
  }

  @Test
  public void createFunction() throws Exception {
    String defaultDbName = "default";
    String funcName = "createfunction";
    String funcName2 = "createfunction2";
    String ownerName = "me";
    String funcClass = "o.a.h.h.createfunc";
    String funcClass2 = "o.a.h.h.createfunc2";
    String funcResource = "file:/tmp/somewhere";
    String funcResource2 = "file:/tmp/somewhere2";
    Function func =
        new Function(funcName, defaultDbName, funcClass, ownerName, PrincipalType.USER, startTime,
            FunctionType.JAVA, Arrays.asList(new ResourceUri(ResourceType.JAR, funcResource)));
    // Event 1
    msClient.createFunction(func);

    // Get notifications from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(1, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(0);
    assertEquals(firstEventId + 1, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(EventType.CREATE_FUNCTION.toString(), event.getEventType());
    assertEquals(defaultDbName, event.getDbName());

    // Parse the message field
    Function funcObj = JSONMessageFactory.getFunctionObj(JSONMessageFactory.getJsonTree(event));
    assertEquals(defaultDbName, funcObj.getDbName());
    assertEquals(funcName, funcObj.getFunctionName());
    assertEquals(funcClass, funcObj.getClassName());
    assertEquals(ownerName, funcObj.getOwnerName());
    assertEquals(FunctionType.JAVA, funcObj.getFunctionType());
    assertEquals(1, funcObj.getResourceUrisSize());
    assertEquals(ResourceType.JAR, funcObj.getResourceUris().get(0).getResourceType());
    assertEquals(funcResource, funcObj.getResourceUris().get(0).getUri());

    // When hive.metastore.transactional.event.listeners is set,
    // a failed event should not create a new notification
    DummyRawStoreFailEvent.setEventSucceed(false);
    func =
        new Function(funcName2, defaultDbName, funcClass2, ownerName, PrincipalType.USER,
            startTime, FunctionType.JAVA, Arrays.asList(new ResourceUri(ResourceType.JAR,
                funcResource2)));
    try {
      msClient.createFunction(func);
      fail("Error: create function should've failed");
    } catch (Exception ex) {
      // expected
    }
    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(1, rsp.getEventsSize());
  }

  @Test
  public void dropFunction() throws Exception {
    String defaultDbName = "default";
    String funcName = "dropfunction";
    String funcName2 = "dropfunction2";
    String ownerName = "me";
    String funcClass = "o.a.h.h.dropfunction";
    String funcClass2 = "o.a.h.h.dropfunction2";
    String funcResource = "file:/tmp/somewhere";
    String funcResource2 = "file:/tmp/somewhere2";
    Function func =
        new Function(funcName, defaultDbName, funcClass, ownerName, PrincipalType.USER, startTime,
            FunctionType.JAVA, Arrays.asList(new ResourceUri(ResourceType.JAR, funcResource)));
    // Event 1
    msClient.createFunction(func);
    // Event 2
    msClient.dropFunction(defaultDbName, funcName);

    // Get notifications from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(EventType.DROP_FUNCTION.toString(), event.getEventType());
    assertEquals(defaultDbName, event.getDbName());

    // Parse the message field
    Function funcObj = JSONMessageFactory.getFunctionObj(JSONMessageFactory.getJsonTree(event));
    assertEquals(defaultDbName, funcObj.getDbName());
    assertEquals(funcName, funcObj.getFunctionName());
    assertEquals(funcClass, funcObj.getClassName());
    assertEquals(ownerName, funcObj.getOwnerName());
    assertEquals(FunctionType.JAVA, funcObj.getFunctionType());
    assertEquals(1, funcObj.getResourceUrisSize());
    assertEquals(ResourceType.JAR, funcObj.getResourceUris().get(0).getResourceType());
    assertEquals(funcResource, funcObj.getResourceUris().get(0).getUri());

    // When hive.metastore.transactional.event.listeners is set,
    // a failed event should not create a new notification
    func =
        new Function(funcName2, defaultDbName, funcClass2, ownerName, PrincipalType.USER,
            startTime, FunctionType.JAVA, Arrays.asList(new ResourceUri(ResourceType.JAR,
                funcResource2)));
    msClient.createFunction(func);
    DummyRawStoreFailEvent.setEventSucceed(false);
    try {
      msClient.dropFunction(defaultDbName, funcName2);
      fail("Error: drop function should've failed");
    } catch (Exception ex) {
      // expected
    }
    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(3, rsp.getEventsSize());
  }

  @Test
  public void createIndex() throws Exception {
    String indexName = "createIndex";
    String dbName = "default";
    String tableName = "createIndexTable";
    String indexTableName = tableName + "__" + indexName + "__";
    int startTime = (int) (System.currentTimeMillis() / 1000);
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", ""));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    Map<String, String> params = new HashMap<String, String>();
    params.put("key", "value");
    StorageDescriptor sd =
        new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 17, serde,
            Arrays.asList("bucketcol"), Arrays.asList(new Order("sortcol", 1)), params);
    Table table =
        new Table(tableName, dbName, "me", startTime, startTime, 0, sd, null, emptyParameters,
            null, null, null);
    // Event 1
    msClient.createTable(table);
    Index index =
        new Index(indexName, null, "default", tableName, startTime, startTime, indexTableName, sd,
            emptyParameters, false);
    Table indexTable =
        new Table(indexTableName, dbName, "me", startTime, startTime, 0, sd, null, emptyParameters,
            null, null, null);
    // Event 2, 3 (index table and index)
    msClient.createIndex(index, indexTable);

    // Get notifications from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(3, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(2);
    assertEquals(firstEventId + 3, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(EventType.CREATE_INDEX.toString(), event.getEventType());
    assertEquals(dbName, event.getDbName());

    // Parse the message field
    Index indexObj = JSONMessageFactory.getIndexObj(JSONMessageFactory.getJsonTree(event));
    assertEquals(dbName, indexObj.getDbName());
    assertEquals(indexName, indexObj.getIndexName());
    assertEquals(tableName, indexObj.getOrigTableName());
    assertEquals(indexTableName, indexObj.getIndexTableName());

    // When hive.metastore.transactional.event.listeners is set,
    // a failed event should not create a new notification
    DummyRawStoreFailEvent.setEventSucceed(false);
    index =
        new Index("createIndexTable2", null, "default", tableName, startTime, startTime,
            "createIndexTable2__createIndexTable2__", sd, emptyParameters, false);
    Table indexTable2 =
        new Table("createIndexTable2__createIndexTable2__", dbName, "me", startTime, startTime, 0,
            sd, null, emptyParameters, null, null, null);
    try {
      msClient.createIndex(index, indexTable2);
      fail("Error: create index should've failed");
    } catch (Exception ex) {
      // expected
    }
    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(3, rsp.getEventsSize());
  }

  @Test
  public void dropIndex() throws Exception {
    String indexName = "dropIndex";
    String dbName = "default";
    String tableName = "dropIndexTable";
    String indexTableName = tableName + "__" + indexName + "__";
    int startTime = (int) (System.currentTimeMillis() / 1000);
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", ""));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    Map<String, String> params = new HashMap<String, String>();
    params.put("key", "value");
    StorageDescriptor sd =
        new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 17, serde,
            Arrays.asList("bucketcol"), Arrays.asList(new Order("sortcol", 1)), params);
    Table table =
        new Table(tableName, dbName, "me", startTime, startTime, 0, sd, null, emptyParameters,
            null, null, null);
    // Event 1
    msClient.createTable(table);
    Index index =
        new Index(indexName, null, "default", tableName, startTime, startTime, indexTableName, sd,
            emptyParameters, false);
    Table indexTable =
        new Table(indexTableName, dbName, "me", startTime, startTime, 0, sd, null, emptyParameters,
            null, null, null);
    // Event 2, 3 (index table and index)
    msClient.createIndex(index, indexTable);
    // Event 4 (drops index and indexTable)
    msClient.dropIndex(dbName, tableName, indexName, true);

    // Get notifications from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(4, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(3);
    assertEquals(firstEventId + 4, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(EventType.DROP_INDEX.toString(), event.getEventType());
    assertEquals(dbName, event.getDbName());

    // Parse the message field
    Index indexObj = JSONMessageFactory.getIndexObj(JSONMessageFactory.getJsonTree(event));
    assertEquals(dbName, indexObj.getDbName());
    assertEquals(indexName.toLowerCase(), indexObj.getIndexName());
    assertEquals(tableName.toLowerCase(), indexObj.getOrigTableName());
    assertEquals(indexTableName.toLowerCase(), indexObj.getIndexTableName());

    // When hive.metastore.transactional.event.listeners is set,
    // a failed event should not create a new notification
    index =
        new Index("dropIndexTable2", null, "default", tableName, startTime, startTime,
            "dropIndexTable__dropIndexTable2__", sd, emptyParameters, false);
    Table indexTable2 =
        new Table("dropIndexTable__dropIndexTable2__", dbName, "me", startTime, startTime, 0, sd,
            null, emptyParameters, null, null, null);
    msClient.createIndex(index, indexTable2);
    DummyRawStoreFailEvent.setEventSucceed(false);
    try {
      // drops index and indexTable
      msClient.dropIndex(dbName, tableName, "dropIndex2", true);
      fail("Error: drop index should've failed");
    } catch (Exception ex) {
      // expected
    }

    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(6, rsp.getEventsSize());
  }

  @Test
  public void alterIndex() throws Exception {
    String indexName = "alterIndex";
    String dbName = "default";
    String tableName = "alterIndexTable";
    String indexTableName = tableName + "__" + indexName + "__";
    int startTime = (int) (System.currentTimeMillis() / 1000);
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", ""));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    Map<String, String> params = new HashMap<String, String>();
    params.put("key", "value");
    StorageDescriptor sd =
        new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 17, serde,
            Arrays.asList("bucketcol"), Arrays.asList(new Order("sortcol", 1)), params);
    Table table =
        new Table(tableName, dbName, "me", startTime, startTime, 0, sd, null, emptyParameters,
            null, null, null);
    // Event 1
    msClient.createTable(table);
    Index oldIndex =
        new Index(indexName, null, "default", tableName, startTime, startTime, indexTableName, sd,
            emptyParameters, false);
    Table oldIndexTable =
        new Table(indexTableName, dbName, "me", startTime, startTime, 0, sd, null, emptyParameters,
            null, null, null);
    // Event 2, 3
    msClient.createIndex(oldIndex, oldIndexTable); // creates index and index table
    Index newIndex =
        new Index(indexName, null, "default", tableName, startTime, startTime + 1, indexTableName,
            sd, emptyParameters, false);
    // Event 4
    msClient.alter_index(dbName, tableName, indexName, newIndex);

    // Get notifications from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(4, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(3);
    assertEquals(firstEventId + 4, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(EventType.ALTER_INDEX.toString(), event.getEventType());
    assertEquals(dbName, event.getDbName());

    // Parse the message field
    Index indexObj =
        JSONMessageFactory.getIndexObj(JSONMessageFactory.getJsonTree(event), "afterIndexObjJson");
    assertEquals(dbName, indexObj.getDbName());
    assertEquals(indexName, indexObj.getIndexName());
    assertEquals(tableName, indexObj.getOrigTableName());
    assertEquals(indexTableName, indexObj.getIndexTableName());
    assertTrue(indexObj.getCreateTime() < indexObj.getLastAccessTime());

    // When hive.metastore.transactional.event.listeners is set,
    // a failed event should not create a new notification
    DummyRawStoreFailEvent.setEventSucceed(false);
    try {
      msClient.alter_index(dbName, tableName, indexName, newIndex);
      fail("Error: alter index should've failed");
    } catch (Exception ex) {
      // expected
    }
    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(4, rsp.getEventsSize());
  }

  @Test
  public void insertTable() throws Exception {
    String defaultDbName = "default";
    String tblName = "inserttbl";
    String tblOwner = "me";
    String serdeLocation = "file:/tmp";
    String fileAdded = "/warehouse/mytable/b1";
    FieldSchema col1 = new FieldSchema("col1", "int", "no comment");
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(col1);
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd =
        new StorageDescriptor(cols, serdeLocation, "input", "output", false, 0, serde, null, null,
            emptyParameters);
    Table table =
        new Table(tblName, defaultDbName, tblOwner, startTime, startTime, 0, sd, null,
            emptyParameters, null, null, null);
    // Event 1
    msClient.createTable(table);

    FireEventRequestData data = new FireEventRequestData();
    InsertEventRequestData insertData = new InsertEventRequestData();
    data.setInsertData(insertData);
    insertData.addToFilesAdded(fileAdded);
    FireEventRequest rqst = new FireEventRequest(true, data);
    rqst.setDbName(defaultDbName);
    rqst.setTableName(tblName);
    // Event 2
    msClient.fireListenerEvent(rqst);

    // Get notifications from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(EventType.INSERT.toString(), event.getEventType());
    assertEquals(defaultDbName, event.getDbName());
    assertEquals(tblName, event.getTableName());
    // Parse the message field
    verifyInsertJSON(event, defaultDbName, tblName, false);
  }

  @Test
  public void insertPartition() throws Exception {
    String defaultDbName = "default";
    String tblName = "insertptn";
    String tblOwner = "me";
    String serdeLocation = "file:/tmp";
    String fileAdded = "/warehouse/mytable/b1";
    FieldSchema col1 = new FieldSchema("col1", "int", "no comment");
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(col1);
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd =
        new StorageDescriptor(cols, serdeLocation, "input", "output", false, 0, serde, null, null,
            emptyParameters);
    FieldSchema partCol1 = new FieldSchema("ds", "string", "no comment");
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> partCol1Vals = Arrays.asList("today");
    LinkedHashMap<String, String> partKeyVals = new LinkedHashMap<String, String>();
    partKeyVals.put("ds", "today");
    partCols.add(partCol1);
    Table table =
        new Table(tblName, defaultDbName, tblOwner, startTime, startTime, 0, sd, partCols,
            emptyParameters, null, null, null);
    // Event 1
    msClient.createTable(table);
    Partition partition =
        new Partition(partCol1Vals, defaultDbName, tblName, startTime, startTime, sd,
            emptyParameters);
    // Event 2
    msClient.add_partition(partition);
    FireEventRequestData data = new FireEventRequestData();
    InsertEventRequestData insertData = new InsertEventRequestData();
    data.setInsertData(insertData);
    insertData.addToFilesAdded(fileAdded);
    FireEventRequest rqst = new FireEventRequest(true, data);
    rqst.setDbName(defaultDbName);
    rqst.setTableName(tblName);
    rqst.setPartitionVals(partCol1Vals);
    // Event 3
    msClient.fireListenerEvent(rqst);

    // Get notifications from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(3, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(2);
    assertEquals(firstEventId + 3, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(EventType.INSERT.toString(), event.getEventType());
    assertEquals(defaultDbName, event.getDbName());
    assertEquals(tblName, event.getTableName());
    // Parse the message field
    verifyInsertJSON(event, defaultDbName, tblName, false);
    ObjectNode jsonTree = JSONMessageFactory.getJsonTree(event);
    LinkedHashMap<String, String> partKeyValsFromNotif =
        JSONMessageFactory.getAsMap((ObjectNode) jsonTree.get("partKeyVals"),
            new LinkedHashMap<String, String>());
    assertEquals(partKeyVals, partKeyValsFromNotif);
  }

  @Test
  public void getOnlyMaxEvents() throws Exception {
    Database db = new Database("db1", "no description", "file:/tmp", emptyParameters);
    msClient.createDatabase(db);
    db = new Database("db2", "no description", "file:/tmp", emptyParameters);
    msClient.createDatabase(db);
    db = new Database("db3", "no description", "file:/tmp", emptyParameters);
    msClient.createDatabase(db);

    // Get notifications from metastore
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
        return event.getEventType().equals(EventType.DROP_DATABASE.toString());
      }
    };

    // Get notifications from metastore
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
        return event.getEventType().equals(EventType.CREATE_DATABASE.toString());
      }
    };

    // Get notifications from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 1, filter);
    assertEquals(1, rsp.getEventsSize());
    assertEquals(firstEventId + 1, rsp.getEvents().get(0).getEventId());
  }

  @Test
  public void sqlInsertTable() throws Exception {
    String defaultDbName = "default";
    String tblName = "sqlins";
    // Event 1
    driver.run("create table " + tblName + " (c int)");
    // Event 2 (alter: marker stats event), 3 (insert), 4 (alter: stats update event)
    driver.run("insert into table " + tblName + " values (1)");
    // Event 5
    driver.run("alter table " + tblName + " add columns (c2 int)");
    // Event 6
    driver.run("drop table " + tblName);

    // Get notifications from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(6, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(0);
    assertEquals(firstEventId + 1, event.getEventId());
    assertEquals(EventType.CREATE_TABLE.toString(), event.getEventType());

    event = rsp.getEvents().get(2);
    assertEquals(firstEventId + 3, event.getEventId());
    assertEquals(EventType.INSERT.toString(), event.getEventType());
    // Parse the message field
    verifyInsertJSON(event, defaultDbName, tblName, true);

    event = rsp.getEvents().get(4);
    assertEquals(firstEventId + 5, event.getEventId());
    assertEquals(EventType.ALTER_TABLE.toString(), event.getEventType());

    event = rsp.getEvents().get(5);
    assertEquals(firstEventId + 6, event.getEventId());
    assertEquals(EventType.DROP_TABLE.toString(), event.getEventType());
  }

  @Test
  public void sqlCTAS() throws Exception {
    String sourceTblName = "sqlctasins1";
    String targetTblName = "sqlctasins2";
    // Event 1
    driver.run("create table " + sourceTblName + " (c int)");
    // Event 2 (alter: marker stats event), 3 (insert), 4 (alter: stats update event)
    driver.run("insert into table " + sourceTblName + " values (1)");
    // Event 5, 6 (alter: stats update event)
    driver.run("create table " + targetTblName + " as select c from " + sourceTblName);

    // Get notifications from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(6, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(0);
    assertEquals(firstEventId + 1, event.getEventId());
    assertEquals(EventType.CREATE_TABLE.toString(), event.getEventType());

    event = rsp.getEvents().get(2);
    assertEquals(firstEventId + 3, event.getEventId());
    assertEquals(EventType.INSERT.toString(), event.getEventType());
    // Parse the message field
    verifyInsertJSON(event, null, sourceTblName, true);

    event = rsp.getEvents().get(4);
    assertEquals(firstEventId + 5, event.getEventId());
    assertEquals(EventType.CREATE_TABLE.toString(), event.getEventType());
  }

  @Test
  public void sqlTempTable() throws Exception {
    String tempTblName = "sqltemptbl";
    driver.run("create temporary table " + tempTblName + "  (c int)");
    driver.run("insert into table " + tempTblName + " values (1)");

    // Get notifications from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(0, rsp.getEventsSize());
  }

  @Test
  public void sqlDb() throws Exception {
    String dbName = "sqldb";
    // Event 1
    driver.run("create database " + dbName);
    // Event 2
    driver.run("drop database " + dbName);

    // Get notifications from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(0);
    assertEquals(firstEventId + 1, event.getEventId());
    assertEquals(EventType.CREATE_DATABASE.toString(), event.getEventType());
    event = rsp.getEvents().get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertEquals(EventType.DROP_DATABASE.toString(), event.getEventType());
  }

  @Test
  public void sqlInsertPartition() throws Exception {
    String tblName = "sqlinsptn";
    // Event 1
    driver.run("create table " + tblName + " (c int) partitioned by (ds string)");
    // Event 2, 3, 4
    driver.run("insert into table " + tblName + " partition (ds = 'today') values (1)");
    // Event 5, 6, 7
    driver.run("insert into table " + tblName + " partition (ds = 'today') values (2)");
    // Event 8, 9, 10
    driver.run("insert into table " + tblName + " partition (ds) values (3, 'today')");
    // Event 9, 10
    driver.run("alter table " + tblName + " add partition (ds = 'yesterday')");
    // Event 10, 11, 12
    driver.run("insert into table " + tblName + " partition (ds = 'yesterday') values (2)");
    // Event 12, 13, 14
    driver.run("insert into table " + tblName + " partition (ds) values (3, 'yesterday')");
    // Event 15, 16, 17
    driver.run("insert into table " + tblName + " partition (ds) values (3, 'tomorrow')");
    // Event 18
    driver.run("alter table " + tblName + " drop partition (ds = 'tomorrow')");
    // Event 19, 20, 21
    driver.run("insert into table " + tblName + " partition (ds) values (42, 'todaytwo')");
    // Event 22, 23, 24
    driver.run("insert overwrite table " + tblName + " partition(ds='todaytwo') select c from "
        + tblName + " where 'ds'='today'");

    // Get notifications from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(24, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertEquals(EventType.ADD_PARTITION.toString(), event.getEventType());

    event = rsp.getEvents().get(3);
    assertEquals(firstEventId + 4, event.getEventId());
    assertEquals(EventType.INSERT.toString(), event.getEventType());
    // Parse the message field
    verifyInsertJSON(event, null, tblName, true);

    event = rsp.getEvents().get(6);
    assertEquals(firstEventId + 7, event.getEventId());
    assertEquals(EventType.INSERT.toString(), event.getEventType());
    // Parse the message field
    verifyInsertJSON(event, null, tblName, true);

    event = rsp.getEvents().get(9);
    assertEquals(firstEventId + 10, event.getEventId());
    assertEquals(EventType.ADD_PARTITION.toString(), event.getEventType());

    event = rsp.getEvents().get(10);
    assertEquals(firstEventId + 11, event.getEventId());
    assertEquals(EventType.INSERT.toString(), event.getEventType());
    // Parse the message field
    verifyInsertJSON(event, null, tblName, true);

    event = rsp.getEvents().get(13);
    assertEquals(firstEventId + 14, event.getEventId());
    assertEquals(EventType.INSERT.toString(), event.getEventType());
    // Parse the message field
    verifyInsertJSON(event, null, tblName, true);

    event = rsp.getEvents().get(16);
    assertEquals(firstEventId + 17, event.getEventId());
    assertEquals(EventType.ADD_PARTITION.toString(), event.getEventType());

    event = rsp.getEvents().get(18);
    assertEquals(firstEventId + 19, event.getEventId());
    assertEquals(EventType.DROP_PARTITION.toString(), event.getEventType());

    event = rsp.getEvents().get(19);
    assertEquals(firstEventId + 20, event.getEventId());
    assertEquals(EventType.ADD_PARTITION.toString(), event.getEventType());

    event = rsp.getEvents().get(20);
    assertEquals(firstEventId + 21, event.getEventId());
    assertEquals(EventType.ALTER_PARTITION.toString(), event.getEventType());
    assertTrue(event.getMessage().matches(".*\"ds\":\"todaytwo\".*"));

    event = rsp.getEvents().get(21);
    assertEquals(firstEventId + 22, event.getEventId());
    assertEquals(EventType.INSERT.toString(), event.getEventType());
    // replace-overwrite introduces no new files
    assertTrue(event.getMessage().matches(".*\"files\":\\[\\].*"));

    event = rsp.getEvents().get(22);
    assertEquals(firstEventId + 23, event.getEventId());
    assertEquals(EventType.ALTER_PARTITION.toString(), event.getEventType());
    assertTrue(event.getMessage().matches(".*\"ds\":\"todaytwo\".*"));

    event = rsp.getEvents().get(23);
    assertEquals(firstEventId + 24, event.getEventId());
    assertEquals(EventType.ALTER_PARTITION.toString(), event.getEventType());
    assertTrue(event.getMessage().matches(".*\"ds\":\"todaytwo\".*"));
  }

  private void verifyInsertJSON(NotificationEvent event, String dbName, String tblName,
      boolean verifyChecksums) throws Exception {
    // Parse the message field
    ObjectNode jsonTree = JSONMessageFactory.getJsonTree(event);
    System.out.println("JSONInsertMessage: " + jsonTree.toString());
    assertEquals(EventType.INSERT.toString(), jsonTree.get("eventType").asText());
    if (dbName != null) {
      assertEquals(dbName, jsonTree.get("db").asText());
    }
    if (tblName != null) {
      assertEquals(tblName, jsonTree.get("table").asText());
    }
    // Should have list of files
    List<String> files =
        JSONMessageFactory.getAsList((ArrayNode) jsonTree.get("files"), new ArrayList<String>());
    assertTrue(files.size() > 0);
    if (verifyChecksums) {
      // Should have list of file checksums
      List<String> fileChecksums =
          JSONMessageFactory.getAsList((ArrayNode) jsonTree.get("fileChecksums"),
              new ArrayList<String>());
      assertTrue(fileChecksums.size() > 0);

    }
  }

  @Test
  public void cleanupNotifs() throws Exception {
    Database db = new Database("cleanup1", "no description", "file:/tmp", emptyParameters);
    msClient.createDatabase(db);
    msClient.dropDatabase("cleanup1");

    LOG.info("Pulling events immediately after createDatabase/dropDatabase");
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());

    // sleep for expiry time, and then fetch again
    // sleep twice the TTL interval - things should have been cleaned by then.
    Thread.sleep(EVENTS_TTL * 2 * 1000);

    LOG.info("Pulling events again after cleanup");
    NotificationEventResponse rsp2 = msClient.getNextNotification(firstEventId, 0, null);
    LOG.info("second trigger done");
    assertEquals(0, rsp2.getEventsSize());
  }
}
