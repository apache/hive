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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.MetaStoreEventListenerConstants;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventRequestData;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.InsertEventRequestData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.AlterDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.BatchAcidWriteEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateFunctionEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropFunctionEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.metastore.events.AllocWriteIdEvent;
import org.apache.hadoop.hive.metastore.events.AcidWriteEvent;
import org.apache.hadoop.hive.metastore.messaging.AddPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateFunctionMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateTableMessage;
import org.apache.hadoop.hive.metastore.messaging.DropDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.DropFunctionMessage;
import org.apache.hadoop.hive.metastore.messaging.DropPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.DropTableMessage;
import org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType;
import org.apache.hadoop.hive.metastore.messaging.InsertMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.metastore.messaging.json.JSONMessageEncoder;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.hcatalog.api.repl.ReplicationV1CompatRule;
import org.apache.hive.hcatalog.data.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tests DbNotificationListener when used as a transactional event listener
 * (hive.metastore.transactional.event.listeners)
 */
public class TestDbNotificationListener
{
  private static final Logger LOG = LoggerFactory.getLogger(TestDbNotificationListener.class
          .getName());
  private static final int EVENTS_TTL = 30;
  private static final int CLEANUP_SLEEP_TIME = 10;
  private static Map<String, String> emptyParameters = new HashMap<String, String>();
  private static IMetaStoreClient msClient;
  private static IDriver driver;
  private static MessageDeserializer md;

  static {
    try {
      md = MessageFactory.getInstance(JSONMessageEncoder.FORMAT).getDeserializer();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private int startTime;
  private long firstEventId;
  private final String testTempDir = Paths.get(System.getProperty("java.io.tmpdir"), "testDbNotif").toString();

  private static List<String> testsToSkipForReplV1BackwardCompatTesting =
      new ArrayList<>(Arrays.asList("cleanupNotifs", "cleanupNotificationWithError", "sqlTempTable"));
  // Make sure we skip backward-compat checking for those tests that don't generate events

  private static ReplicationV1CompatRule bcompat = null;

  @Rule
  public TestRule replV1BackwardCompatibleRule = bcompat;
  // Note - above looks funny because it seems like we're instantiating a static var, and
  // then a non-static var as the rule, but the reason this is required is because Rules
  // are not allowed to be static, but we wind up needing it initialized from a static
  // context. So, bcompat is initialzed in a static context, but this rule is initialized
  // before the tests run, and will pick up an initialized value of bcompat.

  /* This class is used to verify that HiveMetaStore calls the non-transactional listeners with the
    * current event ID set by the DbNotificationListener class */
  public static class MockMetaStoreEventListener extends MetaStoreEventListener {
    private static Stack<Pair<EventType, String>> eventsIds = new Stack<>();

    private static void pushEventId(EventType eventType, final ListenerEvent event) {
      if (event.getStatus()) {
        Map<String, String> parameters = event.getParameters();
        if (parameters.containsKey(MetaStoreEventListenerConstants.DB_NOTIFICATION_EVENT_ID_KEY_NAME)) {
          Pair<EventType, String> pair =
              new Pair<>(eventType, parameters.get(MetaStoreEventListenerConstants.DB_NOTIFICATION_EVENT_ID_KEY_NAME));
          eventsIds.push(pair);
        }
      }
    }

    public static void popAndVerifyLastEventId(EventType eventType, long id) {
      if (!eventsIds.isEmpty()) {
        Pair<EventType, String> pair = eventsIds.pop();

        assertEquals("Last event type does not match.", eventType, pair.first);
        assertEquals("Last event ID does not match.", Long.toString(id), pair.second);
      } else {
        assertTrue("List of events is empty.",false);
      }
    }

    public static void clearEvents() {
      eventsIds.clear();
    }

    public MockMetaStoreEventListener(Configuration config) {
      super(config);
    }

    @Override
    public void onCreateTable (CreateTableEvent tableEvent) throws MetaException {
      pushEventId(EventType.CREATE_TABLE, tableEvent);
    }

    @Override
    public void onDropTable (DropTableEvent tableEvent)  throws MetaException {
      pushEventId(EventType.DROP_TABLE, tableEvent);
    }

    @Override
    public void onAlterTable (AlterTableEvent tableEvent) throws MetaException {
      pushEventId(EventType.ALTER_TABLE, tableEvent);
    }

    @Override
    public void onAlterDatabase(AlterDatabaseEvent dbEvent) throws MetaException {
      pushEventId(EventType.ALTER_DATABASE, dbEvent);
    }

    @Override
    public void onAddPartition (AddPartitionEvent partitionEvent) throws MetaException {
      pushEventId(EventType.ADD_PARTITION, partitionEvent);
    }

    @Override
    public void onDropPartition (DropPartitionEvent partitionEvent)  throws MetaException {
      pushEventId(EventType.DROP_PARTITION, partitionEvent);
    }

    @Override
    public void onAlterPartition (AlterPartitionEvent partitionEvent)  throws MetaException {
      pushEventId(EventType.ALTER_PARTITION, partitionEvent);
    }

    @Override
    public void onCreateDatabase (CreateDatabaseEvent dbEvent) throws MetaException {
      pushEventId(EventType.CREATE_DATABASE, dbEvent);
    }

    @Override
    public void onDropDatabase (DropDatabaseEvent dbEvent) throws MetaException {
      pushEventId(EventType.DROP_DATABASE, dbEvent);
    }

    @Override
    public void onCreateFunction (CreateFunctionEvent fnEvent) throws MetaException {
      pushEventId(EventType.CREATE_FUNCTION, fnEvent);
    }

    @Override
    public void onDropFunction (DropFunctionEvent fnEvent) throws MetaException {
      pushEventId(EventType.DROP_FUNCTION, fnEvent);
    }

    @Override
    public void onInsert(InsertEvent insertEvent) throws MetaException {
      pushEventId(EventType.INSERT, insertEvent);
    }

    public void onAllocWriteId(AllocWriteIdEvent allocWriteIdEvent) throws MetaException {
      pushEventId(EventType.ALLOC_WRITE_ID, allocWriteIdEvent);
    }

    public void onAcidWrite(AcidWriteEvent acidWriteEvent) throws MetaException {
      pushEventId(EventType.ACID_WRITE, acidWriteEvent);
    }

    public void onBatchAcidWrite(BatchAcidWriteEvent batchAcidWriteEvent) throws MetaException {
      pushEventId(EventType.BATCH_ACID_WRITE, batchAcidWriteEvent);
    }
  }

  @SuppressWarnings("rawtypes")
  @BeforeClass
  public static void connectToMetastore() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.METASTORE_TRANSACTIONAL_EVENT_LISTENERS,
        DbNotificationListener.class.getName());
    conf.setVar(HiveConf.ConfVars.METASTORE_EVENT_LISTENERS, MockMetaStoreEventListener.class.getName());
    conf.setVar(HiveConf.ConfVars.METASTORE_EVENT_DB_LISTENER_TTL, String.valueOf(EVENTS_TTL) + "s");
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setBoolVar(HiveConf.ConfVars.FIRE_EVENTS_FOR_DML, true);
    conf.setVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL, DummyRawStoreFailEvent.class.getName());
    MetastoreConf.setTimeVar(conf, MetastoreConf.ConfVars.EVENT_DB_LISTENER_CLEAN_INTERVAL, CLEANUP_SLEEP_TIME, TimeUnit.SECONDS);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY, JSONMessageEncoder.class.getName());
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    SessionState.start(new CliSessionState(conf));
    msClient = new HiveMetaStoreClient(conf);
    driver = DriverFactory.newDriver(conf);
    md = JSONMessageEncoder.getInstance().getDeserializer();

    bcompat = new ReplicationV1CompatRule(msClient, conf, testsToSkipForReplV1BackwardCompatTesting );
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

  @AfterClass
  public static void tearDownAfterClass() {

    if (msClient != null) {
      msClient.close();
    }
    if (driver != null) {
      driver.close();
    }

  }

  @After
  public void tearDown() {
    MockMetaStoreEventListener.clearEvents();
  }

  // Test if the number of events between the given event ids and with the given database name are
  // same as expected. toEventId = 0 is treated as unbounded. Same is the case with limit 0.
  private void testEventCounts(String dbName, long fromEventId, Long toEventId, Integer limit,
                               long expectedCount) throws Exception {
    NotificationEventsCountRequest rqst = new NotificationEventsCountRequest(fromEventId, dbName);

    if (toEventId != null) {
      rqst.setToEventId(toEventId);
    }
    if (limit != null) {
      rqst.setLimit(limit);
    }

    assertEquals(expectedCount, msClient.getNotificationEventsCount(rqst).getEventsCount());
  }

  @Test
  public void createDatabase() throws Exception {
    String dbName = "createdb";
    String dbName2 = "createdb2";
    String dbLocationUri = testTempDir;
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
    CreateDatabaseMessage createDbMsg = md.getCreateDatabaseMessage(event.getMessage());
    assertEquals(dbName, createDbMsg.getDB());
    assertEquals(db, createDbMsg.getDatabaseObject());

    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_DATABASE, firstEventId + 1);

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

    // There's only one event corresponding to CREATE DATABASE
    testEventCounts(dbName, firstEventId, null, null, 1);
    testEventCounts(dbName2, firstEventId, null, null, 0);
  }

  @Test
  public void alterDatabase() throws Exception {
    String dbName = "alterdb";
    String dbLocationUri = testTempDir;
    String dbDescription = "no description";
    msClient.createDatabase(new Database(dbName, dbDescription, dbLocationUri, emptyParameters));
    // get the db for comparison below since it may include additional parameters
    Database dbBefore = msClient.getDatabase(dbName);
    // create alter database notification
    String newDesc = "test database";
    Database dbAfter = dbBefore.deepCopy();
    dbAfter.setDescription(newDesc);
    dbAfter.setOwnerName("test2");
    dbAfter.setOwnerType(PrincipalType.USER);
    msClient.alterDatabase(dbName, dbAfter);
    dbAfter = msClient.getDatabase(dbName);

    // Read notification from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(2, rsp.getEventsSize());
    // check the contents of alter database notification
    NotificationEvent event = rsp.getEvents().get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(EventType.ALTER_DATABASE.toString(), event.getEventType());
    assertEquals(dbName, event.getDbName());
    assertNull(event.getTableName());

    // Parse the message field
    AlterDatabaseMessage alterDatabaseMessage = md.getAlterDatabaseMessage(event.getMessage());
    assertEquals(dbName, alterDatabaseMessage.getDB());
    assertEquals(dbBefore, alterDatabaseMessage.getDbObjBefore());
    assertEquals(dbAfter, alterDatabaseMessage.getDbObjAfter());
  }

  @Test
  public void dropDatabase() throws Exception {
    String dbName = "dropdb";
    String dbName2 = "dropdb2";
    String dbLocationUri = testTempDir;
    String dbDescription = "no description";
    msClient.createDatabase(new Database(dbName, dbDescription, dbLocationUri, emptyParameters));

    // Get the DB for comparison below since it may include additional parameters
    Database db = msClient.getDatabase(dbName);
    // Drop the database
    msClient.dropDatabase(dbName);

    // Read notification from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);

    // Two events: one for create db and other for drop db
    assertEquals(2, rsp.getEventsSize());
    testEventCounts(dbName, firstEventId, null, null, 2);

    // Read event from notification
    NotificationEvent event = rsp.getEvents().get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(EventType.DROP_DATABASE.toString(), event.getEventType());
    assertEquals(dbName, event.getDbName());
    assertNull(event.getTableName());

    // Parse the message field
    DropDatabaseMessage dropDbMsg = md.getDropDatabaseMessage(event.getMessage());
    assertEquals(dbName, dropDbMsg.getDB());
    assertEquals(db, dropDbMsg.getDatabaseObject());

    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.DROP_DATABASE, firstEventId + 2);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_DATABASE, firstEventId + 1);

    // When hive.metastore.transactional.event.listeners is set,
    // a failed event should not create a new notification
    msClient.createDatabase(new Database(dbName2, dbDescription, dbLocationUri, emptyParameters));
    DummyRawStoreFailEvent.setEventSucceed(false);
    try {
      msClient.dropDatabase(dbName2);
      fail("Error: drop database should've failed");
    } catch (Exception ex) {
      // expected
    }
    rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(3, rsp.getEventsSize());
    testEventCounts(dbName2, firstEventId, null, null, 1);
  }

  @Test
  public void createTable() throws Exception {
    String defaultDbName = "default";
    String tblName = "createtable";
    String tblName2 = "createtable2";
    String tblOwner = "me";
    String serdeLocation = testTempDir;
    FieldSchema col1 = new FieldSchema("col1", "int", "no comment");
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(col1);
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd =
        new StorageDescriptor(cols, serdeLocation, "input", "output", false, 0, serde, null, null,
            emptyParameters);
    Table table =
        new Table(tblName, defaultDbName, tblOwner, startTime, startTime, 0, sd, null,
            emptyParameters, null, null, TableType.MANAGED_TABLE.toString());
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
    CreateTableMessage createTblMsg = md.getCreateTableMessage(event.getMessage());
    assertEquals(defaultDbName, createTblMsg.getDB());
    assertEquals(tblName, createTblMsg.getTable());
    assertEquals(table, createTblMsg.getTableObj());
    assertEquals(TableType.MANAGED_TABLE.toString(), createTblMsg.getTableType());

    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_TABLE, firstEventId + 1);

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
    testEventCounts(defaultDbName, firstEventId, null, null, 1);
  }

  @Test
  public void alterTable() throws Exception {
    String defaultDbName = "default";
    String tblName = "altertabletbl";
    String tblOwner = "me";
    String serdeLocation = testTempDir;
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

    AlterTableMessage alterTableMessage = md.getAlterTableMessage(event.getMessage());
    assertEquals(table, alterTableMessage.getTableObjAfter());
    assertEquals(TableType.MANAGED_TABLE.toString(), alterTableMessage.getTableType());

    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.ALTER_TABLE, firstEventId + 2);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_TABLE, firstEventId + 1);

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
    testEventCounts(defaultDbName, firstEventId, null, null, 2);
  }

  @Test
  public void dropTable() throws Exception {
    String defaultDbName = "default";
    String tblName = "droptbl";
    String tblName2 = "droptbl2";
    String tblOwner = "me";
    String serdeLocation = testTempDir;
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
    DropTableMessage dropTblMsg = md.getDropTableMessage(event.getMessage());
    assertEquals(defaultDbName, dropTblMsg.getDB());
    assertEquals(tblName, dropTblMsg.getTable());
    assertEquals(TableType.MANAGED_TABLE.toString(), dropTblMsg.getTableType());
    Table tableObj = dropTblMsg.getTableObj();
    assertEquals(table.getDbName(), tableObj.getDbName());
    assertEquals(table.getTableName(), tableObj.getTableName());
    assertEquals(table.getOwner(), tableObj.getOwner());
    assertEquals(table.getParameters(), tableObj.getParameters());
    assertEquals(TableType.MANAGED_TABLE.toString(), tableObj.getTableType());

    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.DROP_TABLE, firstEventId + 2);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_TABLE, firstEventId + 1);

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
    testEventCounts(defaultDbName, firstEventId, null, null, 3);
  }

  @Test
  public void addPartition() throws Exception {
    String defaultDbName = "default";
    String tblName = "addptn";
    String tblName2 = "addptn2";
    String tblOwner = "me";
    String serdeLocation = testTempDir;
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
    AddPartitionMessage addPtnMsg = md.getAddPartitionMessage(event.getMessage());
    assertEquals(defaultDbName, addPtnMsg.getDB());
    assertEquals(tblName, addPtnMsg.getTable());
    Iterator<Partition> ptnIter = addPtnMsg.getPartitionObjs().iterator();
    assertTrue(ptnIter.hasNext());
    assertEquals(partition, ptnIter.next());
    assertEquals(TableType.MANAGED_TABLE.toString(), addPtnMsg.getTableType());

    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.ADD_PARTITION, firstEventId + 2);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_TABLE, firstEventId + 1);

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
    testEventCounts(defaultDbName, firstEventId, null, null, 2);
  }

  @Test
  public void alterPartition() throws Exception {
    String defaultDbName = "default";
    String tblName = "alterptn";
    String tblOwner = "me";
    String serdeLocation = testTempDir;
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
    AlterPartitionMessage alterPtnMsg = md.getAlterPartitionMessage(event.getMessage());
    assertEquals(defaultDbName, alterPtnMsg.getDB());
    assertEquals(tblName, alterPtnMsg.getTable());
    assertEquals(newPart, alterPtnMsg.getPtnObjAfter());
    assertEquals(TableType.MANAGED_TABLE.toString(), alterPtnMsg.getTableType());

    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.ADD_PARTITION, firstEventId + 2);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_TABLE, firstEventId + 1);

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
    testEventCounts(defaultDbName, firstEventId, null, null, 3);
  }

  @Test
  public void dropPartition() throws Exception {
    String defaultDbName = "default";
    String tblName = "dropptn";
    String tblOwner = "me";
    String serdeLocation = testTempDir;
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
    DropPartitionMessage dropPtnMsg = md.getDropPartitionMessage(event.getMessage());
    assertEquals(defaultDbName, dropPtnMsg.getDB());
    assertEquals(tblName, dropPtnMsg.getTable());
    Table tableObj = dropPtnMsg.getTableObj();
    assertEquals(table.getDbName(), tableObj.getDbName());
    assertEquals(table.getTableName(), tableObj.getTableName());
    assertEquals(table.getOwner(), tableObj.getOwner());
    assertEquals(TableType.MANAGED_TABLE.toString(), dropPtnMsg.getTableType());

    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.DROP_PARTITION, firstEventId + 3);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.ADD_PARTITION, firstEventId + 2);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_TABLE, firstEventId + 1);

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
    testEventCounts(defaultDbName, firstEventId, null, null, 4);
  }

  @Test
  public void exchangePartition() throws Exception {
    String dbName = "default";
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", "nocomment"));
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("part", "int", ""));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd1 =
        new StorageDescriptor(cols, Paths.get(testTempDir, "1").toString(), "input", "output", false, 0, serde, null,
            null, emptyParameters);
    Table tab1 = new Table("tab1", dbName, "me", startTime, startTime, 0, sd1, partCols,
        emptyParameters, null, null, null);
    msClient.createTable(tab1);
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(1, rsp.getEventsSize()); // add_table

    StorageDescriptor sd2 =
        new StorageDescriptor(cols, Paths.get(testTempDir, "2").toString(), "input", "output", false, 0, serde, null,
            null, emptyParameters);
    Table tab2 = new Table("tab2", dbName, "me", startTime, startTime, 0, sd2, partCols,
        emptyParameters, null, null, null); // add_table
    msClient.createTable(tab2);
    rsp = msClient.getNextNotification(firstEventId + 1, 0, null);
    assertEquals(1, rsp.getEventsSize());

    StorageDescriptor sd1part =
        new StorageDescriptor(cols, Paths.get(testTempDir, "1", "part=1").toString(), "input", "output", false, 0,
            serde, null, null, emptyParameters);
    StorageDescriptor sd2part =
        new StorageDescriptor(cols, Paths.get(testTempDir, "1", "part=2").toString(), "input", "output", false, 0,
            serde, null, null, emptyParameters);
    StorageDescriptor sd3part =
        new StorageDescriptor(cols, Paths.get(testTempDir, "1", "part=3").toString(), "input", "output", false, 0,
            serde, null, null, emptyParameters);
    Partition part1 = new Partition(Arrays.asList("1"), "default", tab1.getTableName(),
        startTime, startTime, sd1part, emptyParameters);
    Partition part2 = new Partition(Arrays.asList("2"), "default", tab1.getTableName(),
        startTime, startTime, sd2part, emptyParameters);
    Partition part3 = new Partition(Arrays.asList("3"), "default", tab1.getTableName(),
        startTime, startTime, sd3part, emptyParameters);
    msClient.add_partitions(Arrays.asList(part1, part2, part3));
    rsp = msClient.getNextNotification(firstEventId + 2, 0, null);
    assertEquals(1, rsp.getEventsSize()); // add_partition

    msClient.exchange_partition(ImmutableMap.of("part", "1"),
        dbName, tab1.getTableName(), dbName, tab2.getTableName());

    rsp = msClient.getNextNotification(firstEventId + 3, 0, null);
    assertEquals(2, rsp.getEventsSize());

    NotificationEvent event = rsp.getEvents().get(0);
    assertEquals(firstEventId + 4, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(EventType.ADD_PARTITION.toString(), event.getEventType());
    assertEquals(dbName, event.getDbName());
    assertEquals(tab2.getTableName(), event.getTableName());

    // Parse the message field
    AddPartitionMessage addPtnMsg = md.getAddPartitionMessage(event.getMessage());
    assertEquals(dbName, addPtnMsg.getDB());
    assertEquals(tab2.getTableName(), addPtnMsg.getTable());
    Iterator<Partition> ptnIter = addPtnMsg.getPartitionObjs().iterator();
    assertEquals(TableType.MANAGED_TABLE.toString(), addPtnMsg.getTableType());

    assertTrue(ptnIter.hasNext());
    Partition msgPart = ptnIter.next();
    assertEquals(part1.getValues(), msgPart.getValues());
    assertEquals(dbName, msgPart.getDbName());
    assertEquals(tab2.getTableName(), msgPart.getTableName());

    event = rsp.getEvents().get(1);
    assertEquals(firstEventId + 5, event.getEventId());
    assertTrue(event.getEventTime() >= startTime);
    assertEquals(EventType.DROP_PARTITION.toString(), event.getEventType());
    assertEquals(dbName, event.getDbName());
    assertEquals(tab1.getTableName(), event.getTableName());

    // Parse the message field
    DropPartitionMessage dropPtnMsg = md.getDropPartitionMessage(event.getMessage());
    assertEquals(dbName, dropPtnMsg.getDB());
    assertEquals(tab1.getTableName(), dropPtnMsg.getTable());
    assertEquals(TableType.MANAGED_TABLE.toString(), dropPtnMsg.getTableType());
    Iterator<Map<String, String>> parts = dropPtnMsg.getPartitions().iterator();
    assertTrue(parts.hasNext());
    assertEquals(part1.getValues(), Lists.newArrayList(parts.next().values()));

    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.DROP_PARTITION, firstEventId + 5);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.ADD_PARTITION, firstEventId + 4);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.ADD_PARTITION, firstEventId + 3);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_TABLE, firstEventId + 2);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_TABLE, firstEventId + 1);
    testEventCounts(dbName, firstEventId, null, null, 5);
  }

  @Test
  public void createFunction() throws Exception {
    String defaultDbName = "default";
    String funcName = "createfunction";
    String funcName2 = "createfunction2";
    String ownerName = "me";
    String funcClass = "o.a.h.h.createfunc";
    String funcClass2 = "o.a.h.h.createfunc2";
    String funcResource = Paths.get(testTempDir, "somewhere").toString();
    String funcResource2 = Paths.get(testTempDir, "somewhere2").toString();
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
    CreateFunctionMessage createFuncMsg = md.getCreateFunctionMessage(event.getMessage());
    assertEquals(defaultDbName, createFuncMsg.getDB());
    Function funcObj = createFuncMsg.getFunctionObj();
    assertEquals(defaultDbName, funcObj.getDbName());
    assertEquals(funcName, funcObj.getFunctionName());
    assertEquals(funcClass, funcObj.getClassName());
    assertEquals(ownerName, funcObj.getOwnerName());
    assertEquals(FunctionType.JAVA, funcObj.getFunctionType());
    assertEquals(1, funcObj.getResourceUrisSize());
    assertEquals(ResourceType.JAR, funcObj.getResourceUris().get(0).getResourceType());
    assertEquals(funcResource, funcObj.getResourceUris().get(0).getUri());

    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_FUNCTION, firstEventId + 1);

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
    testEventCounts(defaultDbName, firstEventId, null, null, 1);
  }

  @Test
  public void dropFunction() throws Exception {
    String defaultDbName = "default";
    String funcName = "dropfunction";
    String funcName2 = "dropfunction2";
    String ownerName = "me";
    String funcClass = "o.a.h.h.dropfunction";
    String funcClass2 = "o.a.h.h.dropfunction2";
    String funcResource = Paths.get(testTempDir, "somewhere").toString();
    String funcResource2 = Paths.get(testTempDir, "somewhere2").toString();
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
    DropFunctionMessage dropFuncMsg = md.getDropFunctionMessage(event.getMessage());
    assertEquals(defaultDbName, dropFuncMsg.getDB());
    assertEquals(funcName, dropFuncMsg.getFunctionName());

    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.DROP_FUNCTION, firstEventId + 2);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_FUNCTION, firstEventId + 1);

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
    testEventCounts(defaultDbName, firstEventId, null, null, 3);
  }

  @Test
  public void insertTable() throws Exception {
    String defaultDbName = "default";
    String tblName = "inserttbl";
    String tblOwner = "me";
    String serdeLocation = testTempDir;
    String fileAdded = "/warehouse/mytable/b1";
    String checksumAdded = "1234";
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
    insertData.addToFilesAddedChecksum(checksumAdded);
    insertData.setReplace(false);
    FireEventRequest rqst = new FireEventRequest(true, data);
    rqst.setDbName(defaultDbName);
    rqst.setTableName(tblName);
    // Event 2
    FireEventResponse response = msClient.fireListenerEvent(rqst);
    assertTrue("Event id must be set in the fireEvent response", response.isSetEventIds());
    Assert.assertNotNull(response.getEventIds());
    Assert.assertTrue(response.getEventIds().size() == 1);
    Assert.assertEquals(firstEventId+2, response.getEventIds().get(0).longValue());

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
    verifyInsert(event, defaultDbName, tblName);

    // Parse the message field
    InsertMessage insertMessage = md.getInsertMessage(event.getMessage());
    assertEquals(defaultDbName, insertMessage.getDB());
    assertEquals(tblName, insertMessage.getTable());
    assertEquals(TableType.MANAGED_TABLE.toString(), insertMessage.getTableType());
    assertFalse(insertMessage.isReplace());

    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.INSERT, firstEventId + 2);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_TABLE, firstEventId + 1);
    testEventCounts(defaultDbName, firstEventId, null, null, 2);
  }

  @Test
  public void insertPartition() throws Exception {
    String defaultDbName = "default";
    String tblName = "insertptn";
    String tblOwner = "me";
    String serdeLocation = testTempDir;
    String fileAdded = "/warehouse/mytable/b1";
    String checksumAdded = "1234";
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
    List<String> partKeyVals = new ArrayList<String>();
    partKeyVals.add("today");

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
    insertData.addToFilesAddedChecksum(checksumAdded);
    insertData.setReplace(false);
    FireEventRequest rqst = new FireEventRequest(true, data);
    rqst.setDbName(defaultDbName);
    rqst.setTableName(tblName);
    rqst.setPartitionVals(partCol1Vals);
    // Event 3
    verifyInsertEventReceived(defaultDbName, tblName, Arrays.asList(partKeyVals), rqst,
        firstEventId + 3, 1);

    // Verify the eventID was passed to the non-transactional listener
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.INSERT, firstEventId + 3);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.ADD_PARTITION, firstEventId + 2);
    MockMetaStoreEventListener.popAndVerifyLastEventId(EventType.CREATE_TABLE, firstEventId + 1);
    testEventCounts(defaultDbName, firstEventId, null, null, 3);

    // fire multiple insert events on partition
    // add some more partitions
    partition =
        new Partition(Arrays.asList("yesterday"), defaultDbName, tblName, startTime, startTime, sd,
            emptyParameters);
    // Event 4
    msClient.add_partition(partition);
    partition =
        new Partition(Arrays.asList("tomorrow"), defaultDbName, tblName, startTime, startTime, sd,
            emptyParameters);
    // Event 5
    msClient.add_partition(partition);
    data = new FireEventRequestData();
    List<InsertEventRequestData> insertEventRequestDataList = new ArrayList<>();
    data.setInsertDatas(insertEventRequestDataList);
    // event 6
    InsertEventRequestData insertData1 = new InsertEventRequestData();
    insertData1.addToFilesAdded(fileAdded);
    insertData1.addToFilesAddedChecksum(checksumAdded);
    insertData1.setReplace(false);
    insertData1.setPartitionVal(Arrays.asList("yesterday"));

    // event 7
    InsertEventRequestData insertData2 = new InsertEventRequestData();
    insertData2.addToFilesAdded(fileAdded);
    insertData2.addToFilesAddedChecksum(checksumAdded);
    insertData2.setReplace(false);
    insertData2.setPartitionVal(Arrays.asList("today"));

    // event 8
    InsertEventRequestData insertData3 = new InsertEventRequestData();
    insertData3.addToFilesAdded(fileAdded);
    insertData3.addToFilesAddedChecksum(checksumAdded);
    insertData3.setReplace(false);
    insertData3.setPartitionVal(Arrays.asList("tomorrow"));
    // fire insert event on 3 partitions
    insertEventRequestDataList.add(insertData1);
    insertEventRequestDataList.add(insertData2);
    insertEventRequestDataList.add(insertData3);

    rqst = new FireEventRequest(true, data);
    rqst.setDbName(defaultDbName);
    rqst.setTableName(tblName);

    verifyInsertEventReceived(defaultDbName, tblName, Arrays
        .asList(Arrays.asList("yesterday"), Arrays.asList("today"),
            Arrays.asList("tomorrow")), rqst, firstEventId + 6, 3);

    // negative test. partition values must be set when firing bulk insert events
    data.getInsertDatas().get(1).unsetPartitionVal();
    boolean threwException = false;
    try {
      FireEventResponse response = msClient.fireListenerEvent(rqst);
    } catch (MetaException ex) {
      threwException = true;
      Assert.assertTrue(ex instanceof  MetaException);
      Assert.assertTrue(ex.getMessage()
          .contains("Partition values must be set when firing multiple insert events"));
    }
    Assert.assertTrue("bulk insert event API didn't "
        + "throw exception when partition values were not set", threwException);
  }

  private void verifyInsertEventReceived(String defaultDbName, String tblName,
      List<List<String>> partKeyVals, FireEventRequest rqst, long expectedStartEventId,
      int expectedNumOfEvents) throws Exception {
    FireEventResponse response = msClient.fireListenerEvent(rqst);
    assertTrue("Event id must be set in the fireEvent response", response.isSetEventIds());
    Assert.assertNotNull(response.getEventIds());
    Assert.assertTrue(response.getEventIds().size() == expectedNumOfEvents);
    for (int i = 0; i < expectedNumOfEvents; i++) {
      Assert.assertEquals(expectedStartEventId + i, response.getEventIds().get(i).longValue());
    }

    // Get notifications from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(expectedStartEventId-1, 0, null);
    assertEquals(expectedNumOfEvents, rsp.getEventsSize());
    for (int i=0; i<expectedNumOfEvents; i++) {
      NotificationEvent event = rsp.getEvents().get(i);
      assertEquals(expectedStartEventId+i, event.getEventId());
      assertTrue(event.getEventTime() >= startTime);
      assertEquals(EventType.INSERT.toString(), event.getEventType());
      assertEquals(defaultDbName, event.getDbName());
      assertEquals(tblName, event.getTableName());
      // Parse the message field
      verifyInsert(event, defaultDbName, tblName);
      InsertMessage insertMessage = md.getInsertMessage(event.getMessage());
      List<String> ptnValues = insertMessage.getPtnObj().getValues();
      assertFalse(insertMessage.isReplace());
      assertEquals(partKeyVals.get(i), ptnValues);
    }
  }


  @Test
  public void getOnlyMaxEvents() throws Exception {
    Database db = new Database("db1", "no description", testTempDir, emptyParameters);
    msClient.createDatabase(db);
    db = new Database("db2", "no description", testTempDir, emptyParameters);
    msClient.createDatabase(db);
    db = new Database("db3", "no description", testTempDir, emptyParameters);
    msClient.createDatabase(db);

    // Get notifications from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 2, null);
    assertEquals(2, rsp.getEventsSize());
    assertEquals(firstEventId + 1, rsp.getEvents().get(0).getEventId());
    assertEquals(firstEventId + 2, rsp.getEvents().get(1).getEventId());
  }

  @Test
  public void filter() throws Exception {
    Database db = new Database("f1", "no description", testTempDir, emptyParameters);
    msClient.createDatabase(db);
    db = new Database("f2", "no description", testTempDir, emptyParameters);
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
    Database db = new Database("f10", "no description", testTempDir, emptyParameters);
    msClient.createDatabase(db);
    db = new Database("f11", "no description", testTempDir, emptyParameters);
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
    assertEquals(7, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(0);
    assertEquals(firstEventId + 1, event.getEventId());
    assertEquals(EventType.CREATE_TABLE.toString(), event.getEventType());

    event = rsp.getEvents().get(2);
    assertEquals(firstEventId + 3, event.getEventId());
    assertEquals(EventType.INSERT.toString(), event.getEventType());
    // Parse the message field
    verifyInsert(event, defaultDbName, tblName);
    InsertMessage insertMsg = md.getInsertMessage(event.getMessage());
    assertFalse(insertMsg.isReplace());

    event = rsp.getEvents().get(5);
    assertEquals(firstEventId + 6, event.getEventId());
    assertEquals(EventType.ALTER_TABLE.toString(), event.getEventType());

    event = rsp.getEvents().get(6);
    assertEquals(firstEventId + 7, event.getEventId());
    assertEquals(EventType.DROP_TABLE.toString(), event.getEventType());
    testEventCounts(defaultDbName, firstEventId, null, null, 7);
  }

  @Test
  public void sqlCTAS() throws Exception {
    String defaultDbName = "default";
    String sourceTblName = "sqlctasins1";
    String targetTblName = "sqlctasins2";
    // Event 1
    driver.run("create table " + sourceTblName + " (c int)");
    // Event 2 (alter: marker stats event), 3 (insert), 4 (alter: stats update event)
    driver.run("insert into table " + sourceTblName + " values (1)");
    // Event 5, 6 (alter), 7 (alter: stats update event)
    driver.run("create table " + targetTblName + " as select c from " + sourceTblName);

    // Get notifications from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(8, rsp.getEventsSize());
    NotificationEvent event = rsp.getEvents().get(0);
    assertEquals(firstEventId + 1, event.getEventId());
    assertEquals(EventType.CREATE_TABLE.toString(), event.getEventType());

    event = rsp.getEvents().get(2);
    assertEquals(firstEventId + 3, event.getEventId());
    assertEquals(EventType.INSERT.toString(), event.getEventType());
    // Parse the message field
    verifyInsert(event, null, sourceTblName);

    event = rsp.getEvents().get(5);
    assertEquals(firstEventId + 6, event.getEventId());
    assertEquals(EventType.CREATE_TABLE.toString(), event.getEventType());
    testEventCounts(defaultDbName, firstEventId, null, null, 8);
  }

  @Test
  public void sqlTempTable() throws Exception {
    String defaultDbName = "default";
    String tempTblName = "sqltemptbl";
    driver.run("create temporary table " + tempTblName + "  (c int)");
    driver.run("insert into table " + tempTblName + " values (1)");

    // Get notifications from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(0, rsp.getEventsSize());
    testEventCounts(defaultDbName, firstEventId, null, null, 0);
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
    String defaultDbName = "default";
    String tblName = "sqlinsptn";
    // Event 1
    driver.run("create table " + tblName + " (c int) partitioned by (ds string)");
    // Event 2, 3, 4
    driver.run("insert into table " + tblName + " partition (ds = 'today') values (1)");
    // Event 5, 6, 7
    driver.run("insert into table " + tblName + " partition (ds = 'today') values (2)");
    // Event 8, 9, 10
    driver.run("insert into table " + tblName + " partition (ds = 'today') values (3)");
    // Event 9, 10
    driver.run("alter table " + tblName + " add partition (ds = 'yesterday')");

    testEventCounts(defaultDbName, firstEventId, null, null, 13);
    // Test a limit higher than available events
    testEventCounts(defaultDbName, firstEventId, null, 100, 13);
    // Test toEventId lower than current eventId
    testEventCounts(defaultDbName, firstEventId, firstEventId + 5, null, 5);

    // Event 10, 11, 12
    driver.run("insert into table " + tblName + " partition (ds = 'yesterday') values (2)");
    // Event 12, 13, 14
    driver.run("insert into table " + tblName + " partition (ds = 'yesterday') values (3)");
    // Event 15, 16, 17
    driver.run("insert into table " + tblName + " partition (ds = 'tomorrow') values (2)");
    // Event 18
    driver.run("alter table " + tblName + " drop partition (ds = 'tomorrow')");
    // Event 19, 20, 21
    driver.run("insert into table " + tblName + " partition (ds) values (42, 'todaytwo')");
    // Event 22, 23, 24
    driver.run("insert overwrite table " + tblName + " partition(ds='todaytwo') select c from "
        + tblName + " where 'ds'='today'");

    // Get notifications from metastore
    NotificationEventResponse rsp = msClient.getNextNotification(firstEventId, 0, null);
    assertEquals(31, rsp.getEventsSize());

    NotificationEvent event = rsp.getEvents().get(1);
    assertEquals(firstEventId + 2, event.getEventId());
    assertEquals(EventType.ADD_PARTITION.toString(), event.getEventType());

    event = rsp.getEvents().get(3);
    assertEquals(firstEventId + 4, event.getEventId());
    assertEquals(EventType.UPDATE_PARTITION_COLUMN_STAT.toString(), event.getEventType());

    event = rsp.getEvents().get(4);
    assertEquals(firstEventId + 5, event.getEventId());
    assertEquals(EventType.INSERT.toString(), event.getEventType());
    // Parse the message field
    verifyInsert(event, null, tblName);
    // Verify the replace flag.
    InsertMessage insertMsg = md.getInsertMessage(event.getMessage());
    assertFalse(insertMsg.isReplace());
    event = rsp.getEvents().get(8);
    assertEquals(firstEventId + 9, event.getEventId());
    assertEquals(EventType.INSERT.toString(), event.getEventType());
    // Parse the message field
    verifyInsert(event, null, tblName);

    event = rsp.getEvents().get(12);
    assertEquals(firstEventId + 13, event.getEventId());
    assertEquals(EventType.ADD_PARTITION.toString(), event.getEventType());

    event = rsp.getEvents().get(13);
    assertEquals(firstEventId + 14, event.getEventId());
    assertEquals(EventType.INSERT.toString(), event.getEventType());
    // Parse the message field
    verifyInsert(event, null, tblName);

    event = rsp.getEvents().get(17);
    assertEquals(firstEventId + 18, event.getEventId());
    assertEquals(EventType.INSERT.toString(), event.getEventType());
    // Parse the message field
    verifyInsert(event, null, tblName);

    event = rsp.getEvents().get(21);
    assertEquals(firstEventId + 22, event.getEventId());
    assertEquals(EventType.ADD_PARTITION.toString(), event.getEventType());

    event = rsp.getEvents().get(24);
    assertEquals(firstEventId + 25, event.getEventId());
    assertEquals(EventType.DROP_PARTITION.toString(), event.getEventType());

    event = rsp.getEvents().get(25);
    assertEquals(firstEventId + 26, event.getEventId());
    assertEquals(EventType.ADD_PARTITION.toString(), event.getEventType());

    event = rsp.getEvents().get(26);
    assertEquals(firstEventId + 27, event.getEventId());
    assertEquals(EventType.ALTER_PARTITION.toString(), event.getEventType());
    assertTrue(event.getMessage().matches(".*\"ds\":\"todaytwo\".*"));

    // Test fromEventId different from the very first
    testEventCounts(defaultDbName, event.getEventId(), null, null, 4);

    event = rsp.getEvents().get(28);
    assertEquals(firstEventId + 29, event.getEventId());
    assertEquals(EventType.INSERT.toString(), event.getEventType());
    // Verify the replace flag.
    insertMsg = md.getInsertMessage(event.getMessage());
    assertTrue(insertMsg.isReplace());
    // replace-overwrite introduces no new files
    // the insert overwrite creates an empty file with the current change
    //assertTrue(event.getMessage().matches(".*\"files\":\\[\\].*"));

    event = rsp.getEvents().get(29);
    assertEquals(firstEventId + 30, event.getEventId());
    assertEquals(EventType.ALTER_PARTITION.toString(), event.getEventType());
    assertTrue(event.getMessage().matches(".*\"ds\":\"todaytwo\".*"));

    event = rsp.getEvents().get(30);
    assertEquals(firstEventId + 31, event.getEventId());
    assertEquals(EventType.ALTER_PARTITION.toString(), event.getEventType());
    assertTrue(event.getMessage().matches(".*\"ds\":\"todaytwo\".*"));
    testEventCounts(defaultDbName, firstEventId, null, null, 31);

    // Test a limit within the available events
    testEventCounts(defaultDbName, firstEventId, null, 10, 10);
    // Test toEventId greater than current eventId
    testEventCounts(defaultDbName, firstEventId, firstEventId + 100, null, 31);
    // Test toEventId greater than current eventId with some limit within available events
    testEventCounts(defaultDbName, firstEventId, firstEventId + 100, 10, 10);
    // Test toEventId greater than current eventId with some limit beyond available events
    testEventCounts(defaultDbName, firstEventId, firstEventId + 100, 50, 31);
  }

  private void verifyInsert(NotificationEvent event, String dbName, String tblName) throws Exception {
    // Parse the message field
    InsertMessage insertMsg = md.getInsertMessage(event.getMessage());
    System.out.println("InsertMessage: " + insertMsg.toString());
    if (dbName != null ){
      assertEquals(dbName, insertMsg.getTableObj().getDbName());
    }
    if (tblName != null){
      assertEquals(tblName, insertMsg.getTableObj().getTableName());
    }
    // Should have files
    Iterator<String> files = insertMsg.getFiles().iterator();
    assertTrue(files.hasNext());
  }

  /**
   * The method creates some events in notification log and fetches the events
   * based on the fields set on the NotificationEventRequest object. It includes
   * setting database name, table name(s), event skip list (i.e., filter out events
   * that are not required). These fields are optional.
   * @throws Exception
   */
  @Test
  public void fetchNotificationEventBasedOnTables() throws Exception {
    String dbName = "default";
    String table1 = "test_tbl1";
    String table2 = "test_tbl2";
    String table3 = "test_tbl3";
    // Generate some table events
    generateSometableEvents(dbName, table1);
    generateSometableEvents(dbName, table2);
    generateSometableEvents(dbName, table3);

    // Verify events by table names
    NotificationEventRequest request = new NotificationEventRequest();
    request.setLastEvent(firstEventId);
    request.setMaxEvents(-1);
    request.setDbName(dbName);
    request.setTableNames(Arrays.asList(table1));
    NotificationEventResponse rsp1 = msClient.getNextNotification(request, true, null);
    assertEquals(12, rsp1.getEventsSize());
    request.setTableNames(Arrays.asList(table1, table2));
    request.setEventTypeSkipList(Arrays.asList("CREATE_TABLE"));
    NotificationEventResponse rsp2 = msClient.getNextNotification(request, true, null);
    // The actual count of events should 24. Having CREATE_TABLE event in the event skip
    // list will result in events count reduced 22 as it skips fetching 2 create_table events
    // associated with two different tables.
    assertEquals(22, rsp2.getEventsSize());
    request.unsetTableNames();
    request.unsetEventTypeSkipList();
    NotificationEventResponse rsp3 = msClient.getNextNotification(request, true, null);
    assertEquals(36, rsp3.getEventsSize());

    NotificationEventsCountRequest eventsReq = new NotificationEventsCountRequest(firstEventId, dbName);
    eventsReq.setTableNames(Arrays.asList(table1));
    assertEquals(12, msClient.getNotificationEventsCount(eventsReq).getEventsCount());
    eventsReq.setTableNames(Arrays.asList(table1, table2));
    assertEquals(24, msClient.getNotificationEventsCount(eventsReq).getEventsCount());
    eventsReq.unsetTableNames();
    assertEquals(36, msClient.getNotificationEventsCount(eventsReq).getEventsCount());
  }

  private void generateSometableEvents(String dbName, String tableName) throws Exception {
    // CREATE_DATABASE event is generated but we filter this out while fetching events.
    driver.run("create database if not exists "+dbName);
    driver.run("use "+dbName);
    // Event 1: CREATE_TABLE event
    driver.run("create table " + tableName + " (c int) partitioned by (ds string)");
    // Event 2: ADD_PARTITION, 3: ALTER_PARTITION, 4: UPDATE_PART_COL_STAT_EVENT events
    driver.run("insert into table " + tableName + " partition (ds = 'today') values (1)");
    // Event 5: INSERT, 6: ALTER_PARTITION, 7: UPDATE_PART_COL_STAT_EVENT events
    driver.run("insert into table " + tableName + " partition (ds = 'today') values (2)");
    // Event 8: INSERT, 9: ALTER_PARTITION, 10: UPDATE_PART_COL_STAT_EVENT events
    driver.run("insert into table " + tableName + " partition (ds) values (3, 'today')");
    // Event 11: ADD_PARTITION, Event 12: ALTER_PARTITION events
    driver.run("alter table " + tableName + " add partition (ds = 'yesterday')");
  }
}
