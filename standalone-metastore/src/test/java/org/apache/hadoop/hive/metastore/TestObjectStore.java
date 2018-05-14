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


import com.codahale.metrics.Counter;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.metastore.ObjectStore.RetryingExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.ISchemaBranch;
import org.apache.hadoop.hive.metastore.api.ISchemaVersion;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.ISchemaBranch;
import org.apache.hadoop.hive.metastore.api.SchemaCompatibility;
import org.apache.hadoop.hive.metastore.api.SchemaType;
import org.apache.hadoop.hive.metastore.api.SchemaValidation;
import org.apache.hadoop.hive.metastore.api.SchemaVersionState;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.SchemaVersionBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.model.MNotificationLog;
import org.apache.hadoop.hive.metastore.model.MNotificationNextId;
import org.apache.hadoop.hive.registry.serdes.avro.AvroSchemaProvider;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jdo.Query;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Category(MetastoreUnitTest.class)
public class TestObjectStore {
  private ObjectStore objectStore = null;

  private static final String DB1 = "testobjectstoredb1";
  private static final String DB2 = "testobjectstoredb2";
  private static final String SCHEMA_DB = "testschemadb";
  private static final String TABLE1 = "testobjectstoretable1";
  private static final String KEY1 = "testobjectstorekey1";
  private static final String KEY2 = "testobjectstorekey2";
  private static final String OWNER = "testobjectstoreowner";
  private static final String USER1 = "testobjectstoreuser1";
  private static final String ROLE1 = "testobjectstorerole1";
  private static final String ROLE2 = "testobjectstorerole2";
  private static final Logger LOG = LoggerFactory.getLogger(TestObjectStore.class.getName());

  private static final class LongSupplier implements Supplier<Long> {
    public long value = 0;

    @Override
    public Long get() {
      return value;
    }
  }

  @Before
  public void setUp() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetaStoreTestUtils.setConfForStandloneMode(conf);

    objectStore = new ObjectStore();
    objectStore.setConf(conf);
    dropAllStoreObjects(objectStore);
  }

  /**
   * Test database operations
   */
  @Test
  public void testDatabaseOps() throws MetaException, InvalidObjectException,
      NoSuchObjectException {
    Database db1 = new Database(DB1, "description", "locationurl", null);
    Database db2 = new Database(DB2, "description", "locationurl", null);
    objectStore.createDatabase(db1);
    objectStore.createDatabase(db2);

    List<String> databases = objectStore.getAllDatabases();
    LOG.info("databases: " + databases);
    Assert.assertEquals(2, databases.size());
    Assert.assertEquals(DB1, databases.get(0));
    Assert.assertEquals(DB2, databases.get(1));

    objectStore.dropDatabase(DB1);
    databases = objectStore.getAllDatabases();
    Assert.assertEquals(1, databases.size());
    Assert.assertEquals(DB2, databases.get(0));

    objectStore.dropDatabase(DB2);
  }

  /**
   * Test table operations
   */
  @Test
  public void testTableOps() throws MetaException, InvalidObjectException, NoSuchObjectException,
      InvalidInputException {
    Database db1 = new Database(DB1, "description", "locationurl", null);
    objectStore.createDatabase(db1);
    StorageDescriptor sd1 =
        new StorageDescriptor(ImmutableList.of(new FieldSchema("pk_col", "double", null)),
            "location", null, null, false, 0, new SerDeInfo("SerDeName", "serializationLib", null),
            null, null, null);
    HashMap<String, String> params = new HashMap<>();
    params.put("EXTERNAL", "false");
    Table tbl1 =
        new Table(TABLE1, DB1, "owner", 1, 2, 3, sd1, null, params, null, null, "MANAGED_TABLE");
    objectStore.createTable(tbl1);

    List<String> tables = objectStore.getAllTables(DB1);
    Assert.assertEquals(1, tables.size());
    Assert.assertEquals(TABLE1, tables.get(0));

    StorageDescriptor sd2 =
        new StorageDescriptor(ImmutableList.of(new FieldSchema("fk_col", "double", null)),
            "location", null, null, false, 0, new SerDeInfo("SerDeName", "serializationLib", null),
            null, null, null);
    Table newTbl1 = new Table("new" + TABLE1, DB1, "owner", 1, 2, 3, sd2, null, params, null, null,
        "MANAGED_TABLE");
    objectStore.alterTable(DB1, TABLE1, newTbl1);
    tables = objectStore.getTables(DB1, "new*");
    Assert.assertEquals(1, tables.size());
    Assert.assertEquals("new" + TABLE1, tables.get(0));

    objectStore.createTable(tbl1);
    tables = objectStore.getAllTables(DB1);
    Assert.assertEquals(2, tables.size());

    List<SQLForeignKey> foreignKeys = objectStore.getForeignKeys(DB1, TABLE1, null, null);
    Assert.assertEquals(0, foreignKeys.size());

    SQLPrimaryKey pk = new SQLPrimaryKey(DB1, TABLE1, "pk_col", 1,
        "pk_const_1", false, false, false);
    objectStore.addPrimaryKeys(ImmutableList.of(pk));
    SQLForeignKey fk = new SQLForeignKey(DB1, TABLE1, "pk_col",
        DB1, "new" + TABLE1, "fk_col", 1,
        0, 0, "fk_const_1", "pk_const_1", false, false, false);
    objectStore.addForeignKeys(ImmutableList.of(fk));

    // Retrieve from PK side
    foreignKeys = objectStore.getForeignKeys(null, null, DB1, "new" + TABLE1);
    Assert.assertEquals(1, foreignKeys.size());

    List<SQLForeignKey> fks = objectStore.getForeignKeys(null, null, DB1, "new" + TABLE1);
    if (fks != null) {
      for (SQLForeignKey fkcol : fks) {
        objectStore.dropConstraint(fkcol.getFktable_db(), fkcol.getFktable_name(),
            fkcol.getFk_name());
      }
    }
    // Retrieve from FK side
    foreignKeys = objectStore.getForeignKeys(DB1, TABLE1, null, null);
    Assert.assertEquals(0, foreignKeys.size());
    // Retrieve from PK side
    foreignKeys = objectStore.getForeignKeys(null, null, DB1, "new" + TABLE1);
    Assert.assertEquals(0, foreignKeys.size());

    objectStore.dropTable(DB1, TABLE1);
    tables = objectStore.getAllTables(DB1);
    Assert.assertEquals(1, tables.size());

    objectStore.dropTable(DB1, "new" + TABLE1);
    tables = objectStore.getAllTables(DB1);
    Assert.assertEquals(0, tables.size());

    objectStore.dropDatabase(DB1);
  }

  private StorageDescriptor createFakeSd(String location) {
    return new StorageDescriptor(null, location, null, null, false, 0,
        new SerDeInfo("SerDeName", "serializationLib", null), null, null, null);
  }


  /**
   * Tests partition operations
   */
  @Test
  public void testPartitionOps() throws MetaException, InvalidObjectException,
      NoSuchObjectException, InvalidInputException {
    Database db1 = new Database(DB1, "description", "locationurl", null);
    objectStore.createDatabase(db1);
    StorageDescriptor sd = createFakeSd("location");
    HashMap<String, String> tableParams = new HashMap<>();
    tableParams.put("EXTERNAL", "false");
    FieldSchema partitionKey1 = new FieldSchema("Country", ColumnType.STRING_TYPE_NAME, "");
    FieldSchema partitionKey2 = new FieldSchema("State", ColumnType.STRING_TYPE_NAME, "");
    Table tbl1 =
        new Table(TABLE1, DB1, "owner", 1, 2, 3, sd, Arrays.asList(partitionKey1, partitionKey2),
            tableParams, null, null, "MANAGED_TABLE");
    objectStore.createTable(tbl1);
    HashMap<String, String> partitionParams = new HashMap<>();
    partitionParams.put("PARTITION_LEVEL_PRIVILEGE", "true");
    List<String> value1 = Arrays.asList("US", "CA");
    Partition part1 = new Partition(value1, DB1, TABLE1, 111, 111, sd, partitionParams);
    objectStore.addPartition(part1);
    List<String> value2 = Arrays.asList("US", "MA");
    Partition part2 = new Partition(value2, DB1, TABLE1, 222, 222, sd, partitionParams);
    objectStore.addPartition(part2);

    Deadline.startTimer("getPartition");
    List<Partition> partitions = objectStore.getPartitions(DB1, TABLE1, 10);
    Assert.assertEquals(2, partitions.size());
    Assert.assertEquals(111, partitions.get(0).getCreateTime());
    Assert.assertEquals(222, partitions.get(1).getCreateTime());

    int numPartitions = objectStore.getNumPartitionsByFilter(DB1, TABLE1, "");
    Assert.assertEquals(partitions.size(), numPartitions);

    numPartitions = objectStore.getNumPartitionsByFilter(DB1, TABLE1, "country = \"US\"");
    Assert.assertEquals(2, numPartitions);

    objectStore.dropPartition(DB1, TABLE1, value1);
    partitions = objectStore.getPartitions(DB1, TABLE1, 10);
    Assert.assertEquals(1, partitions.size());
    Assert.assertEquals(222, partitions.get(0).getCreateTime());

    objectStore.dropPartition(DB1, TABLE1, value2);
    objectStore.dropTable(DB1, TABLE1);
    objectStore.dropDatabase(DB1);
  }

  /**
   * Test master keys operation
   */
  @Test
  public void testMasterKeyOps() throws MetaException, NoSuchObjectException {
    int id1 = objectStore.addMasterKey(KEY1);
    int id2 = objectStore.addMasterKey(KEY2);

    String[] keys = objectStore.getMasterKeys();
    Assert.assertEquals(2, keys.length);
    Assert.assertEquals(KEY1, keys[0]);
    Assert.assertEquals(KEY2, keys[1]);

    objectStore.updateMasterKey(id1, "new" + KEY1);
    objectStore.updateMasterKey(id2, "new" + KEY2);
    keys = objectStore.getMasterKeys();
    Assert.assertEquals(2, keys.length);
    Assert.assertEquals("new" + KEY1, keys[0]);
    Assert.assertEquals("new" + KEY2, keys[1]);

    objectStore.removeMasterKey(id1);
    keys = objectStore.getMasterKeys();
    Assert.assertEquals(1, keys.length);
    Assert.assertEquals("new" + KEY2, keys[0]);

    objectStore.removeMasterKey(id2);
  }

  /**
   * Test role operation
   */
  @Test
  public void testRoleOps() throws InvalidObjectException, MetaException, NoSuchObjectException {
    objectStore.addRole(ROLE1, OWNER);
    objectStore.addRole(ROLE2, OWNER);
    List<String> roles = objectStore.listRoleNames();
    Assert.assertEquals(2, roles.size());
    Assert.assertEquals(ROLE2, roles.get(1));
    Role role1 = objectStore.getRole(ROLE1);
    Assert.assertEquals(OWNER, role1.getOwnerName());
    objectStore.grantRole(role1, USER1, PrincipalType.USER, OWNER, PrincipalType.ROLE, true);
    objectStore.revokeRole(role1, USER1, PrincipalType.USER, false);
    objectStore.removeRole(ROLE1);
  }

  @Test
  public void testDirectSqlErrorMetrics() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED, true);
    Metrics.initialize(conf);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.HIVE_CODAHALE_METRICS_REPORTER_CLASSES,
        "org.apache.hadoop.hive.common.metrics.metrics2.JsonFileMetricsReporter, " +
            "org.apache.hadoop.hive.common.metrics.metrics2.JmxMetricsReporter"
    );

    // recall setup so that we get an object store with the metrics initalized
    setUp();
    Counter directSqlErrors =
        Metrics.getRegistry().getCounters().get(MetricsConstants.DIRECTSQL_ERRORS);

    objectStore.new GetDbHelper("foo", true, true) {
      @Override
      protected Database getSqlResult(ObjectStore.GetHelper<Database> ctx) throws MetaException {
        return null;
      }

      @Override
      protected Database getJdoResult(ObjectStore.GetHelper<Database> ctx) throws MetaException,
          NoSuchObjectException {
        return null;
      }
    }.run(false);

    Assert.assertEquals(0, directSqlErrors.getCount());

    objectStore.new GetDbHelper("foo", true, true) {
      @Override
      protected Database getSqlResult(ObjectStore.GetHelper<Database> ctx) throws MetaException {
        throw new RuntimeException();
      }

      @Override
      protected Database getJdoResult(ObjectStore.GetHelper<Database> ctx) throws MetaException,
          NoSuchObjectException {
        return null;
      }
    }.run(false);

    Assert.assertEquals(1, directSqlErrors.getCount());
  }

  private static void dropAllStoreObjects(RawStore store)
      throws MetaException, InvalidObjectException, InvalidInputException {
    try {
      Deadline.registerIfNot(100000);
      List<Function> functions = store.getAllFunctions();
      for (Function func : functions) {
        store.dropFunction(func.getDbName(), func.getFunctionName());
      }
      List<String> dbs = store.getAllDatabases();
      for (String db : dbs) {
        List<String> tbls = store.getAllTables(db);
        for (String tbl : tbls) {
          List<Index> indexes = store.getIndexes(db, tbl, 100);
          for (Index index : indexes) {
            store.dropIndex(db, tbl, index.getIndexName());
          }
        }
        for (String tbl : tbls) {
          Deadline.startTimer("getPartition");
          List<Partition> parts = store.getPartitions(db, tbl, 100);
          for (Partition part : parts) {
            store.dropPartition(db, tbl, part.getValues());
          }
          // Find any constraints and drop them
          Set<String> constraints = new HashSet<>();
          List<SQLPrimaryKey> pk = store.getPrimaryKeys(db, tbl);
          if (pk != null) {
            for (SQLPrimaryKey pkcol : pk) {
              constraints.add(pkcol.getPk_name());
            }
          }
          List<SQLForeignKey> fks = store.getForeignKeys(null, null, db, tbl);
          if (fks != null) {
            for (SQLForeignKey fkcol : fks) {
              constraints.add(fkcol.getFk_name());
            }
          }
          for (String constraint : constraints) {
            store.dropConstraint(db, tbl, constraint);
          }
          store.dropTable(db, tbl);
        }
        store.dropDatabase(db);
      }
      List<String> roles = store.listRoleNames();
      for (String role : roles) {
        store.removeRole(role);
      }
    } catch (NoSuchObjectException e) {
    }
  }

  @Test
  public void testQueryCloseOnError() throws Exception {
    ObjectStore spy = Mockito.spy(objectStore);
    spy.getAllDatabases();
    spy.getAllFunctions();
    spy.getAllTables(DB1);
    spy.getPartitionCount();
    Mockito.verify(spy, Mockito.times(2))
        .rollbackAndCleanup(Mockito.anyBoolean(), Mockito.<Query>anyObject());
  }

  @Test
  public void testRetryingExecutorSleep() throws Exception {
    RetryingExecutor re = new ObjectStore.RetryingExecutor(MetastoreConf.newMetastoreConf(), null);
    Assert.assertTrue("invalid sleep value", re.getSleepInterval() >= 0);
  }

  @Ignore // See comment in ObjectStore.getDataSourceProps
  @Test
  public void testNonConfDatanucleusValueSet() {
    String key = "datanucleus.no.such.key";
    String value = "test_value";
    String key1 = "blabla.no.such.key";
    String value1 = "another_value";
    Assume.assumeTrue(System.getProperty(key) == null);
    Configuration localConf = MetastoreConf.newMetastoreConf();
    MetaStoreTestUtils.setConfForStandloneMode(localConf);
    localConf.set(key, value);
    localConf.set(key1, value1);
    objectStore = new ObjectStore();
    objectStore.setConf(localConf);
    Assert.assertEquals(value, objectStore.getProp().getProperty(key));
    Assert.assertNull(objectStore.getProp().getProperty(key1));
  }

  /**
   * Test notification operations
   */
  // TODO MS-SPLIT uncomment once we move EventMessage over
  @Test
  public void testNotificationOps() throws InterruptedException {
    final int NO_EVENT_ID = 0;
    final int FIRST_EVENT_ID = 1;
    final int SECOND_EVENT_ID = 2;

    NotificationEvent event =
        new NotificationEvent(0, 0, EventMessage.EventType.CREATE_DATABASE.toString(), "");
    NotificationEventResponse eventResponse;
    CurrentNotificationEventId eventId;

    // Verify that there is no notifications available yet
    eventId = objectStore.getCurrentNotificationEventId();
    Assert.assertEquals(NO_EVENT_ID, eventId.getEventId());

    // Verify that addNotificationEvent() updates the NotificationEvent with the new event ID
    objectStore.addNotificationEvent(event);
    Assert.assertEquals(FIRST_EVENT_ID, event.getEventId());
    objectStore.addNotificationEvent(event);
    Assert.assertEquals(SECOND_EVENT_ID, event.getEventId());

    // Verify that objectStore fetches the latest notification event ID
    eventId = objectStore.getCurrentNotificationEventId();
    Assert.assertEquals(SECOND_EVENT_ID, eventId.getEventId());

    // Verify that getNextNotification() returns all events
    eventResponse = objectStore.getNextNotification(new NotificationEventRequest());
    Assert.assertEquals(2, eventResponse.getEventsSize());
    Assert.assertEquals(FIRST_EVENT_ID, eventResponse.getEvents().get(0).getEventId());
    Assert.assertEquals(SECOND_EVENT_ID, eventResponse.getEvents().get(1).getEventId());

    // Verify that getNextNotification(last) returns events after a specified event
    eventResponse = objectStore.getNextNotification(new NotificationEventRequest(FIRST_EVENT_ID));
    Assert.assertEquals(1, eventResponse.getEventsSize());
    Assert.assertEquals(SECOND_EVENT_ID, eventResponse.getEvents().get(0).getEventId());

    // Verify that getNextNotification(last) returns zero events if there are no more notifications available
    eventResponse = objectStore.getNextNotification(new NotificationEventRequest(SECOND_EVENT_ID));
    Assert.assertEquals(0, eventResponse.getEventsSize());

    // Verify that cleanNotificationEvents() cleans up all old notifications
    Thread.sleep(1);
    objectStore.cleanNotificationEvents(1);
    eventResponse = objectStore.getNextNotification(new NotificationEventRequest());
    Assert.assertEquals(0, eventResponse.getEventsSize());
  }

  @Ignore(
      "This test is here to allow testing with other databases like mysql / postgres etc\n"
          + " with  user changes to the code. This cannot be run on apache derby because of\n"
          + " https://db.apache.org/derby/docs/10.10/devguide/cdevconcepts842385.html"
  )
  @Test
  public void testConcurrentAddNotifications() throws ExecutionException, InterruptedException {

    final int NUM_THREADS = 10;
    CyclicBarrier cyclicBarrier = new CyclicBarrier(NUM_THREADS,
        () -> LoggerFactory.getLogger("test")
            .debug(NUM_THREADS + " threads going to add notification"));

    Configuration conf = MetastoreConf.newMetastoreConf();
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    /*
       Below are the properties that need to be set based on what database this test is going to be run
     */

//    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER, "com.mysql.jdbc.Driver");
//    conf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY,
//        "jdbc:mysql://localhost:3306/metastore_db");
//    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME, "");
//    conf.setVar(HiveConf.ConfVars.METASTOREPWD, "");

    /*
     we have to  add this one manually as for tests the db is initialized via the metastoreDiretSQL
     and we don't run the schema creation sql that includes the an insert for notification_sequence
     which can be locked. the entry in notification_sequence happens via notification_event insertion.
    */
    objectStore.getPersistenceManager().newQuery(MNotificationLog.class, "eventType==''").execute();
    objectStore.getPersistenceManager().newQuery(MNotificationNextId.class, "nextEventId==-1").execute();

    objectStore.addNotificationEvent(
        new NotificationEvent(0, 0,
            EventMessage.EventType.CREATE_DATABASE.toString(),
            "CREATE DATABASE DB initial"));

    ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
    for (int i = 0; i < NUM_THREADS; i++) {
      final int n = i;

      executorService.execute(
          () -> {
            ObjectStore store = new ObjectStore();
            store.setConf(conf);

            String eventType = EventMessage.EventType.CREATE_DATABASE.toString();
            NotificationEvent dbEvent =
                new NotificationEvent(0, 0, eventType,
                    "CREATE DATABASE DB" + n);
            System.out.println("ADDING NOTIFICATION");

            try {
              cyclicBarrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
              throw new RuntimeException(e);
            }
            store.addNotificationEvent(dbEvent);
            System.out.println("FINISH NOTIFICATION");
          });
    }
    executorService.shutdown();
    Assert.assertTrue(executorService.awaitTermination(15, TimeUnit.SECONDS));

    // we have to setup this again as the underlying PMF keeps getting reinitialized with original
    // reference closed
    ObjectStore store = new ObjectStore();
    store.setConf(conf);

    NotificationEventResponse eventResponse = store.getNextNotification(
        new NotificationEventRequest());
    Assert.assertEquals(NUM_THREADS + 1, eventResponse.getEventsSize());
    long previousId = 0;
    for (NotificationEvent event : eventResponse.getEvents()) {
      Assert.assertTrue("previous:" + previousId + " current:" + event.getEventId(),
          previousId < event.getEventId());
      Assert.assertTrue(previousId + 1 == event.getEventId());
      previousId = event.getEventId();
    }
  }

  @Test
  public void testSchemaOperations() throws Exception {
    Database db1 = new Database(SCHEMA_DB, "description", "locationurl", null);
    objectStore.createDatabase(db1);
    AvroSchemaProvider avroSchemaProvider = new AvroSchemaProvider();

    // create a Schema Metadata
    String schemaName = "test-schema";
    String schemaDesc = "test-desc";
    String schemaGroup = "test-group";
    ISchema iSchema = new ISchema();
    iSchema.setName(schemaName);
    iSchema.setDescription(schemaDesc);
    iSchema.setCompatibility(SchemaCompatibility.BACKWARD);
    iSchema.setCanEvolve(true);
    iSchema.setSchemaGroup(schemaGroup);
    iSchema.setSchemaType(SchemaType.AVRO);
    iSchema.setValidationLevel(SchemaValidation.LATEST);
    iSchema.setDbName(SCHEMA_DB);
    long schemaId = objectStore.createISchema(iSchema);
    ISchema nSchema = objectStore.getISchemaByName(schemaName);
    Assert.assertEquals(new Long(schemaId), new Long(nSchema.getSchemaId()));
    Assert.assertEquals(nSchema.getName(), schemaName);
    Assert.assertEquals(nSchema.getDescription(), schemaDesc);
    Assert.assertEquals(nSchema.getSchemaId(), schemaId);
    Assert.assertEquals(nSchema.getSchemaGroup(), schemaGroup);
    //Alter Schema
    nSchema.setDescription("test-desc-modified");
    nSchema.setName("test-schema-modified");
    nSchema.setDbName(SCHEMA_DB);
    objectStore.alterISchema("test-schema", nSchema);
    Assert.assertEquals(nSchema.getSchemaId(), schemaId);
    Assert.assertEquals(nSchema.getName(), "test-schema-modified");
    Assert.assertEquals(nSchema.getDescription(), "test-desc-modified");

    // Add Schema Version

    String description = "Sample schema for testing";
    String schemaText = "{\n" +
        "  \"type\" : \"record\",\n" +
        "  \"namespace\" : \"org.apache.hadoop.hive.metastore.registries\",\n" +
        "  \"name\" : \"trucks\",\n" +
        "  \"fields\" : [\n" +
        "    { \"name\" : \"driverId\" , \"type\" : \"int\" },\n" +
        "    { \"name\" : \"truckId\" , \"type\" : \"int\" }" +
        "  ]\n" +
        "}";
    String fingerprint = new String(avroSchemaProvider.getFingerprint(schemaText));
    String versionName = "first version";
    long creationTime = 10;
    String serdeName = "serde_for_schema37";
    String serializer = "org.apache.hadoop.hive.metastore.test.Serializer";
    String deserializer = "org.apache.hadoop.hive.metastore.test.Deserializer";
    String serdeDescription = "serdes for sample schema";
    int version = 1;
    ISchemaVersion schemaVersion = new SchemaVersionBuilder()
            .setSchemaName(schemaName)
            .setVersion(version)
            .addCol("driverId", ColumnType.INT_TYPE_NAME)
            .addCol("truckId", ColumnType.INT_TYPE_NAME)
            .setCreatedAt(creationTime)
            .setState(SchemaVersionState.INITIATED)
            .setDescription(description)
            .setSchemaText(schemaText)
            .setFingerprint(fingerprint)
            .setName(versionName)
            .setSerdeName(serdeName)
            .setSerdeSerializerClass(serializer)
            .setSerdeDeserializerClass(deserializer)
            .setSerdeDescription(serdeDescription)
            .build();
    Long schemaVersionId = objectStore.addSchemaVersion(schemaVersion);
    ISchemaVersion nSchemaVersion = objectStore.getSchemaVersion(schemaName, 1);
    Assert.assertEquals(new Long(nSchemaVersion.getSchemaVersionId()), schemaVersionId);
    Assert.assertEquals(nSchemaVersion.getSchemaText(), schemaText);
    Assert.assertEquals(nSchemaVersion.getFingerprint(), fingerprint);

    // Add Schema Version

    String schemaWithDefaults = "{\n" +
        "  \"type\" : \"record\",\n" +
        "  \"namespace\" : \"com.hortonworks.registries\",\n" +
        "  \"name\" : \"trucks\",\n" +
        "  \"fields\" : [\n" +
        "    { \"name\" : \"driverId\" , \"type\" : \"int\" },\n" +
        "    { \"name\" : \"truckId\" , \"type\" : \"int\", \"default\":0 }\n" +
        "  ]\n" +
        "}\n";
    String fingerprintSecondVersion = new String(avroSchemaProvider.getFingerprint(schemaWithDefaults));
    String versionNameSecondVersion = "second version";
    long creationTimeSecondVersion = 11;
    int versionSecondVersion = 2;
    ISchemaVersion schemaVersionSecondVersion = new SchemaVersionBuilder()
        .setSchemaName(schemaName)
        .setVersion(versionSecondVersion)
        .addCol("driverId", ColumnType.INT_TYPE_NAME)
        .addCol("truckId", ColumnType.INT_TYPE_NAME)
        .setCreatedAt(creationTimeSecondVersion)
        .setState(SchemaVersionState.INITIATED)
        .setDescription(description)
        .setSchemaText(schemaWithDefaults)
        .setFingerprint(fingerprintSecondVersion)
        .setName(versionNameSecondVersion)
        .setSerdeName(serdeName)
        .setSerdeSerializerClass(serializer)
        .setSerdeDeserializerClass(deserializer)
        .setSerdeDescription(serdeDescription)
        .build();
    Long schemaVersionIdSecondVersion = objectStore.addSchemaVersion(schemaVersionSecondVersion);
    ISchemaVersion nSchemaVersionSecondVersion = objectStore.getSchemaVersion(schemaName, 2);
    Assert.assertEquals(new Long(nSchemaVersionSecondVersion.getSchemaVersionId()), schemaVersionIdSecondVersion);
    Assert.assertEquals(nSchemaVersionSecondVersion.getSchemaText(), schemaWithDefaults);
    Assert.assertEquals(nSchemaVersionSecondVersion.getFingerprint(), fingerprintSecondVersion);

    // Get Latest SchemaVersion
    ISchemaVersion latestSchemaVersion = objectStore.getLatestSchemaVersion(schemaName);
    Assert.assertEquals(new Long(latestSchemaVersion.getSchemaVersionId()), schemaVersionIdSecondVersion);
    Assert.assertEquals(latestSchemaVersion.getSchemaText(), schemaWithDefaults);
    Assert.assertEquals(latestSchemaVersion.getFingerprint(), fingerprintSecondVersion);
    Assert.assertEquals(latestSchemaVersion.getVersion(), 2);
    Assert.assertEquals(latestSchemaVersion.getSchemaName(), schemaName);

    //Get all SchemaVersions
    List<ISchemaVersion> schemaVersions = objectStore.getAllSchemaVersion(schemaName);
    Assert.assertEquals(schemaVersions.size(), 2);

    // drop schemaVersion
    objectStore.dropSchemaVersion(schemaName, 2);
    List<ISchemaVersion> dropSchemaVersions = objectStore.getAllSchemaVersion(schemaName);
    Assert.assertEquals(dropSchemaVersions.size(), 1);
  }

  @Test
  public void testSchemaBranchOperations() throws Exception {
    Database db1 = new Database(SCHEMA_DB, "description", "locationurl", null);
    objectStore.createDatabase(db1);
    String schemaName = "test-schema";
    String branchName = "master";
    String branchDesc = "master branch";
    Long firstSchemaVersionId= 1L;
    ISchemaBranch schemaBranch = new ISchemaBranch();
    schemaBranch.setName(branchName);
    schemaBranch.setDescription(branchDesc);
    schemaBranch.setSchemaMetadataName(schemaName);
    Long schemaBranchId = objectStore.addSchemaBranch(schemaBranch);

    ISchemaBranch nSchemaBranch = objectStore.getSchemaBranch(schemaBranchId);
    Assert.assertEquals(schemaBranchId, new Long(nSchemaBranch.getSchemaBranchId()));
    Assert.assertEquals(branchName, nSchemaBranch.getName());
    Assert.assertEquals(branchDesc, nSchemaBranch.getDescription());
    Assert.assertEquals(schemaName, nSchemaBranch.getSchemaMetadataName());
    objectStore.mapSchemaBranchToSchemaVersion(schemaBranchId, firstSchemaVersionId);

    List<ISchemaBranch> schemaBranchesBySchemaVersionId = objectStore.getSchemaBranchBySchemaVersionId(firstSchemaVersionId);
    Assert.assertEquals(schemaBranchesBySchemaVersionId.size(), 1);
    ISchemaBranch schemaBranchFromSchemaVersion = schemaBranchesBySchemaVersionId.get(0);
    Assert.assertEquals(schemaBranchFromSchemaVersion.getSchemaMetadataName(), schemaName);
    Assert.assertEquals(schemaBranchFromSchemaVersion.getDescription(), branchDesc);
    Assert.assertEquals(schemaBranchFromSchemaVersion.getName(), branchName);


    ISchemaBranch schemaBranchTest = new ISchemaBranch();
    schemaBranchTest.setName("test");
    schemaBranchTest.setDescription("test branch");
    schemaBranchTest.setSchemaMetadataName(schemaName);
    Long testSchemaBranchId = objectStore.addSchemaBranch(schemaBranchTest);

    // test for retrieving all the branches for a given schemaName
    List<ISchemaBranch> branchesForSchema = objectStore.getSchemaBranchBySchemaName(schemaName);
    Assert.assertEquals(branchesForSchema.size(), 2);
    ISchemaBranch firstBranch = branchesForSchema.get(0);
    Assert.assertEquals(firstBranch.getName(), "test");
    Assert.assertEquals(firstBranch.getDescription(), "test branch");
    Assert.assertEquals(firstBranch.getSchemaMetadataName(), schemaName);


  }


}

