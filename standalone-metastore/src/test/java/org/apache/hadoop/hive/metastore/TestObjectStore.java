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
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
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
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.model.MNotificationLog;
import org.apache.hadoop.hive.metastore.model.MNotificationNextId;
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

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;

@Category(MetastoreUnitTest.class)
public class TestObjectStore {
  private ObjectStore objectStore = null;
  private Configuration conf;

  private static final String DB1 = "testobjectstoredb1";
  private static final String DB2 = "testobjectstoredb2";
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
    conf = MetastoreConf.newMetastoreConf();
    MetaStoreTestUtils.setConfForStandloneMode(conf);

    objectStore = new ObjectStore();
    objectStore.setConf(conf);
    dropAllStoreObjects(objectStore);
    HiveMetaStore.HMSHandler.createDefaultCatalog(objectStore, new Warehouse(conf));
  }

  @Test
  public void catalogs() throws MetaException, NoSuchObjectException {
    final String names[] = {"cat1", "cat2"};
    final String locations[] = {"loc1", "loc2"};
    final String descriptions[] = {"description 1", "description 2"};

    for (int i = 0; i < names.length; i++) {
      Catalog cat = new CatalogBuilder()
          .setName(names[i])
          .setLocation(locations[i])
          .setDescription(descriptions[i])
          .build();
      objectStore.createCatalog(cat);
    }

    List<String> fetchedNames = objectStore.getCatalogs();
    Assert.assertEquals(3, fetchedNames.size());
    for (int i = 0; i < names.length - 1; i++) {
      Assert.assertEquals(names[i], fetchedNames.get(i));
      Catalog cat = objectStore.getCatalog(fetchedNames.get(i));
      Assert.assertEquals(names[i], cat.getName());
      Assert.assertEquals(descriptions[i], cat.getDescription());
      Assert.assertEquals(locations[i], cat.getLocationUri());
    }
    Catalog cat = objectStore.getCatalog(fetchedNames.get(2));
    Assert.assertEquals(DEFAULT_CATALOG_NAME, cat.getName());
    Assert.assertEquals(Warehouse.DEFAULT_CATALOG_COMMENT, cat.getDescription());
    // Location will vary by system.

    for (int i = 0; i < names.length; i++) objectStore.dropCatalog(names[i]);
    fetchedNames = objectStore.getCatalogs();
    Assert.assertEquals(1, fetchedNames.size());
  }

  @Test(expected = NoSuchObjectException.class)
  public void getNoSuchCatalog() throws MetaException, NoSuchObjectException {
    objectStore.getCatalog("no_such_catalog");
  }

  @Test(expected = NoSuchObjectException.class)
  public void dropNoSuchCatalog() throws MetaException, NoSuchObjectException {
    objectStore.dropCatalog("no_such_catalog");
  }

  // TODO test dropping non-empty catalog

  /**
   * Test database operations
   */
  @Test
  public void testDatabaseOps() throws MetaException, InvalidObjectException,
      NoSuchObjectException {
    String catName = "tdo1_cat";
    createTestCatalog(catName);
    Database db1 = new Database(DB1, "description", "locationurl", null);
    Database db2 = new Database(DB2, "description", "locationurl", null);
    db1.setCatalogName(catName);
    db2.setCatalogName(catName);
    objectStore.createDatabase(db1);
    objectStore.createDatabase(db2);

    List<String> databases = objectStore.getAllDatabases(catName);
    LOG.info("databases: " + databases);
    Assert.assertEquals(2, databases.size());
    Assert.assertEquals(DB1, databases.get(0));
    Assert.assertEquals(DB2, databases.get(1));

    objectStore.dropDatabase(catName, DB1);
    databases = objectStore.getAllDatabases(catName);
    Assert.assertEquals(1, databases.size());
    Assert.assertEquals(DB2, databases.get(0));

    objectStore.dropDatabase(catName, DB2);
  }

  /**
   * Test table operations
   */
  @Test
  public void testTableOps() throws MetaException, InvalidObjectException, NoSuchObjectException,
      InvalidInputException {
    Database db1 = new DatabaseBuilder()
        .setName(DB1)
        .setDescription("description")
        .setLocation("locationurl")
        .build(conf);
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

    List<String> tables = objectStore.getAllTables(DEFAULT_CATALOG_NAME, DB1);
    Assert.assertEquals(1, tables.size());
    Assert.assertEquals(TABLE1, tables.get(0));

    StorageDescriptor sd2 =
        new StorageDescriptor(ImmutableList.of(new FieldSchema("fk_col", "double", null)),
            "location", null, null, false, 0, new SerDeInfo("SerDeName", "serializationLib", null),
            null, null, null);
    Table newTbl1 = new Table("new" + TABLE1, DB1, "owner", 1, 2, 3, sd2, null, params, null, null,
        "MANAGED_TABLE");

    // Change different fields and verify they were altered
    newTbl1.setOwner("role1");
    newTbl1.setOwnerType(PrincipalType.ROLE);

    objectStore.alterTable(DEFAULT_CATALOG_NAME, DB1, TABLE1, newTbl1);
    tables = objectStore.getTables(DEFAULT_CATALOG_NAME, DB1, "new*");
    Assert.assertEquals(1, tables.size());
    Assert.assertEquals("new" + TABLE1, tables.get(0));

    // Verify fields were altered during the alterTable operation
    Table alteredTable = objectStore.getTable(DEFAULT_CATALOG_NAME, DB1, "new" + TABLE1);
    Assert.assertEquals("Owner of table was not altered", newTbl1.getOwner(), alteredTable.getOwner());
    Assert.assertEquals("Owner type of table was not altered", newTbl1.getOwnerType(), alteredTable.getOwnerType());

    objectStore.createTable(tbl1);
    tables = objectStore.getAllTables(DEFAULT_CATALOG_NAME, DB1);
    Assert.assertEquals(2, tables.size());

    List<SQLForeignKey> foreignKeys = objectStore.getForeignKeys(DEFAULT_CATALOG_NAME, DB1, TABLE1, null, null);
    Assert.assertEquals(0, foreignKeys.size());

    SQLPrimaryKey pk = new SQLPrimaryKey(DB1, TABLE1, "pk_col", 1,
        "pk_const_1", false, false, false);
    pk.setCatName(DEFAULT_CATALOG_NAME);
    objectStore.addPrimaryKeys(ImmutableList.of(pk));
    SQLForeignKey fk = new SQLForeignKey(DB1, TABLE1, "pk_col",
        DB1, "new" + TABLE1, "fk_col", 1,
        0, 0, "fk_const_1", "pk_const_1", false, false, false);
    objectStore.addForeignKeys(ImmutableList.of(fk));

    // Retrieve from PK side
    foreignKeys = objectStore.getForeignKeys(DEFAULT_CATALOG_NAME, null, null, DB1, "new" + TABLE1);
    Assert.assertEquals(1, foreignKeys.size());

    List<SQLForeignKey> fks = objectStore.getForeignKeys(DEFAULT_CATALOG_NAME, null, null, DB1, "new" + TABLE1);
    if (fks != null) {
      for (SQLForeignKey fkcol : fks) {
        objectStore.dropConstraint(fkcol.getCatName(), fkcol.getFktable_db(), fkcol.getFktable_name(),
            fkcol.getFk_name());
      }
    }
    // Retrieve from FK side
    foreignKeys = objectStore.getForeignKeys(DEFAULT_CATALOG_NAME, DB1, TABLE1, null, null);
    Assert.assertEquals(0, foreignKeys.size());
    // Retrieve from PK side
    foreignKeys = objectStore.getForeignKeys(DEFAULT_CATALOG_NAME, null, null, DB1, "new" + TABLE1);
    Assert.assertEquals(0, foreignKeys.size());

    objectStore.dropTable(DEFAULT_CATALOG_NAME, DB1, TABLE1);
    tables = objectStore.getAllTables(DEFAULT_CATALOG_NAME, DB1);
    Assert.assertEquals(1, tables.size());

    objectStore.dropTable(DEFAULT_CATALOG_NAME, DB1, "new" + TABLE1);
    tables = objectStore.getAllTables(DEFAULT_CATALOG_NAME, DB1);
    Assert.assertEquals(0, tables.size());

    objectStore.dropDatabase(db1.getCatalogName(), DB1);
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
    Database db1 = new DatabaseBuilder()
        .setName(DB1)
        .setDescription("description")
        .setLocation("locationurl")
        .build(conf);
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
    part1.setCatName(DEFAULT_CATALOG_NAME);
    objectStore.addPartition(part1);
    List<String> value2 = Arrays.asList("US", "MA");
    Partition part2 = new Partition(value2, DB1, TABLE1, 222, 222, sd, partitionParams);
    part2.setCatName(DEFAULT_CATALOG_NAME);
    objectStore.addPartition(part2);

    Deadline.startTimer("getPartition");
    List<Partition> partitions = objectStore.getPartitions(DEFAULT_CATALOG_NAME, DB1, TABLE1, 10);
    Assert.assertEquals(2, partitions.size());
    Assert.assertEquals(111, partitions.get(0).getCreateTime());
    Assert.assertEquals(222, partitions.get(1).getCreateTime());

    int numPartitions = objectStore.getNumPartitionsByFilter(DEFAULT_CATALOG_NAME, DB1, TABLE1, "");
    Assert.assertEquals(partitions.size(), numPartitions);

    numPartitions = objectStore.getNumPartitionsByFilter(DEFAULT_CATALOG_NAME, DB1, TABLE1, "country = \"US\"");
    Assert.assertEquals(2, numPartitions);

    objectStore.dropPartition(DEFAULT_CATALOG_NAME, DB1, TABLE1, value1);
    partitions = objectStore.getPartitions(DEFAULT_CATALOG_NAME, DB1, TABLE1, 10);
    Assert.assertEquals(1, partitions.size());
    Assert.assertEquals(222, partitions.get(0).getCreateTime());

    objectStore.dropPartition(DEFAULT_CATALOG_NAME, DB1, TABLE1, value2);
    objectStore.dropTable(DEFAULT_CATALOG_NAME, DB1, TABLE1);
    objectStore.dropDatabase(db1.getCatalogName(), DB1);
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

    objectStore.new GetDbHelper(DEFAULT_CATALOG_NAME, "foo", true, true) {
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

    objectStore.new GetDbHelper(DEFAULT_CATALOG_NAME, "foo", true, true) {
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
      List<Function> functions = store.getAllFunctions(DEFAULT_CATALOG_NAME);
      for (Function func : functions) {
        store.dropFunction(DEFAULT_CATALOG_NAME, func.getDbName(), func.getFunctionName());
      }
      for (String catName : store.getCatalogs()) {
        List<String> dbs = store.getAllDatabases(catName);
        for (String db : dbs) {
          List<String> tbls = store.getAllTables(DEFAULT_CATALOG_NAME, db);
          for (String tbl : tbls) {
            Deadline.startTimer("getPartition");
            List<Partition> parts = store.getPartitions(DEFAULT_CATALOG_NAME, db, tbl, 100);
            for (Partition part : parts) {
              store.dropPartition(DEFAULT_CATALOG_NAME, db, tbl, part.getValues());
            }
            // Find any constraints and drop them
            Set<String> constraints = new HashSet<>();
            List<SQLPrimaryKey> pk = store.getPrimaryKeys(DEFAULT_CATALOG_NAME, db, tbl);
            if (pk != null) {
              for (SQLPrimaryKey pkcol : pk) {
                constraints.add(pkcol.getPk_name());
              }
            }
            List<SQLForeignKey> fks = store.getForeignKeys(DEFAULT_CATALOG_NAME, null, null, db, tbl);
            if (fks != null) {
              for (SQLForeignKey fkcol : fks) {
                constraints.add(fkcol.getFk_name());
              }
            }
            for (String constraint : constraints) {
              store.dropConstraint(DEFAULT_CATALOG_NAME, db, tbl, constraint);
            }
            store.dropTable(DEFAULT_CATALOG_NAME, db, tbl);
          }
          store.dropDatabase(catName, db);
        }
        store.dropCatalog(catName);
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
    spy.getAllDatabases(DEFAULT_CATALOG_NAME);
    spy.getAllFunctions(DEFAULT_CATALOG_NAME);
    spy.getAllTables(DEFAULT_CATALOG_NAME, DB1);
    spy.getPartitionCount();
    Mockito.verify(spy, Mockito.times(3))
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
  public void testNotificationOps() throws InterruptedException, MetaException {
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
  public void testConcurrentAddNotifications() throws ExecutionException, InterruptedException, MetaException {

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
              store.addNotificationEvent(dbEvent);
            } catch (InterruptedException | BrokenBarrierException | MetaException e) {
              throw new RuntimeException(e);
            }
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

  private void createTestCatalog(String catName) throws MetaException {
    Catalog cat = new CatalogBuilder()
        .setName(catName)
        .setLocation("/tmp")
        .build();
    objectStore.createCatalog(cat);
  }
}

