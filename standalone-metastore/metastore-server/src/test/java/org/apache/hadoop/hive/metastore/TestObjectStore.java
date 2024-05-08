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
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hive.metastore.ObjectStore.RetryingExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.AddPackageRequest;
import org.apache.hadoop.hive.metastore.api.DropPackageRequest;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetPackageRequest;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.ListPackageRequest;
import org.apache.hadoop.hive.metastore.api.ListStoredProcedureRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Package;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionListComposingSpec;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StoredProcedure;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.GetPartitionsArgs;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.HiveObjectPrivilegeBuilder;
import org.apache.hadoop.hive.metastore.client.builder.HiveObjectRefBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PrivilegeGrantInfoBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.columnstats.ColStatsBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.model.MNotificationLog;
import org.apache.hadoop.hive.metastore.model.MNotificationNextId;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jdo.Query;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.apache.hadoop.hive.metastore.StatisticsTestUtils.assertEqualStatistics;
import static org.apache.hadoop.hive.metastore.TestHiveMetaStore.createSourceTable;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;

@Category(MetastoreUnitTest.class)
public class TestObjectStore {
  private ObjectStore objectStore = null;
  private Configuration conf;

  private static final String ENGINE = "hive";
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
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);

    // Events that get cleaned happen in batches of 1 to exercise batching code
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.EVENT_CLEAN_MAX_EVENTS, 1L);
    MetastoreConf.setBoolVar(conf, ConfVars.STATS_FETCH_BITVECTOR, true);
    MetastoreConf.setBoolVar(conf, ConfVars.STATS_FETCH_KLL, true);

    MetaStoreTestUtils.setConfForStandloneMode(conf);

    setupRandomObjectStoreUrl();

    objectStore = new ObjectStore();
    objectStore.setConf(conf);

    HMSHandler.createDefaultCatalog(objectStore, new Warehouse(conf));
  }

  private void setupRandomObjectStoreUrl(){
    String currentUrl = MetastoreConf.getVar(conf, ConfVars.CONNECT_URL_KEY);
    currentUrl = currentUrl.replace(MetaStoreServerUtils.JUNIT_DATABASE_PREFIX,
        String.format("%s_%s", MetaStoreServerUtils.JUNIT_DATABASE_PREFIX, UUID.randomUUID().toString()));

    MetastoreConf.setVar(conf, ConfVars.CONNECT_URL_KEY, currentUrl);
  }

  @After
  public void tearDown() throws Exception {
    // Clear the SSL system properties before each test.
    System.clearProperty(ObjectStore.TRUSTSTORE_PATH_KEY);
    System.clearProperty(ObjectStore.TRUSTSTORE_PASSWORD_KEY);
    System.clearProperty(ObjectStore.TRUSTSTORE_TYPE_KEY);
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
    sd1.getSerdeInfo().setDescription("this is sd1 description");
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
    sd2.getSerdeInfo().setDescription("this is sd2 description");
    Table newTbl1 = new Table("new" + TABLE1, DB1, "owner", 1, 2, 3, sd2, null, params, null, null,
        "MANAGED_TABLE");

    // Change different fields and verify they were altered
    newTbl1.setOwner("role1");
    newTbl1.setOwnerType(PrincipalType.ROLE);

    objectStore.alterTable(DEFAULT_CATALOG_NAME, DB1, TABLE1, newTbl1, null);
    tables = objectStore.getTables(DEFAULT_CATALOG_NAME, DB1, "new*");
    Assert.assertEquals(1, tables.size());
    Assert.assertEquals("new" + TABLE1, tables.get(0));

    // Verify fields were altered during the alterTable operation
    Table alteredTable = objectStore.getTable(DEFAULT_CATALOG_NAME, DB1, "new" + TABLE1);
    Assert.assertEquals("Owner of table was not altered", newTbl1.getOwner(), alteredTable.getOwner());
    Assert.assertEquals("Owner type of table was not altered", newTbl1.getOwnerType(), alteredTable.getOwnerType());

    // Verify serde description is altered during the alterTable operation
    Assert.assertNotEquals("Serde Description was not altered", tbl1.getSd().getSerdeInfo().getDescription(), alteredTable.getSd().getSerdeInfo().getDescription());
    Assert.assertEquals("Serde Description was not altered", newTbl1.getSd().getSerdeInfo().getDescription(), alteredTable.getSd().getSerdeInfo().getDescription());

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

  @Test (expected = NoSuchObjectException.class)
  public void testTableOpsWhenTableDoesNotExist() throws NoSuchObjectException, MetaException {
    List<String> colNames = Arrays.asList("c0", "c1");
    objectStore.getTableColumnStatistics(DEFAULT_CATALOG_NAME, DB1, "not_existed_table", colNames, ENGINE, "");
  }

  private StorageDescriptor createFakeSd(String location) {
    return new StorageDescriptor(null, location, null, null, false, 0,
        new SerDeInfo("SerDeName", "serializationLib", null), null, null, null);
  }


  /**
   * Tests partition operations
   */
  @Test
  public void testPartitionOps() throws Exception {
    Database db1 = new DatabaseBuilder()
        .setName(DB1)
        .setDescription("description")
        .setLocation("locationurl")
        .build(conf);
    try (AutoCloseable c = deadline()) {
      objectStore.createDatabase(db1);
    }
    StorageDescriptor sd = createFakeSd("location");
    HashMap<String, String> tableParams = new HashMap<>();
    tableParams.put("EXTERNAL", "false");
    FieldSchema partitionKey1 = new FieldSchema("Country", ColumnType.STRING_TYPE_NAME, "");
    FieldSchema partitionKey2 = new FieldSchema("State", ColumnType.STRING_TYPE_NAME, "");
    Table tbl1 =
        new Table(TABLE1, DB1, "owner", 1, 2, 3, sd, Arrays.asList(partitionKey1, partitionKey2),
            tableParams, null, null, "MANAGED_TABLE");
    try (AutoCloseable c = deadline()) {
      objectStore.createTable(tbl1);
    }
    HashMap<String, String> partitionParams = new HashMap<>();
    partitionParams.put("PARTITION_LEVEL_PRIVILEGE", "true");
    List<String> value1 = Arrays.asList("US", "CA");
    Partition part1 = new Partition(value1, DB1, TABLE1, 111, 111, sd, partitionParams);
    part1.setCatName(DEFAULT_CATALOG_NAME);
    try (AutoCloseable c = deadline()) {
      objectStore.addPartition(part1);
    }
    List<String> value2 = Arrays.asList("US", "MA");
    Partition part2 = new Partition(value2, DB1, TABLE1, 222, 222, sd, partitionParams);
    part2.setCatName(DEFAULT_CATALOG_NAME);
    try (AutoCloseable c = deadline()) {
      objectStore.addPartition(part2);
    }

    List<Partition> partitions;
    try (AutoCloseable c = deadline()) {
      partitions = objectStore.getPartitions(DEFAULT_CATALOG_NAME, DB1, TABLE1, 10);
    }
    Assert.assertEquals(2, partitions.size());
    Assert.assertEquals(111, partitions.get(0).getCreateTime());
    Assert.assertEquals(222, partitions.get(1).getCreateTime());

    int numPartitions;
    try (AutoCloseable c = deadline()) {
      numPartitions = objectStore.getNumPartitionsByFilter(DEFAULT_CATALOG_NAME, DB1, TABLE1, "");
    }
    Assert.assertEquals(partitions.size(), numPartitions);

    List<String> partVal = Collections.singletonList("");
    try (AutoCloseable c = deadline()) {
      numPartitions = objectStore.getNumPartitionsByPs(DEFAULT_CATALOG_NAME, DB1, TABLE1, partVal);
    }
    Assert.assertEquals(partitions.size(), numPartitions);

    try (AutoCloseable c = deadline()) {
      numPartitions = objectStore.getNumPartitionsByFilter(DEFAULT_CATALOG_NAME, DB1, TABLE1, "country = \"US\"");
    }
    Assert.assertEquals(2, numPartitions);

    partVal = Collections.singletonList("US");
    try (AutoCloseable c = deadline()) {
      numPartitions = objectStore.getNumPartitionsByPs(DEFAULT_CATALOG_NAME, DB1, TABLE1, partVal);
    }
    Assert.assertEquals(2, numPartitions);

    try (AutoCloseable c = deadline()) {
      objectStore.dropPartition(DEFAULT_CATALOG_NAME, DB1, TABLE1, value1);
      partitions = objectStore.getPartitions(DEFAULT_CATALOG_NAME, DB1, TABLE1, 10);
    }
    Assert.assertEquals(1, partitions.size());
    Assert.assertEquals(222, partitions.get(0).getCreateTime());

    try (AutoCloseable c = deadline()) {
      objectStore.dropPartition(DEFAULT_CATALOG_NAME, DB1, TABLE1, value2);
      objectStore.dropTable(DEFAULT_CATALOG_NAME, DB1, TABLE1);
      objectStore.dropDatabase(db1.getCatalogName(), DB1);
    }
  }

  @Test
  public void testPartitionOpsWhenTableDoesNotExist() throws InvalidObjectException, MetaException {
    List<String> value1 = Arrays.asList("US", "CA");
    StorageDescriptor sd1 = createFakeSd("location1");
    HashMap<String, String> partitionParams = new HashMap<>();
    partitionParams.put("PARTITION_LEVEL_PRIVILEGE", "true");
    Partition part1 = new Partition(value1, DB1, "not_existed_table", 111, 111, sd1, partitionParams);
    try {
      objectStore.addPartition(part1);
    } catch (InvalidObjectException e) {
      // expected
    }
    try {
      objectStore.getPartition(DEFAULT_CATALOG_NAME, DB1, "not_existed_table", value1);
    } catch (NoSuchObjectException e) {
      // expected
    }

    List<String> value2 = Arrays.asList("US", "MA");
    StorageDescriptor sd2 = createFakeSd("location2");
    Partition part2 = new Partition(value2, DB1, "not_existed_table", 222, 222, sd2, partitionParams);
    List<Partition> parts = Arrays.asList(part1, part2);
    try {
      objectStore.addPartitions(DEFAULT_CATALOG_NAME, DB1, "not_existed_table", parts);
    } catch (InvalidObjectException e) {
      // expected
    }

    PartitionSpec partitionSpec1 = new PartitionSpec(DB1, "not_existed_table", "location1");
    partitionSpec1.setPartitionList(new PartitionListComposingSpec(parts));
    PartitionSpecProxy partitionSpecProxy = PartitionSpecProxy.Factory.get(Arrays.asList(partitionSpec1));
    try {
      objectStore.addPartitions(DEFAULT_CATALOG_NAME, DB1, "not_existed_table", partitionSpecProxy, true);
    } catch (InvalidObjectException e) {
      // expected
    }

    List<List<String>> part_vals = Arrays.asList(Arrays.asList("US", "GA"), Arrays.asList("US", "WA"));
    try {
      objectStore.alterPartitions(DEFAULT_CATALOG_NAME, DB1, "not_existed_table", part_vals, parts, 0, "");
    } catch (MetaException e) {
      // expected
      Assert.assertEquals(e.getMessage(), "Specified catalog.database.table does not exist : hive.testobjectstoredb1.not_existed_table");
    }
  }

  @Test
  public void testListPartitionNamesByFilter() throws Exception {
    Database db1 = new DatabaseBuilder()
        .setName(DB1)
        .setDescription("description")
        .setLocation("locationurl")
        .build(conf);
    try (AutoCloseable c = deadline()) {
      objectStore.createDatabase(db1);
    }
    StorageDescriptor sd = createFakeSd("location");
    HashMap<String, String> tableParams = new HashMap<>();
    tableParams.put("EXTERNAL", "false");
    FieldSchema partitionKey1 = new FieldSchema("Country", ColumnType.STRING_TYPE_NAME, "");
    FieldSchema partitionKey2 = new FieldSchema("State", ColumnType.STRING_TYPE_NAME, "");
    Table tbl1 =
        new Table(TABLE1, DB1, "owner", 1, 2, 3, sd, Arrays.asList(partitionKey1, partitionKey2),
            tableParams, null, null, "MANAGED_TABLE");
    try (AutoCloseable c = deadline()) {
      objectStore.createTable(tbl1);
    }
    HashMap<String, String> partitionParams = new HashMap<>();
    partitionParams.put("PARTITION_LEVEL_PRIVILEGE", "true");
    List<String> value1 = Arrays.asList("US", "CA");
    Partition part1 = new Partition(value1, DB1, TABLE1, 111, 111, sd, partitionParams);
    part1.setCatName(DEFAULT_CATALOG_NAME);
    try (AutoCloseable c = deadline()) {
      objectStore.addPartition(part1);
    }
    List<String> value2 = Arrays.asList("US", "MA");
    Partition part2 = new Partition(value2, DB1, TABLE1, 222, 222, sd, partitionParams);
    part2.setCatName(DEFAULT_CATALOG_NAME);
    try (AutoCloseable c = deadline()) {
      objectStore.addPartition(part2);
    }

    List<String> partNames;
    try (AutoCloseable c = deadline()) {
      partNames = objectStore.listPartitionNamesByFilter(DEFAULT_CATALOG_NAME, DB1, TABLE1,
          new GetPartitionsArgs.GetPartitionsArgsBuilder().filter("Country = 'US'").build());
    }
    Assert.assertEquals(2, partNames.size());
    Assert.assertEquals("country=US/state=CA", partNames.get(0));
    Assert.assertEquals("country=US/state=MA", partNames.get(1));

    try (AutoCloseable c = deadline()) {
      partNames = objectStore.listPartitionNamesByFilter(DEFAULT_CATALOG_NAME, DB1, TABLE1,
          new GetPartitionsArgs.GetPartitionsArgsBuilder().filter("State = 'MA'").build());
    }
    Assert.assertEquals(1, partNames.size());
    Assert.assertEquals("country=US/state=MA", partNames.get(0));

    try (AutoCloseable c = deadline()) {
      partNames = objectStore.listPartitionNamesByFilter(DEFAULT_CATALOG_NAME, DB1, TABLE1,
          new GetPartitionsArgs.GetPartitionsArgsBuilder().filter("Country = 'US' and State = 'MA'").build());
    }
    Assert.assertEquals(1, partNames.size());
    Assert.assertEquals("country=US/state=MA", partNames.get(0));
  }

  @Test
  public void testDropPartitionByName() throws Exception {
    Database db1 = new DatabaseBuilder()
        .setName(DB1)
        .setDescription("description")
        .setLocation("locationurl")
        .build(conf);
    try (AutoCloseable c = deadline()) {
      objectStore.createDatabase(db1);
    }
    StorageDescriptor sd = createFakeSd("location");
    HashMap<String, String> tableParams = new HashMap<>();
    tableParams.put("EXTERNAL", "false");
    FieldSchema partitionKey1 = new FieldSchema("Country", ColumnType.STRING_TYPE_NAME, "");
    FieldSchema partitionKey2 = new FieldSchema("State", ColumnType.STRING_TYPE_NAME, "");
    Table tbl1 =
        new Table(TABLE1, DB1, "owner", 1, 2, 3, sd, Arrays.asList(partitionKey1, partitionKey2),
            tableParams, null, null, "MANAGED_TABLE");
    try (AutoCloseable c = deadline()) {
      objectStore.createTable(tbl1);
    }
    HashMap<String, String> partitionParams = new HashMap<>();
    partitionParams.put("PARTITION_LEVEL_PRIVILEGE", "true");
    List<String> value1 = Arrays.asList("US", "CA");
    Partition part1 = new Partition(value1, DB1, TABLE1, 111, 111, sd, partitionParams);
    part1.setCatName(DEFAULT_CATALOG_NAME);
    try (AutoCloseable c = deadline()) {
      objectStore.addPartition(part1);
    }
    List<String> value2 = Arrays.asList("US", "MA");
    Partition part2 = new Partition(value2, DB1, TABLE1, 222, 222, sd, partitionParams);
    part2.setCatName(DEFAULT_CATALOG_NAME);
    try (AutoCloseable c = deadline()) {
      objectStore.addPartition(part2);
    }

    List<Partition> partitions;
    try (AutoCloseable c = deadline()) {
      objectStore.dropPartition(DEFAULT_CATALOG_NAME, DB1, TABLE1, "country=US/state=CA");
      partitions = objectStore.getPartitions(DEFAULT_CATALOG_NAME, DB1, TABLE1, 10);
    }
    Assert.assertEquals(1, partitions.size());
    Assert.assertEquals(222, partitions.get(0).getCreateTime());
    try (AutoCloseable c = deadline()) {
      objectStore.dropPartition(DEFAULT_CATALOG_NAME, DB1, TABLE1, "country=US/state=MA");
      partitions = objectStore.getPartitions(DEFAULT_CATALOG_NAME, DB1, TABLE1, 10);
    }
    Assert.assertEquals(0, partitions.size());

    try (AutoCloseable c = deadline()) {
      // Illegal partName will do nothing, it doesn't matter
      // because the real HMSHandler will guarantee the partName is legal and exists.
      objectStore.dropPartition(DEFAULT_CATALOG_NAME, DB1, TABLE1, "country=US/state=NON_EXIST");
      objectStore.dropPartition(DEFAULT_CATALOG_NAME, DB1, TABLE1, "country=US/st=CA");
    }

    try (AutoCloseable c = deadline()) {
      objectStore.dropTable(DEFAULT_CATALOG_NAME, DB1, TABLE1);
      objectStore.dropDatabase(db1.getCatalogName(), DB1);
    }
  }

  /**
   * Test the concurrent drop of same partition would leak transaction.
   * https://issues.apache.org/jira/browse/HIVE-16839
   *
   * Note: the leak happens during a race condition, this test case tries
   * to simulate the race condition on best effort, it have two threads trying
   * to drop the same set of partitions
   */
  @Test
  public void testConcurrentDropPartitions() throws MetaException, InvalidObjectException {
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

    // Create some partitions
    List<List<String>> partNames = new LinkedList<>();
    for (char c = 'A'; c < 'Z'; c++) {
      String name = "" + c;
      partNames.add(Arrays.asList(name, name));
    }
    for (List<String> n : partNames) {
      Partition p = new Partition(n, DB1, TABLE1, 111, 111, sd, partitionParams);
      p.setCatName(DEFAULT_CATALOG_NAME);
      objectStore.addPartition(p);
    }

    int numThreads = 2;
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < numThreads; i++) {
      executorService.execute(
        () -> {
          ObjectStore threadObjectStore = new ObjectStore();
          threadObjectStore.setConf(conf);
          for (List<String> p : partNames) {
            try {
              threadObjectStore.dropPartition(DEFAULT_CATALOG_NAME, DB1, TABLE1, p);
              System.out.println("Dropping partition: " + p.get(0));
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        }
      );
    }

    executorService.shutdown();
    try {
      executorService.awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
      Assert.assertTrue("Got interrupted.", false);
    }
    Assert.assertTrue("Expect no active transactions.", !objectStore.isActiveTransaction());
  }

  /**
   * Checks if the JDO cache is able to handle directSQL partition drops in one session.
   */
  @Test
  public void testDirectSQLDropPartitionsCacheInSession()
      throws Exception {
    createPartitionedTable(false, false);
    // query the partitions with JDO
    List<Partition> partitions;
    try(AutoCloseable c =deadline()) {
      partitions = objectStore.getPartitionsInternal(DEFAULT_CATALOG_NAME, DB1, TABLE1,
          false, true, new GetPartitionsArgs.GetPartitionsArgsBuilder().max(10).build());
    }
    Assert.assertEquals(3, partitions.size());

    // drop partitions with directSql
    try(AutoCloseable c =deadline()) {
      objectStore.dropPartitionsInternal(DEFAULT_CATALOG_NAME, DB1, TABLE1,
          Arrays.asList("test_part_col=a0", "test_part_col=a1"), true, false);
    }
    try (AutoCloseable c = deadline()) {
      // query the partitions with JDO, checking the cache is not causing any problem
      partitions = objectStore.getPartitionsInternal(DEFAULT_CATALOG_NAME, DB1, TABLE1, false, true,
          new GetPartitionsArgs.GetPartitionsArgsBuilder().max(10).build());
    }
    Assert.assertEquals(1, partitions.size());
  }

  /**
   * Checks if the JDO cache is able to handle directSQL partition drops cross sessions.
   */
  @Test
  public void testDirectSQLDropPartitionsCacheCrossSession()
      throws Exception {
    ObjectStore objectStore2 = new ObjectStore();
    objectStore2.setConf(conf);

    createPartitionedTable(false, false);
    GetPartitionsArgs args = new GetPartitionsArgs.GetPartitionsArgsBuilder().max(10).build();
    // query the partitions with JDO in the 1st session
    List<Partition> partitions;
    try (AutoCloseable c = deadline()) {
      partitions = objectStore.getPartitionsInternal(DEFAULT_CATALOG_NAME, DB1, TABLE1, false, true, args);
    }
    Assert.assertEquals(3, partitions.size());

    // query the partitions with JDO in the 2nd session
    try (AutoCloseable c = deadline()) {
      partitions = objectStore2.getPartitionsInternal(DEFAULT_CATALOG_NAME, DB1, TABLE1, false, true, args);
    }
    Assert.assertEquals(3, partitions.size());

    // drop partitions with directSql in the 1st session
    try (AutoCloseable c = deadline()) {
      objectStore.dropPartitionsInternal(DEFAULT_CATALOG_NAME, DB1, TABLE1,
          Arrays.asList("test_part_col=a0", "test_part_col=a1"), true, false);
    }

    // query the partitions with JDO in the 2nd session, checking the cache is not causing any
    // problem
    try (AutoCloseable c = deadline()) {
      partitions = objectStore2.getPartitionsInternal(DEFAULT_CATALOG_NAME, DB1, TABLE1, false, true, args);
    }
    Assert.assertEquals(1, partitions.size());
  }

  /**
   * Checks if the directSQL partition drop removes every connected data from the RDBMS tables.
   */
  @Test
  public void testDirectSQLDropPartitionsCleanup() throws Exception {

    createPartitionedTable(true, true);

    // Check, that every table in the expected state before the drop
    checkBackendTableSize("PARTITIONS", 3);
    checkBackendTableSize("PART_PRIVS", 3);
    checkBackendTableSize("PART_COL_PRIVS", 3);
    checkBackendTableSize("PART_COL_STATS", 3);
    checkBackendTableSize("PARTITION_PARAMS", 3);
    checkBackendTableSize("PARTITION_KEY_VALS", 3);
    checkBackendTableSize("SD_PARAMS", 3);
    checkBackendTableSize("BUCKETING_COLS", 3);
    checkBackendTableSize("SKEWED_COL_NAMES", 3);
    checkBackendTableSize("SDS", 4); // Table has an SDS
    checkBackendTableSize("SORT_COLS", 3);
    checkBackendTableSize("SERDE_PARAMS", 3);
    checkBackendTableSize("SERDES", 4); // Table has a serde

    // drop the partitions
    try (AutoCloseable c = deadline()) {
      objectStore.dropPartitionsInternal(DEFAULT_CATALOG_NAME, DB1, TABLE1,
	        Arrays.asList("test_part_col=a0", "test_part_col=a1", "test_part_col=a2"), true, false);
    }

    // Check, if every data is dropped connected to the partitions
    checkBackendTableSize("PARTITIONS", 0);
    checkBackendTableSize("PART_PRIVS", 0);
    checkBackendTableSize("PART_COL_PRIVS", 0);
    checkBackendTableSize("PART_COL_STATS", 0);
    checkBackendTableSize("PARTITION_PARAMS", 0);
    checkBackendTableSize("PARTITION_KEY_VALS", 0);
    checkBackendTableSize("SD_PARAMS", 0);
    checkBackendTableSize("BUCKETING_COLS", 0);
    checkBackendTableSize("SKEWED_COL_NAMES", 0);
    checkBackendTableSize("SDS", 1); // Table has an SDS
    checkBackendTableSize("SORT_COLS", 0);
    checkBackendTableSize("SERDE_PARAMS", 0);
    checkBackendTableSize("SERDES", 1); // Table has a serde
  }

  @Test
  public void testDirectSQLCDsCleanup() throws Exception {
    createPartitionedTable(true, true);
    // Checks there is only one CD before altering partition
    checkBackendTableSize("PARTITIONS", 3);
    checkBackendTableSize("CDS", 1);
    checkBackendTableSize("COLUMNS_V2", 5);
    // Alters a partition to create a new column descriptor
    List<String> partVals = Arrays.asList("a0");
    try (AutoCloseable c = deadline()) {
      Partition part = objectStore.getPartition(DEFAULT_CATALOG_NAME, DB1, TABLE1, partVals);
      StorageDescriptor newSd = part.getSd().deepCopy();
      newSd.addToCols(new FieldSchema("test_add_col", "int", null));
      Partition newPart = part.deepCopy();
      newPart.setSd(newSd);
      objectStore.alterPartition(DEFAULT_CATALOG_NAME, DB1, TABLE1, partVals, newPart, null);
    }
    // Checks now there is one more column descriptor
    checkBackendTableSize("PARTITIONS", 3);
    checkBackendTableSize("CDS", 2);
    checkBackendTableSize("COLUMNS_V2", 11);
    // drop the partitions
    try (AutoCloseable c = deadline()) {
      objectStore.dropPartitionsInternal(DEFAULT_CATALOG_NAME, DB1, TABLE1,
          Arrays.asList("test_part_col=a0", "test_part_col=a1", "test_part_col=a2"), true, false);
    }
    // Checks if the data connected to the partitions is dropped
    checkBackendTableSize("PARTITIONS", 0);
    checkBackendTableSize("CDS", 1); // Table has a CD
    checkBackendTableSize("COLUMNS_V2", 5);
  }

  @Test
  public void testGetPartitionStatistics() throws Exception {
    createPartitionedTable(true, true);

    List<List<ColumnStatistics>> stat;
    try (AutoCloseable c = deadline()) {
      stat = objectStore.getPartitionColumnStatistics(DEFAULT_CATALOG_NAME, DB1, TABLE1,
          Arrays.asList("test_part_col=a0", "test_part_col=a1", "test_part_col=a2"),
          Collections.singletonList("test_part_col"));
    }

    Assert.assertEquals(1, stat.size());
    Assert.assertEquals(3, stat.get(0).size());
    Assert.assertEquals(ENGINE, stat.get(0).get(0).getEngine());
    Assert.assertEquals(1, stat.get(0).get(0).getStatsObj().size());

    ColumnStatisticsData computedStats = stat.get(0).get(0).getStatsObj().get(0).getStatsData();
    ColumnStatisticsData expectedStats = new ColStatsBuilder<>(long.class).numNulls(1).numDVs(2)
        .low(3L).high(4L).hll(3, 4).kll(3, 4).build();
    assertEqualStatistics(expectedStats, computedStats);
  }

  /**
   * Creates DB1 database, TABLE1 table with 3 partitions.
   * @param withPrivileges Should we create privileges as well
   * @param withStatistics Should we create statitics as well
   */
  private void createPartitionedTable(boolean withPrivileges, boolean withStatistics)
      throws Exception {
    Database db1 = new DatabaseBuilder()
                       .setName(DB1)
                       .setDescription("description")
                       .setLocation("locationurl")
                       .build(conf);
    try (AutoCloseable c = deadline()) {
      objectStore.createDatabase(db1);
    }
    Table tbl1 =
        new TableBuilder()
            .setDbName(DB1)
            .setTableName(TABLE1)
            .addCol("test_col1", "int")
            .addCol("test_col2", "int")
            .addPartCol("test_part_col", "int")
            .addCol("test_bucket_col", "int", "test bucket col comment")
            .addCol("test_skewed_col", "int", "test skewed col comment")
            .addCol("test_sort_col", "int", "test sort col comment")
            .build(conf);
    try (AutoCloseable c = deadline()) {
      objectStore.createTable(tbl1);
    }
    MTable mTable1 = objectStore.ensureGetMTable(tbl1.getCatName(), tbl1.getDbName(), tbl1.getTableName());
    PrivilegeBag privilegeBag = new PrivilegeBag();
    // Create partitions for the partitioned table
    for(int i=0; i < 3; i++) {
      Partition part = new PartitionBuilder()
                           .inTable(tbl1)
                           .addValue("a" + i)
                           .addSerdeParam("serdeParam", "serdeParamValue")
                           .addStorageDescriptorParam("sdParam", "sdParamValue")
                           .addBucketCol("test_bucket_col")
                           .addSkewedColName("test_skewed_col")
                           .addSortCol("test_sort_col", 1)
                           .build(conf);
      try (AutoCloseable c = deadline()) {
        objectStore.addPartition(part);
      }
      if (withPrivileges) {
        HiveObjectRef partitionReference = new HiveObjectRefBuilder().buildPartitionReference(part);
        HiveObjectRef partitionColumnReference = new HiveObjectRefBuilder()
            .buildPartitionColumnReference(tbl1, "test_part_col", part.getValues());
        PrivilegeGrantInfo privilegeGrantInfo = new PrivilegeGrantInfoBuilder()
            .setPrivilege("a")
            .build();
        HiveObjectPrivilege partitionPriv = new HiveObjectPrivilegeBuilder()
                                                .setHiveObjectRef(partitionReference)
                                                .setPrincipleName("a")
                                                .setPrincipalType(PrincipalType.USER)
                                                .setGrantInfo(privilegeGrantInfo)
                                                .build();
        privilegeBag.addToPrivileges(partitionPriv);
        HiveObjectPrivilege partitionColPriv = new HiveObjectPrivilegeBuilder()
                                                   .setHiveObjectRef(partitionColumnReference)
                                                   .setPrincipleName("a")
                                                   .setPrincipalType(PrincipalType.USER)
                                                   .setGrantInfo(privilegeGrantInfo)
                                                   .build();
        privilegeBag.addToPrivileges(partitionColPriv);
      }

      if (withStatistics) {
        ColumnStatistics stats = new ColumnStatistics();
        ColumnStatisticsDesc desc = new ColumnStatisticsDesc();
        desc.setCatName(tbl1.getCatName());
        desc.setDbName(tbl1.getDbName());
        desc.setTableName(tbl1.getTableName());
        desc.setPartName("test_part_col=a" + i);
        stats.setStatsDesc(desc);

        List<ColumnStatisticsObj> statsObjList = new ArrayList<>(1);
        stats.setStatsObj(statsObjList);
        stats.setEngine(ENGINE);

        ColumnStatisticsData data = new ColStatsBuilder<>(long.class).numNulls(1).numDVs(2)
            .low(3L).high(4L).hll(3, 4).kll(3, 4).build();

        ColumnStatisticsObj partStats = new ColumnStatisticsObj("test_part_col", "int", data);
        statsObjList.add(partStats);

        try (AutoCloseable c = deadline()) {
          objectStore.updatePartitionColumnStatistics(tbl1, mTable1, stats, part.getValues(), null, -1);
        }
      }
    }
    if (withPrivileges) {
      try (AutoCloseable c = deadline()) {
        objectStore.grantPrivileges(privilegeBag);
      }
    }
  }

  /**
   * Checks if the HMS backend db row number is as expected. If they are not, an
   * {@link AssertionError} is thrown.
   * @param tableName The table in which we count the rows
   * @param size The expected row number
   * @throws SQLException If there is a problem connecting to / querying the backend DB
   */
  private void checkBackendTableSize(String tableName, int size) throws SQLException {
    String connectionStr = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.CONNECT_URL_KEY);
    Connection conn = DriverManager.getConnection(connectionStr);
    Statement stmt = conn.createStatement();

    ResultSet rs = stmt.executeQuery("SELECT COUNT(1) FROM " + tableName);
    rs.next();
    Assert.assertEquals(tableName + " table should contain " + size + " rows", size,
        rs.getLong(1));
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

  @Deprecated
  private static void dropAllStoreObjects(RawStore store)
      throws MetaException, InvalidObjectException, InvalidInputException {
    try {
      List<Function> functions = store.getAllFunctions(DEFAULT_CATALOG_NAME);
      for (Function func : functions) {
        store.dropFunction(DEFAULT_CATALOG_NAME, func.getDbName(), func.getFunctionName());
      }
      for (String catName : store.getCatalogs()) {
        List<String> dbs = store.getAllDatabases(catName);
        for (String db : dbs) {
          List<String> tbls = store.getAllTables(DEFAULT_CATALOG_NAME, db);
          for (String tbl : tbls) {
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
    Assert.assertEquals(value, PersistenceManagerProvider.getProperty(key));
    Assert.assertNull(PersistenceManagerProvider.getProperty(key1));
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
    objectStore.cleanNotificationEvents(0);
    eventResponse = objectStore.getNextNotification(new NotificationEventRequest());
    Assert.assertEquals(0, eventResponse.getEventsSize());
  }

  /**
   * Test metastore configuration property METASTORE_MAX_EVENT_RESPONSE
   */
  @Test
  public void testMaxEventResponse() throws InterruptedException, MetaException {
    NotificationEvent event =
        new NotificationEvent(0, 0, EventMessage.EventType.CREATE_DATABASE.toString(), "");
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.METASTORE_MAX_EVENT_RESPONSE, 1);
    ObjectStore objs = new ObjectStore();
    objs.setConf(conf);
    // Verify if METASTORE_MAX_EVENT_RESPONSE will limit number of events to respond
    for (int i = 0; i < 3; i++) {
      objs.addNotificationEvent(event);
    }
    NotificationEventResponse eventResponse = objs.getNextNotification(new NotificationEventRequest());
    Assert.assertEquals(1, eventResponse.getEventsSize());
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
//    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECT_URL_KEY,
//        "jdbc:mysql://localhost:3306/metastore_db");
//    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME, "");
//    conf.setVar(HiveConf.ConfVars.METASTORE_PWD, "");

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

    ExecutorService executorService = newFixedThreadPool(NUM_THREADS);
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

  /**
   * This test calls ObjectStore.setConf methods from multiple threads. Each threads uses its
   * own instance of ObjectStore to simulate thread-local objectstore behaviour.
   * @throws Exception
   */
  @Test
  public void testConcurrentPMFInitialize() throws Exception {
    final String dataSourceProp = "datanucleus.connectionPool.maxPoolSize";
    // Barrier is used to ensure that all threads start race at the same time
    final int numThreads = 10;
    final int numIteration = 50;
    final CyclicBarrier barrier = new CyclicBarrier(numThreads);
    final AtomicInteger counter = new AtomicInteger(0);
    ExecutorService executor = newFixedThreadPool(numThreads);
    List<Future<Void>> results = new ArrayList<>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      final Random random = new Random();
      Configuration conf = MetastoreConf.newMetastoreConf();
      // DN class initialization can reach a deadlock situation
      // in case the one holding the write lock doesn't get a connection from the CP manager
      conf.set(dataSourceProp, Integer.toString( 2 * numThreads ));
      MetaStoreTestUtils.setConfForStandloneMode(conf);
      results.add(executor.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          // each thread gets its own ObjectStore to simulate threadLocal store
          ObjectStore objectStore = new ObjectStore();
          barrier.await();
          for (int j = 0; j < numIteration; j++) {
            // set connectionPool to a random value to increase the likelihood of pmf
            // re-initialization
            int randomNumber = random.nextInt(100);
            if (randomNumber % 2 == 0) {
              objectStore.setConf(conf);
            } else {
              Assert.assertNotNull(objectStore.getPersistenceManager());
            }
            counter.getAndIncrement();
          }
          return null;
        }
      }));
    }
    for (Future<Void> future : results) {
      future.get(120, TimeUnit.SECONDS);
    }
    Assert.assertEquals("Unexpected number of setConf calls", numIteration * numThreads,
        counter.get());
  }

  /**
   * Test the SSL configuration parameters to ensure that they modify the Java system properties correctly.
   */
  @Test
  public void testSSLPropertiesAreSet() {
    setAndCheckSSLProperties(true, "/tmp/truststore.p12", "password", "pkcs12");
  }

  /**
   * Test the property {@link MetastoreConf.ConfVars#DBACCESS_USE_SSL} to ensure that it correctly
   * toggles whether or not the SSL configuration parameters will be set. Effectively, this is testing whether
   * SSL can be turned on/off correctly.
   */
  @Test
  public void testUseSSLProperty() {
    setAndCheckSSLProperties(false, "/tmp/truststore.jks", "password", "jks");
  }

  /**
   * Test that the deprecated property {@link MetastoreConf.ConfVars#DBACCESS_SSL_PROPS} is overwritten by the
   * MetastoreConf.ConfVars#DBACCESS_SSL_* properties if both are set.
   *
   * This is not an ideal scenario. It is highly recommend to only set the MetastoreConf#ConfVars.DBACCESS_SSL_* properties.
   */
  @Test
  public void testDeprecatedConfigIsOverwritten() {
    // Different from the values in the safe config
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.DBACCESS_SSL_PROPS,
        ObjectStore.TRUSTSTORE_PATH_KEY + "=/tmp/truststore.p12," + ObjectStore.TRUSTSTORE_PASSWORD_KEY + "=pwd," +
            ObjectStore.TRUSTSTORE_TYPE_KEY + "=pkcs12");

    // Safe config
    setAndCheckSSLProperties(true, "/tmp/truststore.jks", "password", "jks");
  }

  /**
   * Test that providing an empty truststore path and truststore password will not throw an exception.
   */
  @Test
  public void testEmptyTrustStoreProps() {
    setAndCheckSSLProperties(true, "", "", "jks");
  }

  /**
   * Tests getPrimaryKeys() when db_name isn't specified.
   */
  @Test
  public void testGetPrimaryKeys() throws Exception {
    Database db1 =
        new DatabaseBuilder().setName(DB1).setDescription("description")
            .setLocation("locationurl").build(conf);
    objectStore.createDatabase(db1);
    StorageDescriptor sd1 = new StorageDescriptor(
        ImmutableList.of(new FieldSchema("pk_col", "double", null)), "location",
        null, null, false, 0,
        new SerDeInfo("SerDeName", "serializationLib", null), null, null, null);
    HashMap<String, String> params = new HashMap<>();
    params.put("EXTERNAL", "false");
    Table tbl1 =
        new Table(TABLE1, DB1, "owner", 1, 2, 3, sd1, null, params, null, null,
            "MANAGED_TABLE");
    objectStore.createTable(tbl1);

    SQLPrimaryKey pk =
        new SQLPrimaryKey(DB1, TABLE1, "pk_col", 1, "pk_const_1", false, false,
            false);
    pk.setCatName(DEFAULT_CATALOG_NAME);
    objectStore.addPrimaryKeys(ImmutableList.of(pk));

    // Primary key retrieval should be success, even if db_name isn't specified.
    assertEquals("pk_col",
        objectStore.getPrimaryKeys(DEFAULT_CATALOG_NAME, null, TABLE1).get(0)
            .getColumn_name());
    objectStore.dropTable(DEFAULT_CATALOG_NAME, DB1, TABLE1);
    objectStore.dropDatabase(db1.getCatalogName(), DB1);
  }

  /**
   * Helper method for setting and checking the SSL configuration parameters.
   * @param useSSL whether or not SSL is enabled
   * @param trustStorePath truststore path, corresponding to the value for {@link MetastoreConf.ConfVars#DBACCESS_SSL_TRUSTSTORE_PATH}
   * @param trustStorePassword truststore password, corresponding to the value for {@link MetastoreConf.ConfVars#DBACCESS_SSL_TRUSTSTORE_PASSWORD}
   * @param trustStoreType truststore type, corresponding to the value for {@link MetastoreConf.ConfVars#DBACCESS_SSL_TRUSTSTORE_TYPE}
   */
  private void setAndCheckSSLProperties(boolean useSSL, String trustStorePath, String trustStorePassword, String trustStoreType) {
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.DBACCESS_USE_SSL, useSSL);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.DBACCESS_SSL_TRUSTSTORE_PATH, trustStorePath);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.DBACCESS_SSL_TRUSTSTORE_PASSWORD, trustStorePassword);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.DBACCESS_SSL_TRUSTSTORE_TYPE, trustStoreType);
    objectStore.setConf(conf); // Calls configureSSL()

    // Check that the properties were set correctly
    checkSSLProperty(useSSL, ObjectStore.TRUSTSTORE_PATH_KEY, trustStorePath);
    checkSSLProperty(useSSL, ObjectStore.TRUSTSTORE_PASSWORD_KEY, trustStorePassword);
    checkSSLProperty(useSSL, ObjectStore.TRUSTSTORE_TYPE_KEY, trustStoreType);
  }

  @Test
  public void testUpdateStoredProc() throws Exception {
    objectStore.createDatabase(new DatabaseBuilder()
            .setName(DB1)
            .setDescription("description")
            .setLocation("locationurl")
            .build(conf));

    StoredProcedure input = new StoredProcedure();
    input.setCatName("hive");
    input.setSource("print 'Hello world'");
    input.setOwnerName("user1");
    input.setName("toBeUpdated");
    input.setDbName(DB1);
    objectStore.createOrUpdateStoredProcedure(input);
    input.setSource("print 'Hello world2'");
    objectStore.createOrUpdateStoredProcedure(input);

    StoredProcedure retrieved = objectStore.getStoredProcedure("hive", DB1, "toBeUpdated");
    Assert.assertEquals(input, retrieved);
  }

  @Test
  public void testStoredProcSaveAndRetrieve() throws Exception {
    objectStore.createDatabase(new DatabaseBuilder()
            .setName(DB1)
            .setDescription("description")
            .setLocation("locationurl")
            .build(conf));

    StoredProcedure input = new StoredProcedure();
    input.setSource("print 'Hello world'");
    input.setCatName("hive");
    input.setOwnerName("user1");
    input.setName("greetings");
    input.setDbName(DB1);

    objectStore.createOrUpdateStoredProcedure(input);
    StoredProcedure retrieved = objectStore.getStoredProcedure("hive", DB1, "greetings");

    Assert.assertEquals(input, retrieved);
  }

  @Test
  public void testDropStoredProc() throws Exception {
    objectStore.createDatabase(new DatabaseBuilder()
            .setName(DB1)
            .setDescription("description")
            .setLocation("locationurl")
            .build(conf));

    StoredProcedure toBeDeleted = new StoredProcedure();
    toBeDeleted.setSource("print 'Hello world'");
    toBeDeleted.setCatName("hive");
    toBeDeleted.setOwnerName("user1");
    toBeDeleted.setName("greetings");
    toBeDeleted.setDbName(DB1);
    objectStore.createOrUpdateStoredProcedure(toBeDeleted);
    Assert.assertNotNull(objectStore.getStoredProcedure("hive", DB1, "greetings"));
    objectStore.dropStoredProcedure("hive", DB1, "greetings");
    Assert.assertNull(objectStore.getStoredProcedure("hive", DB1, "greetings"));
  }

  @Test
  public void testListStoredProc() throws Exception {
    objectStore.createDatabase(new DatabaseBuilder()
            .setName(DB1)
            .setDescription("description")
            .setLocation("locationurl")
            .build(conf));
    objectStore.createDatabase(new DatabaseBuilder()
            .setName(DB2)
            .setDescription("description")
            .setLocation("locationurl")
            .build(conf));

    StoredProcedure proc1 = new StoredProcedure();
    proc1.setSource("print 'Hello world'");
    proc1.setCatName("hive");
    proc1.setOwnerName("user1");
    proc1.setName("proc1");
    proc1.setDbName(DB1);
    objectStore.createOrUpdateStoredProcedure(proc1);

    StoredProcedure proc2 = new StoredProcedure();
    proc2.setSource("print 'Hello world'");
    proc2.setCatName("hive");
    proc2.setOwnerName("user2");
    proc2.setName("proc2");
    proc2.setDbName(DB2);
    objectStore.createOrUpdateStoredProcedure(proc2);

    List<String> result = objectStore.getAllStoredProcedures(new ListStoredProcedureRequest("hive"));
    Assert.assertEquals(2, result.size());
    assertThat(result, hasItems("proc1", "proc2"));

    ListStoredProcedureRequest req = new ListStoredProcedureRequest("hive");
    req.setDbName(DB1);
    result = objectStore.getAllStoredProcedures(req);
    Assert.assertEquals(1, result.size());
    assertThat(result, hasItems("proc1"));
  }

  @Test
  public void testUpdateCreationMetadataSetsMaterializationTime() throws Exception {
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
    Table tbl1 = new Table(TABLE1, DB1, "owner", 1, 2, 3, sd1, null, params, null, null, "MANAGED_TABLE");
    tbl1.setCatName(db1.getCatalogName());
    objectStore.createTable(tbl1);

    Table matView1 = new Table("mat1", DB1, "owner", 1, 2, 3, sd1, null, params, null, null, "MATERIALIZED_VIEW");
    matView1.setCatName(db1.getCatalogName());

    CreationMetadata creationMetadata = new CreationMetadata();
    creationMetadata.setCatName(db1.getCatalogName());
    creationMetadata.setDbName(matView1.getDbName());
    creationMetadata.setTblName(matView1.getTableName());
    creationMetadata.setTablesUsed(Collections.singleton(tbl1.getDbName() + "." + tbl1.getTableName()));
    creationMetadata.setSourceTables(Collections.singletonList(createSourceTable(tbl1)));
    matView1.setCreationMetadata(creationMetadata);
    objectStore.createTable(matView1);

    CreationMetadata newCreationMetadata = new CreationMetadata(matView1.getCatName(), matView1.getDbName(),
            matView1.getTableName(), ImmutableSet.copyOf(creationMetadata.getTablesUsed()));
    newCreationMetadata.setSourceTables(Collections.unmodifiableList(creationMetadata.getSourceTables()));
    objectStore.updateCreationMetadata(matView1.getCatName(), matView1.getDbName(), matView1.getTableName(), newCreationMetadata);

    assertThat(creationMetadata.getMaterializationTime(), is(not(0)));
  }

  @Test
  public void testCreateAndFindPackage() throws Exception {
    objectStore.createDatabase(new DatabaseBuilder()
            .setName(DB1)
            .setDescription("description")
            .setLocation("locationurl")
            .build(conf));
    AddPackageRequest pkg = new AddPackageRequest("hive", DB1, "pkg1", "user1", "src", "src");
    objectStore.addPackage(pkg);
    Package found = objectStore.findPackage(new GetPackageRequest("hive", DB1, "pkg1"));
    Assert.assertEquals(pkg.getBody(), found.getBody());
    Assert.assertEquals(pkg.getHeader(), found.getHeader());
    Assert.assertEquals(pkg.getCatName(), found.getCatName());
    Assert.assertEquals(pkg.getDbName(), found.getDbName());
    Assert.assertEquals(pkg.getOwnerName(), found.getOwnerName());
    Assert.assertEquals(pkg.getPackageName(), found.getPackageName());
  }

  @Test
  public void testDropPackage() throws Exception {
    objectStore.createDatabase(new DatabaseBuilder()
            .setName(DB1)
            .setDescription("description")
            .setLocation("locationurl")
            .build(conf));
    AddPackageRequest pkg = new AddPackageRequest("hive", DB1, "pkg1", "user1", "header", "body");
    objectStore.addPackage(pkg);
    Assert.assertNotNull(objectStore.findPackage(new GetPackageRequest("hive", DB1, "pkg1")));
    objectStore.dropPackage(new DropPackageRequest("hive", DB1, "pkg1"));
    Assert.assertNull(objectStore.findPackage(new GetPackageRequest("hive", DB1, "pkg1")));
  }

  @Test
  public void testListPackage() throws Exception {
    objectStore.createDatabase(new DatabaseBuilder()
            .setName(DB1)
            .setDescription("description")
            .setLocation("locationurl")
            .build(conf));
    objectStore.createDatabase(new DatabaseBuilder()
            .setName(DB2)
            .setDescription("description")
            .setLocation("locationurl")
            .build(conf));
    AddPackageRequest pkg1 = new AddPackageRequest("hive", DB1, "pkg1", "user1", "header1", "body1");
    AddPackageRequest pkg2 = new AddPackageRequest("hive", DB2, "pkg2", "user1", "header2", "body2");
    objectStore.addPackage(pkg1);
    objectStore.addPackage(pkg2);
    List<String> result = objectStore.listPackages(new ListPackageRequest("hive"));
    assertThat(result, hasItems("pkg1", "pkg2"));
    Assert.assertEquals(2, result.size());
    ListPackageRequest req = new ListPackageRequest("hive");
    req.setDbName(DB1);
    result = objectStore.listPackages(req);
    assertThat(result, hasItems("pkg1"));
    Assert.assertEquals(1, result.size());
  }

  @Test
  public void testSavePoint() throws Exception {
    List<String> partNames = Arrays.asList("test_part_col=a0", "test_part_col=a1", "test_part_col=a2");
    createPartitionedTable(true, false);
    Assert.assertEquals(3, objectStore.getPartitionCount());

    objectStore.openTransaction();
    objectStore.new GetHelper<Object>(DEFAULT_CATALOG_NAME, DB1, TABLE1, true, true) {
      @Override
      protected String describeResult() {
        return "test savepoint";
      }

      @Override
      protected Object getSqlResult(ObjectStore.GetHelper<Object> ctx) throws MetaException {
        // drop the partitions with SQL alone
        try (AutoCloseable c = deadline()) {
          objectStore.dropPartitionsInternal(ctx.catName, ctx.dbName, ctx.tblName, partNames, true,
              false);
          Assert.assertEquals(0, objectStore.getPartitionCount());
        } catch (Exception e) {
          throw new MetaException(e.getMessage());
        }
        throw new MetaException("Throwing exception in direct SQL to test Savepoint");
      }

      @Override
      protected Object getJdoResult(ObjectStore.GetHelper<Object> ctx) throws MetaException {
        // drop the partitions with JDO alone
        try (AutoCloseable c = deadline()) {
          Assert.assertEquals(3, objectStore.getPartitionCount());
          objectStore.dropPartitionsInternal(ctx.catName, ctx.dbName, ctx.tblName, partNames, false,
              true);
        } catch (Exception e) {
          throw new MetaException(e.getMessage());
        }
        return null;
      }
    }.run(false);
    objectStore.commitTransaction();
    Assert.assertEquals(0, objectStore.getPartitionCount());
  }

  @Test
  public void testNoJdoForUnrecoverableException() throws Exception {
    Exception[] unrecoverableExceptions = new Exception[] {
        new SQLIntegrityConstraintViolationException("Unrecoverable ex"),
        new DeadlineException("unrecoverable ex")};
    for (Exception unrecoverableException : unrecoverableExceptions) {
      objectStore.openTransaction();
      AtomicBoolean runDirectSql = new AtomicBoolean(false);
      AtomicBoolean runJdo = new AtomicBoolean(false);
      try {
        objectStore.new GetHelper<Object>(DEFAULT_CATALOG_NAME, DB1, TABLE1, true, true) {
          @Override
          protected String describeResult() {
            return "test not run jdo for unrecoverable exception";
          }

          @Override
          protected Object getSqlResult(ObjectStore.GetHelper ctx) throws MetaException {
            runDirectSql.set(true);
            MetaException me = new MetaException("Throwing unrecoverable exception to test not run jdo.");
            me.initCause(unrecoverableException);
            throw me;
          }

          @Override
          protected Object getJdoResult(ObjectStore.GetHelper ctx) throws MetaException, NoSuchObjectException {
            runJdo.set(true);
            SQLIntegrityConstraintViolationException ex = new SQLIntegrityConstraintViolationException("Unrecoverable ex");
            MetaException me = new MetaException("Throwing unrecoverable exception to test not run jdo.");
            me.initCause(ex);
            throw me;
          }
        }.run(false);
      } catch (MetaException ex) {
        // expected
      }
      objectStore.commitTransaction();
      Assert.assertEquals(true, runDirectSql.get());
      Assert.assertEquals(false, runJdo.get());
    }
  }

  /**
   * Helper method to check whether the Java system properties were set correctly in {@link ObjectStore#configureSSL(Configuration)}
   * @param useSSL whether or not SSL is enabled
   * @param key Java system property key
   * @param value Java system property value indicated by the key
   */
  private void checkSSLProperty(boolean useSSL, String key, String value) {
    if (useSSL && !value.isEmpty()) {
      Assert.assertEquals(value, System.getProperty(key));
    } else {
      Assert.assertNull(System.getProperty(key));
    }
  }

  private void createTestCatalog(String catName) throws MetaException {
    Catalog cat = new CatalogBuilder()
        .setName(catName)
        .setLocation("/tmp")
        .build();
    objectStore.createCatalog(cat);
  }

  AutoCloseable deadline() throws Exception {
    Deadline.registerIfNot(100_000);
    Deadline.startTimer("some method");
    return new AutoCloseable() {

      @Override
      public void close() throws Exception {
        Deadline.stopTimer();
      }
    };
  }
}

