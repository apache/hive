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

package org.apache.hadoop.hive.metastore.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.collect.Lists;

/**
 * Tests for creating partitions.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestAddPartitions {

  // Needed until there is no junit release with @BeforeParam, @AfterParam (junit 4.13)
  // https://github.com/junit-team/junit4/commit/1bf8438b65858565dbb64736bfe13aae9cfc1b5a
  // Then we should remove our own copy
  private static Set<AbstractMetaStoreService> metaStoreServices = null;
  private AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;

  private static final String DB_NAME = "test_partition_db";
  private static final String TABLE_NAME = "test_partition_table";
  private static final String DEFAULT_PARAM_VALUE = "partparamvalue";
  private static final String DEFAULT_PARAM_KEY = "partparamkey";
  private static final String DEFAULT_YEAR_VALUE = "2017";
  private static final String DEFAULT_COL_TYPE = "string";
  private static final String YEAR_COL_NAME = "year";
  private static final String MONTH_COL_NAME = "month";
  private static final short MAX = -1;

  @Parameterized.Parameters(name = "{0}")
  public static List<Object[]> getMetaStoreToTest() throws Exception {
    List<Object[]> result = MetaStoreFactoryForTests.getMetaStores();
    metaStoreServices = result.stream()
                            .map(test -> (AbstractMetaStoreService)test[1])
                            .collect(Collectors.toSet());
    return result;
  }

  public TestAddPartitions(String name, AbstractMetaStoreService metaStore) throws Exception {
    this.metaStore = metaStore;
    this.metaStore.start();
  }

  // Needed until there is no junit release with @BeforeParam, @AfterParam (junit 4.13)
  // https://github.com/junit-team/junit4/commit/1bf8438b65858565dbb64736bfe13aae9cfc1b5a
  // Then we should move this to @AfterParam
  @AfterClass
  public static void stopMetaStores() throws Exception {
    for(AbstractMetaStoreService metaStoreService : metaStoreServices) {
      metaStoreService.stop();
    }
  }

  @Before
  public void setUp() throws Exception {
    // Get new client
    client = metaStore.getClient();

    // Clean up the database
    client.dropDatabase(DB_NAME, true, true, true);
    metaStore.cleanWarehouseDirs();
    Database db = new DatabaseBuilder().
        setName(DB_NAME).
        build();
    client.createDatabase(db);
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (client != null) {
        client.close();
      }
    } finally {
      client = null;
    }
  }

  // Tests for the Partition add_partition(Partition partition) method

  @Test
  public void testAddPartition() throws Exception {

    Table table = createTable();
    Partition partition =
        buildPartition(Lists.newArrayList(DEFAULT_YEAR_VALUE), getYearPartCol(), 1);
    Partition resultPart = client.add_partition(partition);
    Assert.assertNotNull(resultPart);
    verifyPartition(table, "year=2017", Lists.newArrayList(DEFAULT_YEAR_VALUE), 1);
  }

  @Test
  public void testAddPartitionTwoValues() throws Exception {

    String tableLocation = metaStore.getWarehouseRoot() + "/" + TABLE_NAME;
    Table table = createTable(DB_NAME, TABLE_NAME, getYearAndMonthPartCols(), tableLocation);
    Partition partition =
        buildPartition(Lists.newArrayList("2017", "march"), getYearAndMonthPartCols(), 1);
    client.add_partition(partition);
    verifyPartition(table, "year=2017/month=march", Lists.newArrayList("2017", "march"), 1);
  }

  @Test
  public void testAddPartitionWithDefaultAttributes() throws Exception {

    Table table = createTable();

    Partition partition = new PartitionBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .addValue("2017")
        .setCols(getYearPartCol())
        .addCol("test_id", "int", "test col id")
        .addCol("test_value", "string", "test col value")
        .build();

    client.add_partition(partition);

    // Check if the default values are set for all unfilled attributes
    Partition part = client.getPartition(DB_NAME, TABLE_NAME, "year=2017");
    Assert.assertNotNull(part);
    Assert.assertEquals(TABLE_NAME, part.getTableName());
    Assert.assertEquals(DB_NAME, part.getDbName());
    Assert.assertEquals(Lists.newArrayList("2017"), part.getValues());
    List<FieldSchema> cols = new ArrayList<>();
    cols.addAll(getYearPartCol());
    cols.add(new FieldSchema("test_id", "int", "test col id"));
    cols.add(new FieldSchema("test_value", "string", "test col value"));
    Assert.assertEquals(cols, part.getSd().getCols());
    verifyPartitionAttributesDefaultValues(part, table.getSd().getLocation());
  }

  @Test
  public void testAddPartitionUpperCase() throws Exception {

    String tableLocation = metaStore.getWarehouseRoot() + "/" + TABLE_NAME;
    createTable(DB_NAME, TABLE_NAME, getMonthPartCol(), tableLocation);

    Partition partition = buildPartition(Lists.newArrayList("APRIL"), getMonthPartCol(), 1);
    client.add_partition(partition);

    Partition part = client.getPartition(DB_NAME, TABLE_NAME, "month=APRIL");
    Assert.assertNotNull(part);
    Assert.assertEquals(TABLE_NAME, part.getTableName());
    Assert.assertEquals(DB_NAME, part.getDbName());
    Assert.assertEquals("APRIL", part.getValues().get(0));
    Assert.assertEquals(tableLocation + "/month=APRIL", part.getSd().getLocation());
    Assert.assertTrue(metaStore.isPathExists(new Path(part.getSd().getLocation())));
  }

  @Test(expected = InvalidObjectException.class)
  public void testAddPartitionNonExistingDb() throws Exception {

    Partition partition = buildPartition("nonexistingdb", TABLE_NAME, DEFAULT_YEAR_VALUE);
    client.add_partition(partition);
  }

  @Test(expected = InvalidObjectException.class)
  public void testAddPartitionNonExistingTable() throws Exception {

    Partition partition = buildPartition(DB_NAME, "nonexistingtable", DEFAULT_YEAR_VALUE);
    client.add_partition(partition);
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionNullDb() throws Exception {

    Partition partition = buildPartition(null, TABLE_NAME, DEFAULT_YEAR_VALUE);
    client.add_partition(partition);
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionNullTable() throws Exception {

    Partition partition = buildPartition(DB_NAME, null, DEFAULT_YEAR_VALUE);
    client.add_partition(partition);
  }

  @Test(expected = InvalidObjectException.class)
  public void testAddPartitionEmptyDb() throws Exception {

    Partition partition = buildPartition("", TABLE_NAME, DEFAULT_YEAR_VALUE);
    client.add_partition(partition);
  }

  @Test(expected = InvalidObjectException.class)
  public void testAddPartitionEmptyTable() throws Exception {

    Partition partition = buildPartition(DB_NAME, "", DEFAULT_YEAR_VALUE);
    client.add_partition(partition);
  }

  @Test(expected = AlreadyExistsException.class)
  public void testAddPartitionAlreadyExists() throws Exception {

    createTable();
    Partition partition1 = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    Partition partition2 = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    client.add_partition(partition1);
    client.add_partition(partition2);
  }

  @Test
  public void testAddPartitionsWithSameNameCaseSensitive() throws Exception {

    createTable(DB_NAME, TABLE_NAME, getMonthPartCol(),
        metaStore.getWarehouseRoot() + "/" + TABLE_NAME);

    Partition partition1 = buildPartition(Lists.newArrayList("may"), getMonthPartCol(), 1);
    Partition partition2 = buildPartition(Lists.newArrayList("MAY"), getMonthPartCol(), 2);
    client.add_partition(partition1);
    client.add_partition(partition2);

    Partition part = client.getPartition(DB_NAME, TABLE_NAME, "month=MAY");
    Assert.assertEquals(DEFAULT_PARAM_VALUE + "2",
        part.getParameters().get(DEFAULT_PARAM_KEY + "2"));
    Assert.assertEquals(metaStore.getWarehouseRoot() + "/" + TABLE_NAME + "/month=MAY",
        part.getSd().getLocation());
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionNullSd() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    partition.setSd(null);
    client.add_partition(partition);
  }

  @Test
  public void testAddPartitionNullColsInSd() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    partition.getSd().setCols(null);
    client.add_partition(partition);

    // TODO: Not sure that this is the correct behavior. It doesn't make sense to create the
    // partition without column info. This should be investigated later.
    Partition part =
        client.getPartition(DB_NAME, TABLE_NAME, Lists.newArrayList(DEFAULT_YEAR_VALUE));
    Assert.assertNotNull(part);
    Assert.assertNull(part.getSd().getCols());
  }

  @Test
  public void testAddPartitionEmptyColsInSd() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    partition.getSd().setCols(new ArrayList<FieldSchema>());
    client.add_partition(partition);

    // TODO: Not sure that this is the correct behavior. It doesn't make sense to create the
    // partition without column info. This should be investigated later.
    Partition part =
        client.getPartition(DB_NAME, TABLE_NAME, Lists.newArrayList(DEFAULT_YEAR_VALUE));
    Assert.assertNotNull(part);
    Assert.assertTrue(part.getSd().getCols().isEmpty());
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionNullColTypeInSd() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    partition.getSd().getCols().get(0).setType(null);
    client.add_partition(partition);
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionNullColNameInSd() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    partition.getSd().getCols().get(0).setName(null);
    client.add_partition(partition);
  }

  @Test
  public void testAddPartitionInvalidColTypeInSd() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    partition.getSd().getCols().get(0).setType("xyz");
    client.add_partition(partition);

    // TODO: Not sure that this is the correct behavior. It doesn't make sense to create the
    // partition with column with invalid type. This should be investigated later.
    Partition part =
        client.getPartition(DB_NAME, TABLE_NAME, Lists.newArrayList(DEFAULT_YEAR_VALUE));
    Assert.assertNotNull(part);
    Assert.assertEquals("xyz", part.getSd().getCols().get(0).getType());
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionEmptySerdeInfo() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    partition.getSd().setSerdeInfo(null);
    client.add_partition(partition);
  }

  @Test
  public void testAddPartitionNullLocation() throws Exception {

    createTable(DB_NAME, TABLE_NAME, metaStore.getWarehouseRoot() + "/addparttest2");
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE, null);
    client.add_partition(partition);
    Partition part = client.getPartition(DB_NAME, TABLE_NAME, "year=2017");
    Assert.assertEquals(metaStore.getWarehouseRoot() + "/addparttest2/year=2017",
        part.getSd().getLocation());
    Assert.assertTrue(metaStore.isPathExists(new Path(part.getSd().getLocation())));
  }

  @Test
  public void testAddPartitionEmptyLocation() throws Exception {

    createTable(DB_NAME, TABLE_NAME, metaStore.getWarehouseRoot() + "/addparttest3");
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE, "");
    client.add_partition(partition);
    Partition part = client.getPartition(DB_NAME, TABLE_NAME, "year=2017");
    Assert.assertEquals(metaStore.getWarehouseRoot() + "/addparttest3/year=2017",
        part.getSd().getLocation());
    Assert.assertTrue(metaStore.isPathExists(new Path(part.getSd().getLocation())));
  }

  @Test
  public void testAddPartitionNullLocationInTableToo() throws Exception {

    createTable(DB_NAME, TABLE_NAME, null);
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE, null);
    client.add_partition(partition);
    Partition part = client.getPartition(DB_NAME, TABLE_NAME, "year=2017");
    Assert.assertEquals(
        metaStore.getWarehouseRoot() + "/test_partition_db.db/test_partition_table/year=2017",
        part.getSd().getLocation());
    Assert.assertTrue(metaStore.isPathExists(new Path(part.getSd().getLocation())));
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionForView() throws Exception {

    Table table = new TableBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .setType("VIRTUAL_VIEW")
        .addCol("test_id", "int", "test col id")
        .addCol("test_value", DEFAULT_COL_TYPE, "test col value")
        .addPartCol(YEAR_COL_NAME, DEFAULT_COL_TYPE)
        .setLocation(null)
        .build();
    client.createTable(table);
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    client.add_partition(partition);
  }

  @Test
  public void testAddPartitionForExternalTable() throws Exception {

    String tableName = "part_add_ext_table";
    String tableLocation = metaStore.getWarehouseRoot() + "/" + tableName;
    String partitionLocation = tableLocation + "/addparttest";
    createExternalTable(tableName, tableLocation);
    Partition partition = buildPartition(DB_NAME, tableName, DEFAULT_YEAR_VALUE, partitionLocation);
    client.add_partition(partition);
    Partition resultPart =
        client.getPartition(DB_NAME, tableName, Lists.newArrayList(DEFAULT_YEAR_VALUE));
    Assert.assertNotNull(resultPart);
    Assert.assertNotNull(resultPart.getSd());
    Assert.assertEquals(partitionLocation, resultPart.getSd().getLocation());
  }

  @Test
  public void testAddPartitionForExternalTableNullLocation() throws Exception {

    String tableName = "part_add_ext_table";
    createExternalTable(tableName, null);
    Partition partition = buildPartition(DB_NAME, tableName, DEFAULT_YEAR_VALUE, null);
    client.add_partition(partition);
    Partition resultPart =
        client.getPartition(DB_NAME, tableName, Lists.newArrayList(DEFAULT_YEAR_VALUE));
    Assert.assertNotNull(resultPart);
    Assert.assertNotNull(resultPart.getSd());
    String defaultTableLocation = metaStore.getWarehouseRoot() + "/" + DB_NAME + ".db/" + tableName;
    String defaulPartitionLocation = defaultTableLocation + "/year=2017";
    Assert.assertEquals(defaulPartitionLocation, resultPart.getSd().getLocation());
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionTooManyValues() throws Exception {

    createTable();
    Partition partition = buildPartition(Lists.newArrayList(DEFAULT_YEAR_VALUE, "march"),
        getYearAndMonthPartCols(), 1);
    client.add_partition(partition);
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionNoPartColOnTable() throws Exception {

    Table origTable = new TableBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .addCol("test_id", "int", "test col id")
        .addCol("test_value", "string", "test col value")
        .build();
    client.createTable(origTable);
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    client.add_partition(partition);
  }

  @Test(expected=MetaException.class)
  public void testAddPartitionNoColInPartition() throws Exception {

    createTable();
    Partition partition = new PartitionBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .addValue(DEFAULT_YEAR_VALUE)
        .setLocation(metaStore.getWarehouseRoot() + "/addparttest")
        .build();
    client.add_partition(partition);
  }

  @Test
  public void testAddPartitionDifferentNamesAndTypesInColAndTableCol() throws Exception {

    createTable();
    Partition partition = new PartitionBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .addValue("1000")
        .addCol("time", "int")
        .build();

    client.add_partition(partition);
    Partition part = client.getPartition(DB_NAME, TABLE_NAME, "year=1000");
    Assert.assertNotNull(part);
    Assert.assertEquals(TABLE_NAME, part.getTableName());
    Assert.assertEquals("1000", part.getValues().get(0));
    Assert.assertTrue(metaStore.isPathExists(new Path(part.getSd().getLocation())));
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionNoValueInPartition() throws Exception {

    createTable();
    Partition partition = new PartitionBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .addCol(YEAR_COL_NAME, DEFAULT_COL_TYPE)
        .setLocation(metaStore.getWarehouseRoot() + "/addparttest")
        .build();
    client.add_partition(partition);
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionMorePartColInTable() throws Exception {

    createTable(DB_NAME, TABLE_NAME, getYearAndMonthPartCols(), null);
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    client.add_partition(partition);
  }

  @Test
  public void testAddPartitionNullPartition() throws Exception {
    try {
      client.add_partition(null);
      Assert.fail("Exception should have been thrown.");
    } catch (TTransportException | NullPointerException e) {
      // TODO: NPE should not be thrown.
    }
  }

  @Test
  public void testAddPartitionNullValue() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, null);
    try {
      client.add_partition(partition);
    } catch (NullPointerException e) {
      // TODO: This works different in remote and embedded mode.
      // In embedded mode, no exception happens.
    }
  }

  @Test
  public void testAddPartitionEmptyValue() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, "");
    client.add_partition(partition);
    List<String> partitionNames = client.listPartitionNames(DB_NAME, TABLE_NAME, (short) 10);
    Assert.assertNotNull(partitionNames);
    Assert.assertTrue(partitionNames.size() == 1);
    Assert.assertEquals("year=__HIVE_DEFAULT_PARTITION__", partitionNames.get(0));
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionSetInvalidLocation() throws Exception {

    createTable();
    Partition partition =
        buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE, "%^#$$%#$testlocation/part1");
    client.add_partition(partition);
  }

  // Tests for int add_partitions(List<Partition> partitions) method

  @Test
  public void testAddPartitions() throws Exception {

    Table table = createTable();

    List<Partition> partitions = new ArrayList<>();
    Partition partition1 = buildPartition(Lists.newArrayList("2017"), getYearPartCol(), 1);
    Partition partition2 = buildPartition(Lists.newArrayList("2016"), getYearPartCol(), 2);
    Partition partition3 = buildPartition(Lists.newArrayList("2015"), getYearPartCol(), 3);
    partitions.add(partition1);
    partitions.add(partition2);
    partitions.add(partition3);
    int numberOfCreatedParts = client.add_partitions(partitions);
    Assert.assertEquals(3, numberOfCreatedParts);

    verifyPartition(table, "year=2017", Lists.newArrayList("2017"), 1);
    verifyPartition(table, "year=2016", Lists.newArrayList("2016"), 2);
    verifyPartition(table, "year=2015", Lists.newArrayList("2015"), 3);
  }

  @Test
  public void testAddPartitionsMultipleValues() throws Exception {

    Table table = createTable(DB_NAME, TABLE_NAME, getYearAndMonthPartCols(),
        metaStore.getWarehouseRoot() + "/" + TABLE_NAME);

    Partition partition1 =
        buildPartition(Lists.newArrayList("2017", "march"), getYearAndMonthPartCols(), 1);
    Partition partition2 =
        buildPartition(Lists.newArrayList("2017", "june"), getYearAndMonthPartCols(), 2);
    Partition partition3 =
        buildPartition(Lists.newArrayList("2016", "march"), getYearAndMonthPartCols(), 3);

    List<Partition> partitions = new ArrayList<>();
    partitions.add(partition1);
    partitions.add(partition2);
    partitions.add(partition3);
    client.add_partitions(partitions);

    verifyPartition(table, "year=2017/month=march", Lists.newArrayList("2017", "march"), 1);
    verifyPartition(table, "year=2017/month=june", Lists.newArrayList("2017", "june"), 2);
    verifyPartition(table, "year=2016/month=march", Lists.newArrayList("2016", "march"), 3);
  }

  @Test
  public void testAddPartitionsWithDefaultAttributes() throws Exception {

    Table table = createTable();

    Partition partition = new PartitionBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .addValue("2017")
        .setCols(getYearPartCol())
        .addCol("test_id", "int", "test col id")
        .addCol("test_value", "string", "test col value")
        .build();

    client.add_partitions(Lists.newArrayList(partition));

    // Check if the default values are set for all unfilled attributes
    List<Partition> parts =
        client.getPartitionsByNames(DB_NAME, TABLE_NAME, Lists.newArrayList("year=2017"));
    Assert.assertEquals(1, parts.size());
    Partition part = parts.get(0);
    Assert.assertNotNull(part);
    Assert.assertEquals(TABLE_NAME, part.getTableName());
    Assert.assertEquals(DB_NAME, part.getDbName());
    Assert.assertEquals(Lists.newArrayList("2017"), part.getValues());
    List<FieldSchema> cols = new ArrayList<>();
    cols.addAll(getYearPartCol());
    cols.add(new FieldSchema("test_id", "int", "test col id"));
    cols.add(new FieldSchema("test_value", "string", "test col value"));
    Assert.assertEquals(cols, part.getSd().getCols());
    verifyPartitionAttributesDefaultValues(part, table.getSd().getLocation());
  }

  @Test
  public void testAddPartitionsNullList() throws Exception {
    try {
      client.add_partitions(null);
      Assert.fail("Exception should have been thrown.");
    } catch (TTransportException | NullPointerException e) {
      // TODO: NPE should not be thrown
    }
  }

  @Test
  public void testAddPartitionsEmptyList() throws Exception {

    client.add_partitions(new ArrayList<Partition>());
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionsDifferentTable() throws Exception {

    String tableName1 = TABLE_NAME + "1";
    String tableName2 = TABLE_NAME + "2";
    createTable(DB_NAME, tableName1, null);
    createTable(DB_NAME, tableName2, null);

    Partition partition1 = buildPartition(DB_NAME, tableName1, "2017");
    Partition partition2 = buildPartition(DB_NAME, tableName2, "2016");
    Partition partition3 = buildPartition(DB_NAME, tableName1, "2018");

    List<Partition> partitions = new ArrayList<>();
    partitions.add(partition1);
    partitions.add(partition2);
    partitions.add(partition3);
    client.add_partitions(partitions);
  }

  @Test
  public void testAddPartitionsDifferentDBs() throws Exception {

    createDB("parttestdb2");
    createTable();
    createTable("parttestdb2", TABLE_NAME, null);

    Partition partition1 = buildPartition(DB_NAME, TABLE_NAME, "2017");
    Partition partition2 = buildPartition("parttestdb2", TABLE_NAME, "2016");
    Partition partition3 = buildPartition(DB_NAME, TABLE_NAME, "2018");

    List<Partition> partitions = new ArrayList<>();
    partitions.add(partition1);
    partitions.add(partition2);
    partitions.add(partition3);
    try {
      client.add_partitions(partitions);
      Assert.fail("MetaException should have been thrown.");
    } catch (MetaException e) {
      // Expected exception
    }
    client.dropDatabase("parttestdb2", true, true, true);
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionsDuplicateInTheList() throws Exception {

    createTable();

    Partition partition1 = buildPartition(DB_NAME, TABLE_NAME, "2017");
    Partition partition2 = buildPartition(DB_NAME, TABLE_NAME, "2016");
    Partition partition3 = buildPartition(DB_NAME, TABLE_NAME, "2017");

    List<Partition> partitions = new ArrayList<>();
    partitions.add(partition1);
    partitions.add(partition2);
    partitions.add(partition3);
    client.add_partitions(partitions);
  }

  @Test
  public void testAddPartitionsWithSameNameInTheListCaseSensitive() throws Exception {

    createTable();

    Partition partition1 = buildPartition(DB_NAME, TABLE_NAME, "this");
    Partition partition2 = buildPartition(DB_NAME, TABLE_NAME, "next");
    Partition partition3 = buildPartition(DB_NAME, TABLE_NAME, "THIS");

    List<Partition> partitions = new ArrayList<>();
    partitions.add(partition1);
    partitions.add(partition2);
    partitions.add(partition3);
    client.add_partitions(partitions);

    List<String> parts = client.listPartitionNames(DB_NAME, TABLE_NAME, MAX);
    Assert.assertEquals(3, parts.size());
    Assert.assertTrue(parts.contains("year=this"));
    Assert.assertTrue(parts.contains("year=next"));
    Assert.assertTrue(parts.contains("year=THIS"));
  }

  @Test(expected = AlreadyExistsException.class)
  public void testAddPartitionsAlreadyExists() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, "2017");
    client.add_partition(partition);

    Partition partition1 = buildPartition(DB_NAME, TABLE_NAME, "2015");
    Partition partition2 = buildPartition(DB_NAME, TABLE_NAME, "2017");
    Partition partition3 = buildPartition(DB_NAME, TABLE_NAME, "2016");

    List<Partition> partitions = new ArrayList<>();
    partitions.add(partition1);
    partitions.add(partition2);
    partitions.add(partition3);
    client.add_partitions(partitions);
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionsNonExistingTable() throws Exception {

    createTable();
    Partition partition1 = buildPartition(DB_NAME, TABLE_NAME, "2016");
    Partition partition2 = buildPartition(DB_NAME, "nonexistingtable", "2017");

    List<Partition> partitions = new ArrayList<>();
    partitions.add(partition1);
    partitions.add(partition2);
    client.add_partitions(partitions);
  }

  @Test(expected = InvalidObjectException.class)
  public void testAddPartitionsNonExistingDb() throws Exception {

    createTable();
    Partition partition1 = buildPartition("nonexistingdb", TABLE_NAME, "2017");
    Partition partition2 = buildPartition(DB_NAME, TABLE_NAME, "2016");

    List<Partition> partitions = new ArrayList<>();
    partitions.add(partition1);
    partitions.add(partition2);
    client.add_partitions(partitions);
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionsNullDb() throws Exception {

    createTable();
    Partition partition1 = buildPartition(DB_NAME, TABLE_NAME, "2016");
    Partition partition2 = buildPartition(DB_NAME, TABLE_NAME, "2017");
    partition2.setDbName(null);

    List<Partition> partitions = new ArrayList<>();
    partitions.add(partition1);
    partitions.add(partition2);
    client.add_partitions(partitions);
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionsEmptyDb() throws Exception {

    createTable();
    Partition partition1 = buildPartition(DB_NAME, TABLE_NAME, "2016");
    Partition partition2 = buildPartition("", TABLE_NAME, "2017");

    List<Partition> partitions = new ArrayList<>();
    partitions.add(partition1);
    partitions.add(partition2);
    client.add_partitions(partitions);
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionsNullTable() throws Exception {

    createTable();
    Partition partition1 = buildPartition(DB_NAME, TABLE_NAME, "2016");
    Partition partition2 = buildPartition(DB_NAME, TABLE_NAME, "2017");
    partition2.setTableName(null);

    List<Partition> partitions = new ArrayList<>();
    partitions.add(partition1);
    partitions.add(partition2);
    client.add_partitions(partitions);
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionsEmptyTable() throws Exception {

    createTable();
    Partition partition1 = buildPartition(DB_NAME, TABLE_NAME, "2016");
    Partition partition2 = buildPartition(DB_NAME, "", "2017");

    List<Partition> partitions = new ArrayList<>();
    partitions.add(partition1);
    partitions.add(partition2);
    client.add_partitions(partitions);
  }

  @Test
  public void testAddPartitionsOneInvalid() throws Exception {

    createTable();
    String tableLocation = metaStore.getWarehouseRoot() + "/" + TABLE_NAME;
    Partition partition1 = buildPartition(DB_NAME, TABLE_NAME, "2016", tableLocation + "/year=2016");
    Partition partition2 = buildPartition(DB_NAME, TABLE_NAME, "2017", tableLocation + "/year=2017");
    Partition partition3 =
        buildPartition(Lists.newArrayList("2015", "march"), getYearAndMonthPartCols(), 1);
    partition3.getSd().setLocation(tableLocation + "/year=2015/month=march");

    List<Partition> partitions = new ArrayList<>();
    partitions.add(partition1);
    partitions.add(partition2);
    partitions.add(partition3);

    try {
      client.add_partitions(partitions);
      Assert.fail("MetaException should have happened.");
    } catch (MetaException e) {
      // Expected exception
    }

    List<Partition> parts = client.listPartitions(DB_NAME, TABLE_NAME, MAX);
    Assert.assertNotNull(parts);
    Assert.assertTrue(parts.isEmpty());
    // TODO: This does not work correctly. None of the partitions is created, but the folder
    // for the first two is created. It is because in HiveMetaStore.add_partitions_core when
    // going through the partitions, the first two are already put and started in the thread
    // pool when the exception occurs in the third one. When the exception occurs, we go to
    // the finally part, but the map can be empty (it depends on the progress of the other
    // threads) so the folders won't be deleted.
//    Assert.assertFalse(metaStore.isPathExists(new Path(tableLocation + "/year=2016")));
//    Assert.assertFalse(metaStore.isPathExists(new Path(tableLocation + "/year=2017")));
    Assert.assertFalse(metaStore.isPathExists(new Path(tableLocation + "/year=2015/month=march")));
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionsNullSd() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    partition.setSd(null);
    List<Partition> partitions = new ArrayList<>();
    partitions.add(partition);
    client.add_partitions(partitions);
  }

  @Test
  public void testAddPartitionsNullColsInSd() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    partition.getSd().setCols(null);
    client.add_partitions(Lists.newArrayList(partition));

    // TODO: Not sure that this is the correct behavior. It doesn't make sense to create the
    // partition without column info. This should be investigated later.
    Partition part =
        client.getPartition(DB_NAME, TABLE_NAME, Lists.newArrayList(DEFAULT_YEAR_VALUE));
    Assert.assertNotNull(part);
    Assert.assertNull(part.getSd().getCols());
  }

  @Test
  public void testAddPartitionsEmptyColsInSd() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    partition.getSd().setCols(new ArrayList<FieldSchema>());
    client.add_partitions(Lists.newArrayList(partition));

    // TODO: Not sure that this is the correct behavior. It doesn't make sense to create the
    // partition without column info. This should be investigated later.
    Partition part =
        client.getPartition(DB_NAME, TABLE_NAME, Lists.newArrayList(DEFAULT_YEAR_VALUE));
    Assert.assertNotNull(part);
    Assert.assertTrue(part.getSd().getCols().isEmpty());
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionsNullColTypeInSd() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    partition.getSd().getCols().get(0).setType(null);
    client.add_partitions(Lists.newArrayList(partition));
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionsNullColNameInSd() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    partition.getSd().getCols().get(0).setName(null);
    client.add_partitions(Lists.newArrayList(partition));
  }

  @Test
  public void testAddPartitionsInvalidColTypeInSd() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    partition.getSd().getCols().get(0).setType("xyz");
    client.add_partitions(Lists.newArrayList(partition));

    // TODO: Not sure that this is the correct behavior. It doesn't make sense to create the
    // partition with column with invalid type. This should be investigated later.
    Partition part =
        client.getPartition(DB_NAME, TABLE_NAME, Lists.newArrayList(DEFAULT_YEAR_VALUE));
    Assert.assertNotNull(part);
    Assert.assertEquals("xyz", part.getSd().getCols().get(0).getType());
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionsEmptySerdeInfo() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    partition.getSd().setSerdeInfo(null);
    client.add_partitions(Lists.newArrayList(partition));
  }

  @Test
  public void testAddPartitionNullAndEmptyLocation() throws Exception {

    createTable(DB_NAME, TABLE_NAME, metaStore.getWarehouseRoot() + "/addparttest2");
    List<Partition> partitions = new ArrayList<>();
    Partition partition1 = buildPartition(DB_NAME, TABLE_NAME, "2017", null);
    Partition partition2 = buildPartition(DB_NAME, TABLE_NAME, "2016", "");

    partitions.add(partition1);
    partitions.add(partition2);
    client.add_partitions(partitions);

    Partition part1 = client.getPartition(DB_NAME, TABLE_NAME, "year=2017");
    Assert.assertEquals(metaStore.getWarehouseRoot() + "/addparttest2/year=2017",
        part1.getSd().getLocation());
    Assert.assertTrue(metaStore.isPathExists(new Path(part1.getSd().getLocation())));
    Partition part2 = client.getPartition(DB_NAME, TABLE_NAME, "year=2016");
    Assert.assertEquals(metaStore.getWarehouseRoot() + "/addparttest2/year=2016",
        part2.getSd().getLocation());
    Assert.assertTrue(metaStore.isPathExists(new Path(part2.getSd().getLocation())));
  }

  @Test
  public void testAddPartitionsNullLocationInTableToo() throws Exception {

    createTable(DB_NAME, TABLE_NAME, null);
    List<Partition> partitions = new ArrayList<>();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE, null);
    partitions.add(partition);
    client.add_partitions(partitions);

    Partition part = client.getPartition(DB_NAME, TABLE_NAME, "year=2017");
    Assert.assertEquals(
        metaStore.getWarehouseRoot() + "/test_partition_db.db/test_partition_table/year=2017",
        part.getSd().getLocation());
    Assert.assertTrue(metaStore.isPathExists(new Path(part.getSd().getLocation())));
  }

  @Test(expected=MetaException.class)
  public void testAddPartitionsForView() throws Exception {

    Table table = new TableBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .setType("VIRTUAL_VIEW")
        .addCol("test_id", "int", "test col id")
        .addCol("test_value", "string", "test col value")
        .addPartCol(YEAR_COL_NAME, DEFAULT_COL_TYPE)
        .setLocation(null)
        .build();
    client.createTable(table);
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    List<Partition> partitions = Lists.newArrayList(partition);
    client.add_partitions(partitions);
  }

  @Test
  public void testAddPartitionsForExternalTable() throws Exception {

    String tableName = "part_add_ext_table";
    String tableLocation = metaStore.getWarehouseRoot() + "/" + tableName;
    createExternalTable(tableName, tableLocation);
    String location1 = tableLocation + "/addparttest2017";
    String location2 = tableLocation + "/addparttest2018";
    Partition partition1 = buildPartition(DB_NAME, tableName, "2017", location1);
    Partition partition2 = buildPartition(DB_NAME, tableName, "2018", location2);
    List<Partition> partitions = Lists.newArrayList(partition1, partition2);
    client.add_partitions(partitions);

    List<Partition> resultParts = client.getPartitionsByNames(DB_NAME, tableName,
        Lists.newArrayList("year=2017", "year=2018"));
    Assert.assertNotNull(resultParts);
    Assert.assertEquals(2, resultParts.size());
    if (resultParts.get(0).getValues().get(0).equals("2017")) {
      Assert.assertEquals(location1, resultParts.get(0).getSd().getLocation());
      Assert.assertEquals(location2, resultParts.get(1).getSd().getLocation());
    } else {
      Assert.assertEquals(location2, resultParts.get(0).getSd().getLocation());
      Assert.assertEquals(location1, resultParts.get(1).getSd().getLocation());
    }
  }

  @Test
  public void testAddPartitionsForExternalTableNullLocation() throws Exception {

    String tableName = "part_add_ext_table";
    createExternalTable(tableName, null);
    Partition partition1 = buildPartition(DB_NAME, tableName, "2017", null);
    Partition partition2 = buildPartition(DB_NAME, tableName, "2018", null);
    List<Partition> partitions = Lists.newArrayList(partition1, partition2);
    client.add_partitions(partitions);

    List<Partition> resultParts = client.getPartitionsByNames(DB_NAME, tableName,
        Lists.newArrayList("year=2017", "year=2018"));
    Assert.assertNotNull(resultParts);
    Assert.assertEquals(2, resultParts.size());
    String defaultTableLocation = metaStore.getWarehouseRoot() + "/" + DB_NAME + ".db/" + tableName;
    String defaultPartLocation1 = defaultTableLocation + "/year=2017";
    String defaultPartLocation2 = defaultTableLocation + "/year=2018";
    if (resultParts.get(0).getValues().get(0).equals("2017")) {
      Assert.assertEquals(defaultPartLocation1, resultParts.get(0).getSd().getLocation());
      Assert.assertEquals(defaultPartLocation2, resultParts.get(1).getSd().getLocation());
    } else {
      Assert.assertEquals(defaultPartLocation2, resultParts.get(0).getSd().getLocation());
      Assert.assertEquals(defaultPartLocation1, resultParts.get(1).getSd().getLocation());
    }
  }

  @Test(expected=MetaException.class)
  public void testAddPartitionsNoValueInPartition() throws Exception {

    createTable();
    Partition partition = new PartitionBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .addCol(YEAR_COL_NAME, DEFAULT_COL_TYPE)
        .setLocation(metaStore.getWarehouseRoot() + "/addparttest")
        .build();
    List<Partition> partitions = new ArrayList<>();
    partitions.add(partition);
    client.add_partitions(partitions);
  }

  @Test(expected=MetaException.class)
  public void testAddPartitionsMorePartColInTable() throws Exception {

    createTable(DB_NAME, TABLE_NAME, getYearAndMonthPartCols(), null);
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    List<Partition> partitions = new ArrayList<>();
    partitions.add(partition);
    client.add_partitions(partitions);
  }

  @Test
  public void testAddPartitionsNullPartition() throws Exception {
    try {
      List<Partition> partitions = new ArrayList<>();
      partitions.add(null);
      client.add_partitions(partitions);
      Assert.fail("Exception should have been thrown.");
    } catch (TTransportException | NullPointerException e) {
      // TODO: NPE should not be thrown
    }
  }

  @Test
  public void testAddPartitionsNullValue() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, null);
    List<Partition> partitions = new ArrayList<>();
    partitions.add(partition);
    try {
      client.add_partitions(partitions);
    } catch (NullPointerException e) {
      // TODO: This works different in remote and embedded mode.
      // In embedded mode, no exception happens.
    }
  }

  @Test
  public void testAddPartitionsEmptyValue() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, "");
    List<Partition> partitions = new ArrayList<>();
    partitions.add(partition);
    client.add_partitions(partitions);

    List<String> partitionNames = client.listPartitionNames(DB_NAME, TABLE_NAME, MAX);
    Assert.assertNotNull(partitionNames);
    Assert.assertTrue(partitionNames.size() == 1);
    Assert.assertEquals("year=__HIVE_DEFAULT_PARTITION__", partitionNames.get(0));
  }

  // Tests for List<Partition> add_partitions(List<Partition> partitions,
  // boolean ifNotExists, boolean needResults) method

  @Test
  public void testAddParts() throws Exception {

    Table table = createTable();

    List<Partition> partitions = new ArrayList<>();
    Partition partition1 = buildPartition(Lists.newArrayList("2017"), getYearPartCol(), 1);
    Partition partition2 = buildPartition(Lists.newArrayList("2016"), getYearPartCol(), 2);
    Partition partition3 = buildPartition(Lists.newArrayList("2015"), getYearPartCol(), 3);
    partitions.add(partition1);
    partitions.add(partition2);
    partitions.add(partition3);

    List<Partition> addedPartitions = client.add_partitions(partitions, false, false);
    Assert.assertNull(addedPartitions);
    verifyPartition(table, "year=2017", Lists.newArrayList("2017"), 1);
    verifyPartition(table, "year=2016", Lists.newArrayList("2016"), 2);
    verifyPartition(table, "year=2015", Lists.newArrayList("2015"), 3);
  }

  @Test
  public void testAddPartsMultipleValues() throws Exception {

    Table table = createTable(DB_NAME, TABLE_NAME, getYearAndMonthPartCols(),
        metaStore.getWarehouseRoot() + "/" + TABLE_NAME);

    Partition partition1 =
        buildPartition(Lists.newArrayList("2017", "march"), getYearAndMonthPartCols(), 1);
    Partition partition2 =
        buildPartition(Lists.newArrayList("2017", "june"), getYearAndMonthPartCols(), 2);
    Partition partition3 =
        buildPartition(Lists.newArrayList("2016", "march"), getYearAndMonthPartCols(), 3);

    List<Partition> partitions = new ArrayList<>();
    partitions.add(partition1);
    partitions.add(partition2);
    partitions.add(partition3);
    List<Partition> addedPartitions = client.add_partitions(partitions, false, true);
    Assert.assertNotNull(addedPartitions);
    Assert.assertEquals(3, addedPartitions.size());
    verifyPartition(table, "year=2017/month=march", Lists.newArrayList("2017", "march"), 1);
    verifyPartition(table, "year=2017/month=june", Lists.newArrayList("2017", "june"), 2);
    verifyPartition(table, "year=2016/month=march", Lists.newArrayList("2016", "march"), 3);
  }

  @Test(expected = NullPointerException.class)
  public void testAddPartsNullList() throws Exception {
    // TODO: NPE should not be thrown
    client.add_partitions(null, false, false);
  }

  @Test
  public void testAddPartsEmptyList() throws Exception {

    List<Partition> addedPartitions =
        client.add_partitions(new ArrayList<Partition>(), false, true);
    Assert.assertNotNull(addedPartitions);
    Assert.assertTrue(addedPartitions.isEmpty());
  }

  @Test(expected = MetaException.class)
  public void testAddPartsDifferentTable() throws Exception {

    String tableName1 = TABLE_NAME + "1";
    String tableName2 = TABLE_NAME + "2";
    createTable(DB_NAME, tableName1, null);
    createTable(DB_NAME, tableName2, null);

    Partition partition1 = buildPartition(DB_NAME, tableName1, "2017");
    Partition partition2 = buildPartition(DB_NAME, tableName2, "2016");
    Partition partition3 = buildPartition(DB_NAME, tableName1, "2018");

    List<Partition> partitions = new ArrayList<>();
    partitions.add(partition1);
    partitions.add(partition2);
    partitions.add(partition3);
    client.add_partitions(partitions, false, false);
  }

  @Test
  public void testAddPartsDifferentDBs() throws Exception {

    createDB("parttestdb2");
    createTable();
    createTable("parttestdb2", TABLE_NAME, null);

    Partition partition1 = buildPartition(DB_NAME, TABLE_NAME, "2017");
    Partition partition2 = buildPartition("parttestdb2", TABLE_NAME, "2016");
    Partition partition3 = buildPartition(DB_NAME, TABLE_NAME, "2018");

    List<Partition> partitions = new ArrayList<>();
    partitions.add(partition1);
    partitions.add(partition2);
    partitions.add(partition3);
    try {
      client.add_partitions(partitions, false, false);
      Assert.fail("MetaException should have been thrown.");
    } catch (MetaException e) {
      // Expected exception
    }
    client.dropDatabase("parttestdb2", true, true, true);
  }

  @Test(expected = MetaException.class)
  public void testAddPartsDuplicateInTheList() throws Exception {

    createTable();

    Partition partition1 = buildPartition(DB_NAME, TABLE_NAME, "2017");
    Partition partition2 = buildPartition(DB_NAME, TABLE_NAME, "2016");
    Partition partition3 = buildPartition(DB_NAME, TABLE_NAME, "2017");

    List<Partition> partitions = new ArrayList<>();
    partitions.add(partition1);
    partitions.add(partition2);
    partitions.add(partition3);
    client.add_partitions(partitions, true, false);
  }

  @Test(expected = AlreadyExistsException.class)
  public void testAddPartsAlreadyExists() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, "2017");
    client.add_partition(partition);

    Partition partition1 = buildPartition(DB_NAME, TABLE_NAME, "2015");
    Partition partition2 = buildPartition(DB_NAME, TABLE_NAME, "2017");
    Partition partition3 = buildPartition(DB_NAME, TABLE_NAME, "2016");

    List<Partition> partitions = new ArrayList<>();
    partitions.add(partition1);
    partitions.add(partition2);
    partitions.add(partition3);
    client.add_partitions(partitions, false, false);
  }

  @Test
  public void testAddPartsAlreadyExistsIfExistsTrue() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, "2017");
    client.add_partition(partition);

    Partition partition1 = buildPartition(DB_NAME, TABLE_NAME, "2015");
    Partition partition2 = buildPartition(DB_NAME, TABLE_NAME, "2017");
    Partition partition3 = buildPartition(DB_NAME, TABLE_NAME, "2016");

    List<Partition> partitions = new ArrayList<>();
    partitions.add(partition1);
    partitions.add(partition2);
    partitions.add(partition3);
    List<Partition> addedPartitions = client.add_partitions(partitions, true, true);
    Assert.assertEquals(2, addedPartitions.size());
    List<String> partitionNames = client.listPartitionNames(DB_NAME, TABLE_NAME, MAX);
    Assert.assertEquals(3, partitionNames.size());
    Assert.assertTrue(partitionNames.contains("year=2015"));
    Assert.assertTrue(partitionNames.contains("year=2016"));
    Assert.assertTrue(partitionNames.contains("year=2017"));
  }

  @Test(expected = NullPointerException.class)
  public void testAddPartsNullPartition() throws Exception {
    // TODO: NPE should not be thrown
    List<Partition> partitions = new ArrayList<>();
    partitions.add(null);
    client.add_partitions(partitions, false, false);
  }

  // Helper methods
  private void createDB(String dbName) throws TException {
    Database db = new DatabaseBuilder().setName(dbName).build();
    client.createDatabase(db);
  }

  private Table createTable() throws Exception {
    return createTable(DB_NAME, TABLE_NAME, metaStore.getWarehouseRoot() + "/" + TABLE_NAME);
  }

  private Table createTable(String dbName, String tableName, String location) throws Exception {
    return createTable(dbName, tableName, getYearPartCol(), location);
  }

  private Table createTable(String dbName, String tableName, List<FieldSchema> partCols,
      String location) throws Exception {
    Table table = new TableBuilder()
        .setDbName(dbName)
        .setTableName(tableName)
        .addCol("test_id", "int", "test col id")
        .addCol("test_value", "string", "test col value")
        .addTableParam("partTestTableParamKey", "partTestTableParamValue")
        .setPartCols(partCols)
        .addStorageDescriptorParam("partTestSDParamKey", "partTestSDParamValue")
        .setSerdeName(tableName)
        .setStoredAsSubDirectories(false)
        .addSerdeParam("partTestSerdeParamKey", "partTestSerdeParamValue")
        .setLocation(location)
        .build();
    client.createTable(table);
    return client.getTable(dbName, tableName);
  }

  private void createExternalTable(String tableName, String location) throws Exception {
    Table table = new TableBuilder()
        .setDbName(DB_NAME)
        .setTableName(tableName)
        .addCol("test_id", "int", "test col id")
        .addCol("test_value", DEFAULT_COL_TYPE, "test col value")
        .addPartCol(YEAR_COL_NAME, DEFAULT_COL_TYPE)
        .addTableParam("EXTERNAL", "TRUE")
        .setLocation(location)
        .build();
    client.createTable(table);
  }

  private Partition buildPartition(String dbName, String tableName, String value)
      throws MetaException {
    return buildPartition(dbName, tableName, value,
        metaStore.getWarehouseRoot() + "/" + tableName + "/addparttest");
  }

  private Partition buildPartition(String dbName, String tableName, String value,
      String location) throws MetaException {
    Partition partition = new PartitionBuilder()
        .setDbName(dbName)
        .setTableName(tableName)
        .addValue(value)
        .addCol(YEAR_COL_NAME, DEFAULT_COL_TYPE)
        .addCol("test_id", "int", "test col id")
        .addCol("test_value", "string", "test col value")
        .addPartParam(DEFAULT_PARAM_KEY, DEFAULT_PARAM_VALUE)
        .setLocation(location)
        .build();
    return partition;
  }

  private Partition buildPartition(List<String> values, List<FieldSchema> partCols,
      int index) throws MetaException {
    Partition partition = new PartitionBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .setValues(values)
        .addPartParam(DEFAULT_PARAM_KEY + index, DEFAULT_PARAM_VALUE + index)
        .setInputFormat("TestInputFormat" + index)
        .setOutputFormat("TestOutputFormat" + index)
        .setSerdeName("partserde" + index)
        .addStorageDescriptorParam("partsdkey" + index, "partsdvalue" + index)
        .setCols(partCols)
        .setCreateTime(123456)
        .setLastAccessTime(123456)
        .addCol("test_id", "int", "test col id")
        .addCol("test_value", "string", "test col value")
        .build();
    return partition;
  }

  private static List<FieldSchema> getYearAndMonthPartCols() {
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema(YEAR_COL_NAME, DEFAULT_COL_TYPE, "year part col"));
    cols.add(new FieldSchema(MONTH_COL_NAME, DEFAULT_COL_TYPE, "month part col"));
    return cols;
  }

  private static List<FieldSchema> getYearPartCol() {
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema(YEAR_COL_NAME, DEFAULT_COL_TYPE, "year part col"));
    return cols;
  }

  private static List<FieldSchema> getMonthPartCol() {
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema(MONTH_COL_NAME, DEFAULT_COL_TYPE, "month part col"));
    return cols;
  }

  private void verifyPartition(Table table, String name, List<String> values, int index)
      throws Exception {

    Partition part = client.getPartition(table.getDbName(), table.getTableName(), name);
    Assert.assertNotNull("The partition should not be null.", part);
    Assert.assertEquals("The table name in the partition is not correct.", table.getTableName(),
        part.getTableName());
    List<String> partValues = part.getValues();
    Assert.assertEquals(values.size(), partValues.size());
    Assert.assertTrue("The partition has wrong values.", partValues.containsAll(values));
    Assert.assertEquals("The DB name in the partition is not correct.", table.getDbName(),
        part.getDbName());
    Assert.assertEquals("The last access time is not correct.", 123456, part.getLastAccessTime());
    Assert.assertNotEquals(123456, part.getCreateTime());
    Assert.assertEquals(
        "The partition's parameter map should contain the partparamkey - partparamvalue pair.",
        DEFAULT_PARAM_VALUE + index, part.getParameters().get(DEFAULT_PARAM_KEY + index));
    StorageDescriptor sd = part.getSd();
    Assert.assertNotNull("The partition's storage descriptor must not be null.", sd);
    Assert.assertEquals("The input format is not correct.", "TestInputFormat" + index,
        sd.getInputFormat());
    Assert.assertEquals("The output format is not correct.", "TestOutputFormat" + index,
        sd.getOutputFormat());
    Assert.assertEquals("The serdeInfo name is not correct.", "partserde" + index,
        sd.getSerdeInfo().getName());
    Assert.assertEquals(
        "The parameter map of the partition's storage descriptor should contain the partsdkey - partsdvalue pair.",
        "partsdvalue" + index, sd.getParameters().get("partsdkey" + index));
    Assert.assertEquals("The parameter's location is not correct.",
        metaStore.getWarehouseRoot() + "/" + TABLE_NAME + "/" + name, sd.getLocation());
    Assert.assertTrue("The parameter's location should exist on the file system.",
        metaStore.isPathExists(new Path(sd.getLocation())));
    // If the 'metastore.partition.inherit.table.properties' property is set in the metastore
    // config, the partition inherits the listed table parameters.
    // This property is not set in this test, therefore the partition doesn't inherit the table
    // parameters.
    Assert.assertFalse("The partition should not inherit the table parameters.",
        part.getParameters().keySet().contains(table.getParameters().keySet()));
  }

  private void verifyPartitionAttributesDefaultValues(Partition partition, String tableLocation) {
    Assert.assertNotEquals("The partition's last access time should be set.", 0,
        partition.getLastAccessTime());
    Assert.assertNotEquals("The partition's create time should be set.", 0,
        partition.getCreateTime());
    Assert.assertEquals(
        "The partition has to have the 'transient_lastDdlTime' parameter per default.", 1,
        partition.getParameters().size());
    Assert.assertNotNull(
        "The partition has to have the 'transient_lastDdlTime' parameter per default.",
        partition.getParameters().get("transient_lastDdlTime"));
    StorageDescriptor sd = partition.getSd();
    Assert.assertNotNull("The storage descriptor of the partition must not be null.", sd);
    Assert.assertEquals("The partition location is not correct.", tableLocation + "/year=2017",
        sd.getLocation());
    Assert.assertEquals("The input format doesn't have the default value.",
        "org.apache.hadoop.hive.ql.io.HiveInputFormat", sd.getInputFormat());
    Assert.assertEquals("The output format doesn't have the default value.",
        "org.apache.hadoop.hive.ql.io.HiveOutputFormat", sd.getOutputFormat());
    Assert.assertFalse("The compressed attribute doesn't have the default value.",
        sd.isCompressed());
    Assert.assertFalse("The storedAsSubDirectories attribute doesn't have the default value.",
        sd.isStoredAsSubDirectories());
    Assert.assertEquals("The numBuckets attribute doesn't have the default value.", 0,
        sd.getNumBuckets());
    Assert.assertTrue("The default value of the attribute 'bucketCols' should be an empty list.",
        sd.getBucketCols().isEmpty());
    Assert.assertTrue("The default value of the attribute 'sortCols' should be an empty list.",
        sd.getSortCols().isEmpty());
    Assert.assertTrue("Per default the storage descriptor parameters should be empty.",
        sd.getParameters().isEmpty());
    SerDeInfo serdeInfo = sd.getSerdeInfo();
    Assert.assertNotNull("The serdeInfo attribute should not be null.", serdeInfo);
    Assert.assertNull("The default value of the serde's name attribute should be null.",
        serdeInfo.getName());
    Assert.assertEquals("The serde's 'serializationLib' attribute doesn't have the default value.",
        "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", serdeInfo.getSerializationLib());
    Assert.assertTrue("Per default the serde info parameters should be empty.",
        serdeInfo.getParameters().isEmpty());
    SkewedInfo skewedInfo = sd.getSkewedInfo();
    Assert.assertTrue("Per default the skewedInfo column names list should be empty.",
        skewedInfo.getSkewedColNames().isEmpty());
    Assert.assertTrue("Per default the skewedInfo column value list should be empty.",
        skewedInfo.getSkewedColValues().isEmpty());
    Assert.assertTrue("Per default the skewedInfo column value location map should be empty.",
        skewedInfo.getSkewedColValueLocationMaps().isEmpty());
  }
}
