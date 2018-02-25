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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
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
 * Tests for appending partitions.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestAppendPartitions {

  // Needed until there is no junit release with @BeforeParam, @AfterParam (junit 4.13)
  // https://github.com/junit-team/junit4/commit/1bf8438b65858565dbb64736bfe13aae9cfc1b5a
  // Then we should remove our own copy
  private static Set<AbstractMetaStoreService> metaStoreServices = null;
  private AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;

  private static final String DB_NAME = "test_append_part_db";
  private static Table tableWithPartitions;
  private static Table externalTable;
  private static Table tableNoPartColumns;
  private static Table tableView;

  @Parameterized.Parameters(name = "{0}")
  public static List<Object[]> getMetaStoreToTest() throws Exception {
    List<Object[]> result = MetaStoreFactoryForTests.getMetaStores();
    metaStoreServices = result.stream()
                            .map(test -> (AbstractMetaStoreService)test[1])
                            .collect(Collectors.toSet());
    return result;
  }

  public TestAppendPartitions(String name, AbstractMetaStoreService metaStore) throws Exception {
    this.metaStore = metaStore;
    this.metaStore.start();
  }

  // Needed until there is no junit release with @BeforeParam, @AfterParam (junit 4.13)
  // https://github.com/junit-team/junit4/commit/1bf8438b65858565dbb64736bfe13aae9cfc1b5a
  // Then we should move this to @AfterParam
  @AfterClass
  public static void stopMetaStores() throws Exception {
    for (AbstractMetaStoreService metaStoreService : metaStoreServices) {
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
    Database db = new DatabaseBuilder()
        .setName(DB_NAME)
        .build();
    client.createDatabase(db);

    tableWithPartitions = createTableWithPartitions();
    externalTable = createExternalTable();
    tableNoPartColumns = createTableNoPartitionColumns();
    tableView = createView();
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

  // Tests for Partition appendPartition(String tableName, String dbName, List<String> partVals) method

  @Test
  public void testAppendPartition() throws Exception {

    List<String> partitionValues = Lists.newArrayList("2017", "may");
    Table table = tableWithPartitions;

    Partition appendedPart =
        client.appendPartition(table.getDbName(), table.getTableName(), partitionValues);

    Assert.assertNotNull(appendedPart);
    Partition partition =
        client.getPartition(table.getDbName(), table.getTableName(), partitionValues);
    Assert.assertEquals(partition, appendedPart);
    verifyPartition(partition, table, partitionValues, "year=2017/month=may");
    verifyPartitionNames(table, Lists.newArrayList("year=2017/month=march", "year=2017/month=april",
        "year=2018/month=march", "year=2017/month=may"));
  }

  @Test
  public void testAppendPartitionToExternalTable() throws Exception {

    List<String> partitionValues = Lists.newArrayList("2017", "may");
    Table table = externalTable;

    Partition appendedPart =
        client.appendPartition(table.getDbName(), table.getTableName(), partitionValues);

    Assert.assertNotNull(appendedPart);
    Partition partition =
        client.getPartition(table.getDbName(), table.getTableName(), partitionValues);
    Assert.assertEquals(partition, appendedPart);
    verifyPartition(partition, table, partitionValues, "year=2017/month=may");
    verifyPartitionNames(table, Lists.newArrayList("year=2017/month=may"));
  }

  @Test
  public void testAppendPartitionMultiplePartitions() throws Exception {

    List<String> partitionValues1 = Lists.newArrayList("2017", "may");
    List<String> partitionValues2 = Lists.newArrayList("2018", "may");
    List<String> partitionValues3 = Lists.newArrayList("2017", "june");

    Table table = tableWithPartitions;

    client.appendPartition(table.getDbName(), table.getTableName(), partitionValues1);
    client.appendPartition(table.getDbName(), table.getTableName(), partitionValues2);
    client.appendPartition(table.getDbName(), table.getTableName(), partitionValues3);

    verifyPartitionNames(table,
        Lists.newArrayList("year=2017/month=may", "year=2018/month=may", "year=2017/month=june",
            "year=2017/month=march", "year=2017/month=april", "year=2018/month=march"));
  }

  @Test(expected = MetaException.class)
  public void testAppendPartitionToTableWithoutPartCols() throws Exception {

    List<String> partitionValues = Lists.newArrayList("2017", "may");
    Table table = tableNoPartColumns;
    client.appendPartition(table.getDbName(), table.getTableName(), partitionValues);
  }

  @Test(expected = MetaException.class)
  public void testAppendPartitionToView() throws Exception {

    List<String> partitionValues = Lists.newArrayList("2017", "may");
    Table table = tableView;
    client.appendPartition(table.getDbName(), table.getTableName(), partitionValues);
  }

  @Test(expected = AlreadyExistsException.class)
  public void testAppendPartitionAlreadyExists() throws Exception {

    List<String> partitionValues = Lists.newArrayList("2017", "april");
    Table table = tableWithPartitions;
    client.appendPartition(table.getDbName(), table.getTableName(), partitionValues);
  }

  @Test(expected = InvalidObjectException.class)
  public void testAppendPartitionNonExistingDB() throws Exception {

    List<String> partitionValues = Lists.newArrayList("2017", "may");
    client.appendPartition("nonexistingdb", tableWithPartitions.getTableName(), partitionValues);
  }

  @Test(expected = InvalidObjectException.class)
  public void testAppendPartitionNonExistingTable() throws Exception {

    List<String> partitionValues = Lists.newArrayList("2017", "may");
    client.appendPartition(tableWithPartitions.getDbName(), "nonexistingtable", partitionValues);
  }

  @Test(expected = InvalidObjectException.class)
  public void testAppendPartitionEmptyDB() throws Exception {

    List<String> partitionValues = Lists.newArrayList("2017", "may");
    client.appendPartition("", tableWithPartitions.getTableName(), partitionValues);
  }

  @Test(expected = InvalidObjectException.class)
  public void testAppendPartitionEmptyTable() throws Exception {

    List<String> partitionValues = Lists.newArrayList("2017", "may");
    client.appendPartition(tableWithPartitions.getDbName(), "", partitionValues);
  }

  @Test(expected = MetaException.class)
  public void testAppendPartitionNullDB() throws Exception {

    List<String> partitionValues = Lists.newArrayList("2017", "may");
    client.appendPartition(null, tableWithPartitions.getTableName(), partitionValues);
  }

  @Test(expected = MetaException.class)
  public void testAppendPartitionNullTable() throws Exception {

    List<String> partitionValues = Lists.newArrayList("2017", "may");
    client.appendPartition(tableWithPartitions.getDbName(), null, partitionValues);
  }

  @Test(expected = MetaException.class)
  public void testAppendPartitionEmptyPartValues() throws Exception {

    Table table = tableWithPartitions;
    client.appendPartition(table.getDbName(), table.getTableName(), new ArrayList<String>());
  }

  @Test
  public void testAppendPartitionNullPartValues() throws Exception {

    try {
      Table table = tableWithPartitions;
      client.appendPartition(table.getDbName(), table.getTableName(), (List<String>) null);
      Assert.fail("Exception should have been thrown.");
    } catch (TTransportException | NullPointerException e) {
      // TODO: NPE should not be thrown
    }
  }

  @Test
  public void testAppendPartitionLessPartValues() throws Exception {

    List<String> partitionValues = Lists.newArrayList("2019");
    Table table = tableWithPartitions;

    try {
      client.appendPartition(table.getDbName(), table.getTableName(), partitionValues);
      Assert.fail("Exception should have been thrown.");
    } catch (MetaException e) {
      // Expected exception
    }
    verifyPartitionNames(table, Lists.newArrayList("year=2017/month=march", "year=2017/month=april",
        "year=2018/month=march"));
    String partitionLocation = table.getSd().getLocation() + "/year=2019";
    Assert.assertFalse(metaStore.isPathExists(new Path(partitionLocation)));
  }

  @Test
  public void testAppendPartitionMorePartValues() throws Exception {

    List<String> partitionValues = Lists.newArrayList("2019", "march", "12");
    Table table = tableWithPartitions;

    try {
      client.appendPartition(table.getDbName(), table.getTableName(), partitionValues);
      Assert.fail("Exception should have been thrown.");
    } catch (MetaException e) {
      // Expected exception
    }
    verifyPartitionNames(table, Lists.newArrayList("year=2017/month=march", "year=2017/month=april",
        "year=2018/month=march"));
    String partitionLocation = tableWithPartitions.getSd().getLocation() + "/year=2019";
    Assert.assertFalse(metaStore.isPathExists(new Path(partitionLocation)));
  }

  // Tests for Partition appendPartition(String tableName, String dbName, String name) method

  @Test
  public void testAppendPart() throws Exception {

    Table table = tableWithPartitions;
    String partitionName = "year=2017/month=may";

    Partition appendedPart =
        client.appendPartition(table.getDbName(), table.getTableName(), partitionName);

    Assert.assertNotNull(appendedPart);
    Partition partition = client.getPartition(table.getDbName(), table.getTableName(),
        getPartitionValues(partitionName));
    Assert.assertEquals(partition, appendedPart);
    verifyPartition(partition, table, getPartitionValues(partitionName), partitionName);
    verifyPartitionNames(table, Lists.newArrayList("year=2017/month=march", "year=2017/month=april",
        "year=2018/month=march", partitionName));
  }

  @Test
  public void testAppendPartToExternalTable() throws Exception {

    Table table = externalTable;
    String partitionName = "year=2017/month=may";

    Partition appendedPart =
        client.appendPartition(table.getDbName(), table.getTableName(), partitionName);

    Assert.assertNotNull(appendedPart);
    Partition partition = client.getPartition(table.getDbName(), table.getTableName(),
        getPartitionValues(partitionName));
    Assert.assertEquals(partition, appendedPart);
    verifyPartition(partition, table, getPartitionValues(partitionName), partitionName);
    verifyPartitionNames(table, Lists.newArrayList(partitionName));
  }

  @Test
  public void testAppendPartMultiplePartitions() throws Exception {

    String partitionName1 = "year=2017/month=may";
    String partitionName2 = "year=2018/month=may";
    String partitionName3 = "year=2017/month=june";
    Table table = tableWithPartitions;

    client.appendPartition(table.getDbName(), table.getTableName(), partitionName1);
    client.appendPartition(table.getDbName(), table.getTableName(), partitionName2);
    client.appendPartition(table.getDbName(), table.getTableName(), partitionName3);

    verifyPartitionNames(table, Lists.newArrayList(partitionName1, partitionName2, partitionName3,
        "year=2017/month=march", "year=2017/month=april", "year=2018/month=march"));
  }

  @Test(expected = MetaException.class)
  public void testAppendPartToTableWithoutPartCols() throws Exception {

    String partitionName = "year=2017/month=may";
    Table table = tableNoPartColumns;
    client.appendPartition(table.getDbName(), table.getTableName(), partitionName);
  }

  @Test(expected = MetaException.class)
  public void testAppendPartToView() throws Exception {

    String partitionName = "year=2017/month=may";
    Table table = tableView;
    client.appendPartition(table.getDbName(), table.getTableName(), partitionName);
  }

  @Test(expected = AlreadyExistsException.class)
  public void testAppendPartAlreadyExists() throws Exception {

    String partitionName = "year=2017/month=april";
    Table table = tableWithPartitions;
    client.appendPartition(table.getDbName(), table.getTableName(), partitionName);
  }

  @Test(expected = InvalidObjectException.class)
  public void testAppendPartNonExistingDB() throws Exception {

    String partitionName = "year=2017/month=april";
    client.appendPartition("nonexistingdb", tableWithPartitions.getTableName(), partitionName);
  }

  @Test(expected = InvalidObjectException.class)
  public void testAppendPartNonExistingTable() throws Exception {

    String partitionName = "year=2017/month=april";
    client.appendPartition(tableWithPartitions.getDbName(), "nonexistingtable", partitionName);
  }

  @Test(expected = InvalidObjectException.class)
  public void testAppendPartEmptyDB() throws Exception {

    String partitionName = "year=2017/month=april";
    client.appendPartition("", tableWithPartitions.getTableName(), partitionName);
  }

  @Test(expected = InvalidObjectException.class)
  public void testAppendPartEmptyTable() throws Exception {

    String partitionName = "year=2017/month=april";
    client.appendPartition(tableWithPartitions.getDbName(), "", partitionName);
  }

  @Test(expected = MetaException.class)
  public void testAppendPartNullDB() throws Exception {

    String partitionName = "year=2017/month=april";
    client.appendPartition(null, tableWithPartitions.getTableName(), partitionName);
  }

  @Test(expected = MetaException.class)
  public void testAppendPartNullTable() throws Exception {

    String partitionName = "year=2017/month=april";
    client.appendPartition(tableWithPartitions.getDbName(), null, partitionName);
  }

  @Test(expected = MetaException.class)
  public void testAppendPartEmptyPartName() throws Exception {

    Table table = tableWithPartitions;
    client.appendPartition(table.getDbName(), table.getTableName(), "");
  }

  @Test(expected = MetaException.class)
  public void testAppendPartNullPartName() throws Exception {

    Table table = tableWithPartitions;
    client.appendPartition(table.getDbName(), table.getTableName(), (String) null);
  }

  @Test(expected = InvalidObjectException.class)
  public void testAppendPartLessPartValues() throws Exception {

    String partitionName = "year=2019";
    Table table = tableWithPartitions;
    client.appendPartition(table.getDbName(), table.getTableName(), partitionName);
  }

  @Test
  public void testAppendPartMorePartValues() throws Exception {

    String partitionName = "year=2019/month=march/day=12";
    Table table = tableWithPartitions;
    client.appendPartition(table.getDbName(), table.getTableName(), partitionName);
  }

  @Test(expected = InvalidObjectException.class)
  public void testAppendPartInvalidPartName() throws Exception {

    String partitionName = "invalidpartname";
    Table table = tableWithPartitions;
    client.appendPartition(table.getDbName(), table.getTableName(), partitionName);
  }

  @Test(expected = InvalidObjectException.class)
  public void testAppendPartWrongColumnInPartName() throws Exception {

    String partitionName = "year=2019/honap=march";
    Table table = tableWithPartitions;
    client.appendPartition(table.getDbName(), table.getTableName(), partitionName);
  }

  // Helper methods

  private Table createTableWithPartitions() throws Exception {
    Table table = createTable("test_append_part_table_with_parts", getYearAndMonthPartCols(), null,
        TableType.MANAGED_TABLE.name(),
        metaStore.getWarehouseRoot() + "/test_append_part_table_with_parts");
    createPartition(table, Lists.newArrayList("2017", "march"));
    createPartition(table, Lists.newArrayList("2017", "april"));
    createPartition(table, Lists.newArrayList("2018", "march"));
    return table;
  }

  private Table createTableNoPartitionColumns() throws Exception {
    Table table = createTable("test_append_part_table_no_part_columns", null, null, "MANAGED_TABLE",
        metaStore.getWarehouseRoot() + "/test_append_part_table_no_part_columns");
    return table;
  }

  private Table createExternalTable() throws Exception {
    Map<String, String> tableParams = new HashMap<>();
    tableParams.put("EXTERNAL", "TRUE");
    Table table = createTable("test_append_part_external_table", getYearAndMonthPartCols(),
        tableParams, TableType.EXTERNAL_TABLE.name(),
        metaStore.getWarehouseRoot() + "/test_append_part_external_table");
    return table;
  }

  private Table createView() throws Exception {
    Table table = createTable("test_append_part_table_view", getYearAndMonthPartCols(), null,
        TableType.VIRTUAL_VIEW.name(), null);
    return table;
  }

  private Table createTable(String tableName, List<FieldSchema> partCols, Map<String,
      String> tableParams, String tableType, String location) throws Exception {
    Table table = new TableBuilder()
        .setDbName(DB_NAME)
        .setTableName(tableName)
        .addCol("test_id", "int", "test col id")
        .addCol("test_value", "string", "test col value")
        .setPartCols(partCols)
        .setTableParams(tableParams)
        .setType(tableType)
        .setLocation(location)
        .build();
    client.createTable(table);
    return client.getTable(DB_NAME, tableName);
  }

  private void createPartition(Table table, List<String> values) throws Exception {
    Partition partition = new PartitionBuilder()
        .fromTable(table)
        .setValues(values)
        .build();
    client.add_partition(partition);
  }

  private static List<FieldSchema> getYearAndMonthPartCols() {
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("year", "string", "year part col"));
    cols.add(new FieldSchema("month", "string", "month part col"));
    return cols;
  }

  private static List<String> getPartitionValues(String partitionsName) {
    List<String> values = new ArrayList<>();
    if (StringUtils.isEmpty(partitionsName)) {
      return values;
    }
    values = Arrays.stream(partitionsName.split("/")).map(v -> v.split("=")[1])
        .collect(Collectors.toList());
    return values;
  }

  private void verifyPartition(Partition partition, Table table, List<String> expectedPartValues,
      String partitionName) throws Exception {
    Assert.assertEquals(table.getTableName(), partition.getTableName());
    Assert.assertEquals(table.getDbName(), partition.getDbName());
    Assert.assertEquals(expectedPartValues, partition.getValues());
    Assert.assertNotEquals(0, partition.getCreateTime());
    Assert.assertEquals(0, partition.getLastAccessTime());
    Assert.assertEquals(1, partition.getParameters().size());
    Assert.assertTrue(partition.getParameters().containsKey("transient_lastDdlTime"));
    StorageDescriptor partitionSD = partition.getSd();
    Assert.assertEquals(table.getSd().getLocation() + "/" + partitionName,
        partitionSD.getLocation());
    partition.getSd().setLocation(table.getSd().getLocation());
    Assert.assertEquals(table.getSd(), partitionSD);
    Assert.assertTrue(metaStore.isPathExists(new Path(partitionSD.getLocation())));
  }

  private void verifyPartitionNames(Table table, List<String> expectedPartNames) throws Exception {
    List<String> partitionNames =
        client.listPartitionNames(table.getDbName(), table.getTableName(), (short) -1);
    Assert.assertEquals(expectedPartNames.size(), partitionNames.size());
    Assert.assertTrue(partitionNames.containsAll(expectedPartNames));
  }
}
