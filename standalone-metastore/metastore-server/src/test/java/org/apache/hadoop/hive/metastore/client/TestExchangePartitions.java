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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.collect.Lists;

/**
 * Tests for exchanging partitions.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestExchangePartitions extends MetaStoreClientTest {
  private AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;

  protected static final String DB_NAME = "test_partition_db";
  private static final String STRING_COL_TYPE = "string";
  protected static final String INT_COL_TYPE = "int";
  protected static final String YEAR_COL_NAME = "year";
  protected static final String MONTH_COL_NAME = "month";
  protected static final String DAY_COL_NAME = "day";
  protected static final short MAX = -1;
  private Table sourceTable;
  private Table destTable;
  private Partition[] partitions;

  public TestExchangePartitions(String name, AbstractMetaStoreService metaStore) {
    this.metaStore = metaStore;
  }

  @Before
  public void setUp() throws Exception {
    // Get new client
    client = metaStore.getClient();

    // Clean up the database
    client.dropDatabase(DB_NAME, true, true, true);
    metaStore.cleanWarehouseDirs();
    createTestTables();
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (client != null) {
        try {
          client.close();
        } catch (Exception e) {
          // HIVE-19729: Shallow the exceptions based on the discussion in the Jira
        }
      }
    } finally {
      client = null;
    }
  }

  protected IMetaStoreClient getClient() {
    return client;
  }

  protected void setClient(IMetaStoreClient client) {
    this.client = client;
  }

  protected AbstractMetaStoreService getMetaStore() {
    return metaStore;
  }

  protected  Table getSourceTable() {
    return sourceTable;
  }

  protected Table getDestTable() {
    return destTable;
  }

  protected Partition[] getPartitions() {
    return partitions;
  }

  // Tests for the List<Partition> exchange_partitions(Map<String, String> partitionSpecs, String
  // sourceDb, String sourceTable, String destdb, String destTableName) method

  @Test
  public void testExchangePartitions() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    List<Partition> exchangedPartitions =
        client.exchange_partitions(partitionSpecs, sourceTable.getDbName(),
            sourceTable.getTableName(), destTable.getDbName(), destTable.getTableName());

    Assert.assertEquals(1, exchangedPartitions.size());
    String partitionName =
        Warehouse.makePartName(sourceTable.getPartitionKeys(), partitions[1].getValues());
    String exchangedPartitionName = Warehouse.makePartName(sourceTable.getPartitionKeys(),
        exchangedPartitions.get(0).getValues());
    Assert.assertEquals(partitionName, exchangedPartitionName);

    checkExchangedPartitions(sourceTable, destTable, Lists.newArrayList(partitions[1]));
    checkRemainingPartitions(sourceTable, destTable,
        Lists.newArrayList(partitions[0], partitions[2], partitions[3], partitions[4]));
  }

  @Test
  public void testExchangePartitionsDestTableHasPartitions() throws Exception {

    // Create dest table partitions with custom locations
    createPartition(destTable, Lists.newArrayList("2019", "march", "15"),
        destTable.getSd().getLocation() + "/destPart1");
    createPartition(destTable, Lists.newArrayList("2019", "march", "22"),
        destTable.getSd().getLocation() + "/destPart2");
    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);

    client.exchange_partitions(partitionSpecs, DB_NAME, sourceTable.getTableName(), DB_NAME,
        destTable.getTableName());

    checkExchangedPartitions(sourceTable, destTable, Lists.newArrayList(partitions[1]));
    checkRemainingPartitions(sourceTable, destTable,
        Lists.newArrayList(partitions[0], partitions[2], partitions[3], partitions[4]));
    // Check the original partitions of the dest table
    List<String> partitionNames =
        client.listPartitionNames(destTable.getDbName(), destTable.getTableName(), MAX);
    Assert.assertEquals(3, partitionNames.size());
    Assert.assertTrue(partitionNames.containsAll(
        Lists.newArrayList("year=2019/month=march/day=15", "year=2019/month=march/day=22")));
    Assert.assertTrue(
        metaStore.isPathExists(new Path(destTable.getSd().getLocation() + "/destPart1")));
    Assert.assertTrue(
        metaStore.isPathExists(new Path(destTable.getSd().getLocation() + "/destPart2")));
  }

  @Test
  public void testExchangePartitionsYearSet() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(Lists.newArrayList("2017", "", ""));
    List<Partition> exchangedPartitions =
        client.exchange_partitions(partitionSpecs, sourceTable.getDbName(),
            sourceTable.getTableName(), destTable.getDbName(), destTable.getTableName());

    Assert.assertEquals(4, exchangedPartitions.size());
    List<String> exchangedPartNames = new ArrayList<>();
    for (Partition exchangedPartition : exchangedPartitions) {
      String partName =
          Warehouse.makePartName(sourceTable.getPartitionKeys(), exchangedPartition.getValues());
      exchangedPartNames.add(partName);
    }
    Assert.assertTrue(exchangedPartNames.contains("year=2017/month=march/day=15"));
    Assert.assertTrue(exchangedPartNames.contains("year=2017/month=march/day=22"));
    Assert.assertTrue(exchangedPartNames.contains("year=2017/month=april/day=23"));
    Assert.assertTrue(exchangedPartNames.contains("year=2017/month=may/day=23"));
    checkExchangedPartitions(sourceTable, destTable,
        Lists.newArrayList(partitions[0], partitions[1], partitions[2], partitions[3]));
    checkRemainingPartitions(sourceTable, destTable, Lists.newArrayList(partitions[4]));
  }

  @Test
  public void testExchangePartitionsYearAndMonthSet() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(Lists.newArrayList("2017", "march", ""));
    client.exchange_partitions(partitionSpecs, sourceTable.getDbName(),
        sourceTable.getTableName(), destTable.getDbName(), destTable.getTableName());

    checkExchangedPartitions(sourceTable, destTable,
        Lists.newArrayList(partitions[0], partitions[1]));
    checkRemainingPartitions(sourceTable, destTable,
        Lists.newArrayList(partitions[2], partitions[3], partitions[4]));
  }

  @Test
  public void testExchangePartitionsBetweenDBs() throws Exception {

    String dbName = "newDatabase";
    createDB(dbName);
    Table dest = createTable(dbName, "test_dest_table_diff_db", getYearMonthAndDayPartCols(), null);

    Map<String, String> partitionSpecs = getPartitionSpec(Lists.newArrayList("2017", "march", ""));
    client.exchange_partitions(partitionSpecs, sourceTable.getDbName(), sourceTable.getTableName(),
        dest.getDbName(), dest.getTableName());

    checkExchangedPartitions(sourceTable, dest, Lists.newArrayList(partitions[0], partitions[1]));
    checkRemainingPartitions(sourceTable, dest,
        Lists.newArrayList(partitions[2], partitions[3], partitions[4]));
    client.dropDatabase(dbName, true, true, true);
  }

  @Test
  public void testExchangePartitionsCustomTableLocations() throws Exception {

    Table source = createTable(DB_NAME, "test_source_table_cust_loc", getYearMonthAndDayPartCols(),
        metaStore.getExternalWarehouseRoot() + "/sourceTable");
    Table dest = createTable(DB_NAME, "test_dest_table_cust_loc", getYearMonthAndDayPartCols(),
        metaStore.getExternalWarehouseRoot() + "/destTable");
    Partition[] parts = new Partition[2];
    parts[0] = createPartition(source, Lists.newArrayList("2019", "may", "15"), null);
    parts[1] = createPartition(source, Lists.newArrayList("2019", "june", "14"), null);

    Map<String, String> partitionSpecs = getPartitionSpec(parts[1]);

    client.exchange_partitions(partitionSpecs, source.getDbName(), source.getTableName(),
        dest.getDbName(), dest.getTableName());

    checkExchangedPartitions(source, dest, Lists.newArrayList(parts[1]));
    checkRemainingPartitions(source, dest, Lists.newArrayList(parts[0]));
  }

  @Test
  public void testExchangePartitionsCustomTableAndPartLocation() throws Exception {

    Table source = createTable(DB_NAME, "test_source_table_cust_loc",
        getYearMonthAndDayPartCols(), metaStore.getExternalWarehouseRoot() + "/sourceTable");
    Table dest = createTable(DB_NAME, "test_dest_table_cust_loc", getYearMonthAndDayPartCols(),
        metaStore.getExternalWarehouseRoot() + "/destTable");
    Partition[] parts = new Partition[2];
    parts[0] = createPartition(source, Lists.newArrayList("2019", "may", "11"),
        source.getSd().getLocation() + "/2019m11");
    parts[1] = createPartition(source, Lists.newArrayList("2019", "july", "23"),
        source.getSd().getLocation() + "/2019j23");

    Map<String, String> partitionSpecs = getPartitionSpec(parts[1]);
    try {
      client.exchange_partitions(partitionSpecs, source.getDbName(),
          source.getTableName(), dest.getDbName(), dest.getTableName());
      Assert.fail("MetaException should have been thrown.");
    } catch (MetaException e) {
      // Expected exception as FileNotFoundException will occur if the partitions have custom
      // location
    }

    checkRemainingPartitions(source, dest,
        Lists.newArrayList(parts[0], parts[1]));
    List<Partition> destTablePartitions =
        client.listPartitions(dest.getDbName(), dest.getTableName(), (short) -1);
    Assert.assertTrue(destTablePartitions.isEmpty());
  }

  @Test
  public void testExchangePartitionsCustomPartLocation() throws Exception {

    Table source = createTable(DB_NAME, "test_source_table", getYearMonthAndDayPartCols(), null);
    Table dest = createTable(DB_NAME, "test_dest_table", getYearMonthAndDayPartCols(), null);
    Partition[] parts = new Partition[2];
    parts[0] = createPartition(source, Lists.newArrayList("2019", "march", "15"),
        source.getSd().getLocation() + "/2019m15");
    parts[1] = createPartition(source, Lists.newArrayList("2019", "march", "22"),
        source.getSd().getLocation() + "/2019m22");

    Map<String, String> partitionSpecs = getPartitionSpec(parts[1]);
    try {
      client.exchange_partitions(partitionSpecs, source.getDbName(),
          source.getTableName(), dest.getDbName(), dest.getTableName());
      Assert.fail("MetaException should have been thrown.");
    } catch (MetaException e) {
      // Expected exception as FileNotFoundException will occur if the partitions have custom
      // location
    }

    checkRemainingPartitions(source, dest,
        Lists.newArrayList(parts[0], parts[1]));
    List<Partition> destTablePartitions =
        client.listPartitions(dest.getDbName(), dest.getTableName(), (short) -1);
    Assert.assertTrue(destTablePartitions.isEmpty());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionsNonExistingPartLocation() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    metaStore.cleanWarehouseDirs();
    client.exchange_partitions(partitionSpecs, sourceTable.getDbName(),
        sourceTable.getTableName(), destTable.getDbName(), destTable.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionsNonExistingSourceTable() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partitions(partitionSpecs, DB_NAME, "nonexistingtable", destTable.getDbName(),
        destTable.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionsNonExistingSourceDB() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partitions(partitionSpecs, "nonexistingdb", sourceTable.getTableName(),
        destTable.getDbName(), destTable.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionsNonExistingDestTable() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partitions(partitionSpecs, sourceTable.getDbName(), sourceTable.getTableName(),
        DB_NAME, "nonexistingtable");
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionsNonExistingDestDB() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partitions(partitionSpecs, sourceTable.getDbName(), sourceTable.getTableName(),
        "nonexistingdb", destTable.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionsEmptySourceTable() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partitions(partitionSpecs, DB_NAME, "", destTable.getDbName(),
        destTable.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionsEmptySourceDB() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partitions(partitionSpecs, "", sourceTable.getTableName(),
        destTable.getDbName(), destTable.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionsEmptyDestTable() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partitions(partitionSpecs, sourceTable.getDbName(), sourceTable.getTableName(),
        DB_NAME, "");
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionsEmptyDestDB() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partitions(partitionSpecs, sourceTable.getDbName(), sourceTable.getTableName(),
        "", destTable.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionsNullSourceTable() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partitions(partitionSpecs, DB_NAME, null, destTable.getDbName(),
        destTable.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionsNullSourceDB() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partitions(partitionSpecs, null, sourceTable.getTableName(),
        destTable.getDbName(), destTable.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionsNullDestTable() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partitions(partitionSpecs, sourceTable.getDbName(), sourceTable.getTableName(),
        DB_NAME, null);
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionsNullDestDB() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partitions(partitionSpecs, sourceTable.getDbName(), sourceTable.getTableName(),
        null, destTable.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionsEmptyPartSpec() throws Exception {

    Map<String, String> partitionSpecs = new HashMap<>();
    client.exchange_partitions(partitionSpecs, sourceTable.getDbName(),
        sourceTable.getTableName(), destTable.getDbName(), destTable.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionsNullPartSpec() throws Exception {
    client.exchange_partitions(null, sourceTable.getDbName(), sourceTable.getTableName(), null,
        destTable.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionsPartAlreadyExists() throws Exception {

    Partition partition =
        buildPartition(destTable, Lists.newArrayList("2017", "march", "22"), null);
    client.add_partition(partition);

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partitions(partitionSpecs, DB_NAME, sourceTable.getTableName(), DB_NAME,
        destTable.getTableName());
  }

  @Test
  public void testExchangePartitionsOneFail() throws Exception {

    Partition partition =
        buildPartition(destTable, Lists.newArrayList("2017", "march", "22"), null);
    client.add_partition(partition);

    Map<String, String> partitionSpecs = getPartitionSpec(Lists.newArrayList("2017", "", ""));
    try {
      client.exchange_partitions(partitionSpecs, DB_NAME, sourceTable.getTableName(), DB_NAME,
          destTable.getTableName());
      Assert.fail(
          "Exception should have been thrown as one of the partitions already exists in the dest table.");
    } catch (MetaException e) {
      // Expected exception
    }

    checkRemainingPartitions(sourceTable, destTable,
        Lists.newArrayList(partitions[0], partitions[2], partitions[3], partitions[4]));
    List<Partition> partitionsInDestTable =
        client.listPartitions(destTable.getDbName(), destTable.getTableName(), MAX);
    Assert.assertEquals(1, partitionsInDestTable.size());
    Assert.assertEquals(partitions[1].getValues(), partitionsInDestTable.get(0).getValues());
    Assert.assertTrue(
        metaStore.isPathExists(new Path(partitionsInDestTable.get(0).getSd().getLocation())));
    Partition resultPart = client.getPartition(sourceTable.getDbName(),
        sourceTable.getTableName(), partitions[1].getValues());
    Assert.assertNotNull(resultPart);
    Assert.assertTrue(metaStore.isPathExists(new Path(partitions[1].getSd().getLocation())));
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionsDifferentColsInTables() throws Exception {

    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("test_id", INT_COL_TYPE, "test col id"));
    cols.add(new FieldSchema("test_value", STRING_COL_TYPE, "test col value"));
    cols.add(new FieldSchema("test_name", STRING_COL_TYPE, "test col name"));
    Table dest = createTable(DB_NAME, "test_dest_table", getYearMonthAndDayPartCols(), cols, null);

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partitions(partitionSpecs, sourceTable.getDbName(), sourceTable.getTableName(),
        dest.getDbName(), dest.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionsDifferentColNameInTables() throws Exception {

    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("id", INT_COL_TYPE, "test col id"));
    cols.add(new FieldSchema("test_value", STRING_COL_TYPE, "test col value"));
    Table dest = createTable(DB_NAME, "test_dest_table", getYearMonthAndDayPartCols(), cols, null);

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partitions(partitionSpecs, sourceTable.getDbName(), sourceTable.getTableName(),
        dest.getDbName(), dest.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionsDifferentColTypesInTables() throws Exception {

    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("test_id", STRING_COL_TYPE, "test col id"));
    cols.add(new FieldSchema("test_value", STRING_COL_TYPE, "test col value"));
    Table dest = createTable(DB_NAME, "test_dest_table", getYearMonthAndDayPartCols(), cols, null);

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partitions(partitionSpecs, sourceTable.getDbName(), sourceTable.getTableName(),
        dest.getDbName(), dest.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionsDifferentPartColsInTables() throws Exception {

    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema(YEAR_COL_NAME, STRING_COL_TYPE, "year part col"));
    cols.add(new FieldSchema(MONTH_COL_NAME, STRING_COL_TYPE, "month part col"));
    Table dest = createTable(DB_NAME, "test_dest_table", cols, null);

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partitions(partitionSpecs, sourceTable.getDbName(),
        sourceTable.getTableName(), dest.getDbName(), dest.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionsDifferentPartColNameInTables() throws Exception {

    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema(YEAR_COL_NAME, STRING_COL_TYPE, "year part col"));
    cols.add(new FieldSchema(MONTH_COL_NAME, STRING_COL_TYPE, "month part col"));
    cols.add(new FieldSchema("nap", STRING_COL_TYPE, "day part col"));
    Table dest = createTable(DB_NAME, "test_dest_table", cols, null);

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partitions(partitionSpecs, sourceTable.getDbName(),
        sourceTable.getTableName(), dest.getDbName(), dest.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionsDifferentPartColTypesInTables() throws Exception {

    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema(YEAR_COL_NAME, STRING_COL_TYPE, "year part col"));
    cols.add(new FieldSchema(MONTH_COL_NAME, INT_COL_TYPE, "month part col"));
    cols.add(new FieldSchema(DAY_COL_NAME, STRING_COL_TYPE, "day part col"));
    Table dest = createTable(DB_NAME, "test_dest_table", cols, null);

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partitions(partitionSpecs, sourceTable.getDbName(),
        sourceTable.getTableName(), dest.getDbName(), dest.getTableName());
  }

  @Test
  public void testExchangePartitionsLessValueInPartSpec() throws Exception {

    Map<String, String> partitionSpecs = new HashMap<>();
    partitionSpecs.put(YEAR_COL_NAME, "2017");
    partitionSpecs.put(MONTH_COL_NAME, "march");
    client.exchange_partitions(partitionSpecs, sourceTable.getDbName(),
        sourceTable.getTableName(), destTable.getDbName(), destTable.getTableName());
    checkExchangedPartitions(sourceTable, destTable,
        Lists.newArrayList(partitions[0], partitions[1]));
    checkRemainingPartitions(sourceTable, destTable,
        Lists.newArrayList(partitions[2], partitions[3], partitions[4]));
  }

  @Test
  public void testExchangePartitionsMoreValueInPartSpec() throws Exception {

    Map<String, String> partitionSpecs = new HashMap<>();
    partitionSpecs.put(YEAR_COL_NAME, "2017");
    partitionSpecs.put(MONTH_COL_NAME, "march");
    partitionSpecs.put(DAY_COL_NAME, "22");
    partitionSpecs.put("hour", "18");
    client.exchange_partitions(partitionSpecs, sourceTable.getDbName(),
        sourceTable.getTableName(), destTable.getDbName(), destTable.getTableName());
    checkExchangedPartitions(sourceTable, destTable, Lists.newArrayList(partitions[1]));
    checkRemainingPartitions(sourceTable, destTable,
        Lists.newArrayList(partitions[0], partitions[2], partitions[3], partitions[4]));
  }

  @Test
  public void testExchangePartitionsDifferentValuesInPartSpec() throws Exception {

    Map<String, String> partitionSpecs = new HashMap<>();
    partitionSpecs.put(YEAR_COL_NAME, "2017");
    partitionSpecs.put("honap", "march");
    partitionSpecs.put("nap", "22");
    client.exchange_partitions(partitionSpecs, sourceTable.getDbName(),
        sourceTable.getTableName(), destTable.getDbName(), destTable.getTableName());
    checkExchangedPartitions(sourceTable, destTable,
        Lists.newArrayList(partitions[0], partitions[1], partitions[2], partitions[3]));
    checkRemainingPartitions(sourceTable, destTable, Lists.newArrayList(partitions[4]));
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionsNonExistingValuesInPartSpec() throws Exception {

    Map<String, String> partitionSpecs = new HashMap<>();
    partitionSpecs.put("ev", "2017");
    partitionSpecs.put("honap", "march");
    partitionSpecs.put("nap", "22");
    client.exchange_partitions(partitionSpecs, sourceTable.getDbName(),
        sourceTable.getTableName(), destTable.getDbName(), destTable.getTableName());
  }

  @Test
  public void testExchangePartitionsOnlyMonthSetInPartSpec() throws Exception {

    Map<String, String> partitionSpecs = new HashMap<>();
    partitionSpecs.put(YEAR_COL_NAME, "");
    partitionSpecs.put(MONTH_COL_NAME, "march");
    partitionSpecs.put(DAY_COL_NAME, "");
    try {
      client.exchange_partitions(partitionSpecs, sourceTable.getDbName(),
          sourceTable.getTableName(), destTable.getDbName(), destTable.getTableName());
      Assert.fail("MetaException should have been thrown.");
    } catch (MetaException e) {
      // Expected exception
    }
    checkRemainingPartitions(sourceTable, destTable, Lists.newArrayList(partitions[0],
        partitions[1], partitions[2], partitions[3], partitions[4]));
    List<Partition> partsInDestTable =
        client.listPartitions(destTable.getDbName(), destTable.getTableName(), MAX);
    Assert.assertTrue(partsInDestTable.isEmpty());
  }

  @Test
  public void testExchangePartitionsYearAndDaySetInPartSpec() throws Exception {

    Map<String, String> partitionSpecs = new HashMap<>();
    partitionSpecs.put(YEAR_COL_NAME, "2017");
    partitionSpecs.put(MONTH_COL_NAME, "");
    partitionSpecs.put(DAY_COL_NAME, "22");
    try {
      client.exchange_partitions(partitionSpecs, sourceTable.getDbName(),
          sourceTable.getTableName(), destTable.getDbName(), destTable.getTableName());
      Assert.fail("MetaException should have been thrown.");
    } catch (MetaException e) {
      // Expected exception
    }
    checkRemainingPartitions(sourceTable, destTable, Lists.newArrayList(partitions[0],
        partitions[1], partitions[2], partitions[3], partitions[4]));
    List<Partition> partsInDestTable =
        client.listPartitions(destTable.getDbName(), destTable.getTableName(), MAX);
    Assert.assertTrue(partsInDestTable.isEmpty());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionsNoPartExists() throws Exception {

    Map<String, String> partitionSpecs =
        getPartitionSpec(Lists.newArrayList("2017", "march", "25"));
    client.exchange_partitions(partitionSpecs, sourceTable.getDbName(),
        sourceTable.getTableName(), destTable.getDbName(), destTable.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionsNoPartExistsYearAndMonthSet() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(Lists.newArrayList("2017", "august", ""));
    client.exchange_partitions(partitionSpecs, sourceTable.getDbName(),
        sourceTable.getTableName(), destTable.getDbName(), destTable.getTableName());
  }

  // Tests for the Partition exchange_partition(Map<String, String> partitionSpecs, String
  // sourceDb, String sourceTable, String destdb, String destTableName) method

  @Test
  public void testExchangePartition() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    Partition exchangedPartition =
        client.exchange_partition(partitionSpecs, sourceTable.getDbName(),
            sourceTable.getTableName(), destTable.getDbName(), destTable.getTableName());

    Assert.assertEquals(new Partition(), exchangedPartition);
    checkExchangedPartitions(sourceTable, destTable, Lists.newArrayList(partitions[1]));
    checkRemainingPartitions(sourceTable, destTable,
        Lists.newArrayList(partitions[0], partitions[2], partitions[3], partitions[4]));
  }

  @Test
  public void testExchangePartitionDestTableHasPartitions() throws Exception {

    // Create dest table partitions with custom locations
    createPartition(destTable, Lists.newArrayList("2019", "march", "15"),
        destTable.getSd().getLocation() + "/destPart1");
    createPartition(destTable, Lists.newArrayList("2019", "march", "22"),
        destTable.getSd().getLocation() + "/destPart2");
    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);

    client.exchange_partition(partitionSpecs, DB_NAME, sourceTable.getTableName(), DB_NAME,
        destTable.getTableName());

    checkExchangedPartitions(sourceTable, destTable, Lists.newArrayList(partitions[1]));
    checkRemainingPartitions(sourceTable, destTable,
        Lists.newArrayList(partitions[0], partitions[2], partitions[3], partitions[4]));
    // Check the original partitions of the dest table
    List<String> partitionNames =
        client.listPartitionNames(destTable.getDbName(), destTable.getTableName(), MAX);
    Assert.assertEquals(3, partitionNames.size());
    Assert.assertTrue(partitionNames.containsAll(
        Lists.newArrayList("year=2019/month=march/day=15", "year=2019/month=march/day=22")));
    Assert.assertTrue(
        metaStore.isPathExists(new Path(destTable.getSd().getLocation() + "/destPart1")));
    Assert.assertTrue(
        metaStore.isPathExists(new Path(destTable.getSd().getLocation() + "/destPart2")));
  }

  @Test
  public void testExchangePartitionYearSet() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(Lists.newArrayList("2017", "", ""));
    Partition exchangedPartition =
        client.exchange_partition(partitionSpecs, sourceTable.getDbName(),
            sourceTable.getTableName(), destTable.getDbName(), destTable.getTableName());

    Assert.assertEquals(new Partition(), exchangedPartition);
    checkExchangedPartitions(sourceTable, destTable,
        Lists.newArrayList(partitions[0], partitions[1], partitions[2], partitions[3]));
    checkRemainingPartitions(sourceTable, destTable, Lists.newArrayList(partitions[4]));
  }

  @Test
  public void testExchangePartitionYearAndMonthSet() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(Lists.newArrayList("2017", "march", ""));
    client.exchange_partition(partitionSpecs, sourceTable.getDbName(),
        sourceTable.getTableName(), destTable.getDbName(), destTable.getTableName());

    checkExchangedPartitions(sourceTable, destTable,
        Lists.newArrayList(partitions[0], partitions[1]));
    checkRemainingPartitions(sourceTable, destTable,
        Lists.newArrayList(partitions[2], partitions[3], partitions[4]));
  }

  @Test
  public void testExchangePartitionBetweenDBs() throws Exception {

    String dbName = "newDatabase";
    createDB(dbName);
    Table dest = createTable(dbName, "test_dest_table_diff_db", getYearMonthAndDayPartCols(), null);

    Map<String, String> partitionSpecs = getPartitionSpec(Lists.newArrayList("2017", "march", ""));
    client.exchange_partition(partitionSpecs, sourceTable.getDbName(), sourceTable.getTableName(),
        dest.getDbName(), dest.getTableName());

    checkExchangedPartitions(sourceTable, dest, Lists.newArrayList(partitions[0], partitions[1]));
    checkRemainingPartitions(sourceTable, dest,
        Lists.newArrayList(partitions[2], partitions[3], partitions[4]));
    client.dropDatabase(dbName, true, true, true);
  }

  @Test
  public void testExchangePartitionCustomTableLocations() throws Exception {

    Table source = createTable(DB_NAME, "test_source_table_cust_loc",
        getYearMonthAndDayPartCols(), metaStore.getExternalWarehouseRoot() + "/sourceTable");
    Table dest = createTable(DB_NAME, "test_dest_table_cust_loc", getYearMonthAndDayPartCols(),
        metaStore.getExternalWarehouseRoot() + "/destTable");
    Partition[] parts = new Partition[2];
    parts[0] = createPartition(source, Lists.newArrayList("2019", "may", "15"), null);
    parts[1] = createPartition(source, Lists.newArrayList("2019", "june", "14"), null);

    Map<String, String> partitionSpecs = getPartitionSpec(parts[1]);

    client.exchange_partition(partitionSpecs, source.getDbName(), source.getTableName(),
        dest.getDbName(), dest.getTableName());

    checkExchangedPartitions(source, dest, Lists.newArrayList(parts[1]));
    checkRemainingPartitions(source, dest, Lists.newArrayList(parts[0]));
  }

  @Test
  public void testExchangePartitionCustomTableAndPartLocation() throws Exception {

    Table source = createTable(DB_NAME, "test_source_table_cust_loc",
        getYearMonthAndDayPartCols(), metaStore.getExternalWarehouseRoot() + "/sourceTable");
    Table dest = createTable(DB_NAME, "test_dest_table_cust_loc", getYearMonthAndDayPartCols(),
        metaStore.getExternalWarehouseRoot() + "/destTable");
    Partition[] parts = new Partition[2];
    parts[0] = createPartition(source, Lists.newArrayList("2019", "may", "11"),
        source.getSd().getLocation() + "/2019m11");
    parts[1] = createPartition(source, Lists.newArrayList("2019", "july", "23"),
        source.getSd().getLocation() + "/2019j23");

    Map<String, String> partitionSpecs = getPartitionSpec(parts[1]);
    try {
      client.exchange_partition(partitionSpecs, source.getDbName(),
          source.getTableName(), dest.getDbName(), dest.getTableName());
      Assert.fail("MetaException should have been thrown.");
    } catch (MetaException e) {
      // Expected exception as FileNotFoundException will occur if the partitions have custom
      // location
    }

    checkRemainingPartitions(source, dest,
        Lists.newArrayList(parts[0], parts[1]));
    List<Partition> destTablePartitions =
        client.listPartitions(dest.getDbName(), dest.getTableName(), (short) -1);
    Assert.assertTrue(destTablePartitions.isEmpty());
  }

  @Test
  public void testExchangePartitionCustomPartLocation() throws Exception {

    Table source = createTable(DB_NAME, "test_source_table", getYearMonthAndDayPartCols(), null);
    Table dest = createTable(DB_NAME, "test_dest_table", getYearMonthAndDayPartCols(), null);
    Partition[] parts = new Partition[2];
    parts[0] = createPartition(source, Lists.newArrayList("2019", "march", "15"),
        source.getSd().getLocation() + "/2019m15");
    parts[1] = createPartition(source, Lists.newArrayList("2019", "march", "22"),
        source.getSd().getLocation() + "/2019m22");

    Map<String, String> partitionSpecs = getPartitionSpec(parts[1]);
    try {
      client.exchange_partition(partitionSpecs, source.getDbName(),
          source.getTableName(), dest.getDbName(), dest.getTableName());
      Assert.fail("MetaException should have been thrown.");
    } catch (MetaException e) {
      // Expected exception as FileNotFoundException will occur if the partitions have custom
      // location
    }

    checkRemainingPartitions(source, dest,
        Lists.newArrayList(parts[0], parts[1]));
    List<Partition> destTablePartitions =
        client.listPartitions(dest.getDbName(), dest.getTableName(), (short) -1);
    Assert.assertTrue(destTablePartitions.isEmpty());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionNonExistingPartLocation() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    metaStore.cleanWarehouseDirs();
    client.exchange_partition(partitionSpecs, sourceTable.getDbName(),
        sourceTable.getTableName(), destTable.getDbName(), destTable.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionNonExistingSourceTable() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partition(partitionSpecs, DB_NAME, "nonexistingtable", destTable.getDbName(),
        destTable.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionNonExistingSourceDB() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partition(partitionSpecs, "nonexistingdb", sourceTable.getTableName(),
        destTable.getDbName(), destTable.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionNonExistingDestTable() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partition(partitionSpecs, sourceTable.getDbName(), sourceTable.getTableName(),
        DB_NAME, "nonexistingtable");
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionNonExistingDestDB() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partition(partitionSpecs, sourceTable.getDbName(), sourceTable.getTableName(),
        "nonexistingdb", destTable.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionEmptySourceTable() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partition(partitionSpecs, DB_NAME, "", destTable.getDbName(),
        destTable.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionEmptySourceDB() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partition(partitionSpecs, "", sourceTable.getTableName(), destTable.getDbName(),
        destTable.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionEmptyDestTable() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partition(partitionSpecs, sourceTable.getDbName(), sourceTable.getTableName(),
        DB_NAME, "");
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionEmptyDestDB() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partition(partitionSpecs, sourceTable.getDbName(), sourceTable.getTableName(),
        "", destTable.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionNullSourceTable() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partition(partitionSpecs, DB_NAME, null, destTable.getDbName(),
        destTable.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionNullSourceDB() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partition(partitionSpecs, null, sourceTable.getTableName(),
        destTable.getDbName(), destTable.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionNullDestTable() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partition(partitionSpecs, sourceTable.getDbName(), sourceTable.getTableName(),
        DB_NAME, null);
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionNullDestDB() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partition(partitionSpecs, sourceTable.getDbName(), sourceTable.getTableName(),
        null, destTable.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionEmptyPartSpec() throws Exception {

    Map<String, String> partitionSpecs = new HashMap<>();
    client.exchange_partition(partitionSpecs, sourceTable.getDbName(),
        sourceTable.getTableName(), destTable.getDbName(), destTable.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionNullPartSpec() throws Exception {

    client.exchange_partition(null, sourceTable.getDbName(), sourceTable.getTableName(), null,
        destTable.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionPartAlreadyExists() throws Exception {

    Partition partition =
        buildPartition(destTable, Lists.newArrayList("2017", "march", "22"), null);
    client.add_partition(partition);

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partition(partitionSpecs, DB_NAME, sourceTable.getTableName(), DB_NAME,
        destTable.getTableName());
  }

  @Test
  public void testExchangePartitionOneFail() throws Exception {

    Partition partition =
        buildPartition(destTable, Lists.newArrayList("2017", "march", "22"), null);
    client.add_partition(partition);

    Map<String, String> partitionSpecs = getPartitionSpec(Lists.newArrayList("2017", "", ""));
    try {
      client.exchange_partition(partitionSpecs, DB_NAME, sourceTable.getTableName(), DB_NAME,
          destTable.getTableName());
      Assert.fail(
          "Exception should have been thrown as one of the partitions already exists in the dest table.");
    } catch (MetaException e) {
      // Expected exception
    }

    checkRemainingPartitions(sourceTable, destTable,
        Lists.newArrayList(partitions[0], partitions[2], partitions[3], partitions[4]));
    List<Partition> partitionsInDestTable =
        client.listPartitions(destTable.getDbName(), destTable.getTableName(), MAX);
    Assert.assertEquals(1, partitionsInDestTable.size());
    Assert.assertEquals(partitions[1].getValues(), partitionsInDestTable.get(0).getValues());
    Assert.assertTrue(
        metaStore.isPathExists(new Path(partitionsInDestTable.get(0).getSd().getLocation())));
    Partition resultPart = client.getPartition(sourceTable.getDbName(),
        sourceTable.getTableName(), partitions[1].getValues());
    Assert.assertNotNull(resultPart);
    Assert.assertTrue(metaStore.isPathExists(new Path(partitions[1].getSd().getLocation())));
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionDifferentColsInTables() throws Exception {

    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("test_id", INT_COL_TYPE, "test col id"));
    cols.add(new FieldSchema("test_value", STRING_COL_TYPE, "test col value"));
    cols.add(new FieldSchema("test_name", STRING_COL_TYPE, "test col name"));
    Table dest = createTable(DB_NAME, "test_dest_table", getYearMonthAndDayPartCols(), cols, null);

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partition(partitionSpecs, sourceTable.getDbName(), sourceTable.getTableName(),
        dest.getDbName(), dest.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionDifferentColNameInTables() throws Exception {

    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("id", INT_COL_TYPE, "test col id"));
    cols.add(new FieldSchema("test_value", STRING_COL_TYPE, "test col value"));
    Table dest = createTable(DB_NAME, "test_dest_table", getYearMonthAndDayPartCols(), cols, null);

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partition(partitionSpecs, sourceTable.getDbName(), sourceTable.getTableName(),
        dest.getDbName(), dest.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionDifferentColTypesInTables() throws Exception {

    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("test_id", STRING_COL_TYPE, "test col id"));
    cols.add(new FieldSchema("test_value", STRING_COL_TYPE, "test col value"));
    Table dest = createTable(DB_NAME, "test_dest_table", getYearMonthAndDayPartCols(), cols, null);

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partition(partitionSpecs, sourceTable.getDbName(), sourceTable.getTableName(),
        dest.getDbName(), dest.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionDifferentPartColsInTables() throws Exception {

    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema(YEAR_COL_NAME, STRING_COL_TYPE, "year part col"));
    cols.add(new FieldSchema(MONTH_COL_NAME, STRING_COL_TYPE, "month part col"));
    Table dest = createTable(DB_NAME, "test_dest_table", cols, null);

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partition(partitionSpecs, sourceTable.getDbName(),
        sourceTable.getTableName(), dest.getDbName(), dest.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionDifferentPartColNameInTables() throws Exception {

    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema(YEAR_COL_NAME, STRING_COL_TYPE, "year part col"));
    cols.add(new FieldSchema(MONTH_COL_NAME, STRING_COL_TYPE, "month part col"));
    cols.add(new FieldSchema("nap", STRING_COL_TYPE, "day part col"));
    Table dest = createTable(DB_NAME, "test_dest_table", cols, null);

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partition(partitionSpecs, sourceTable.getDbName(),
        sourceTable.getTableName(), dest.getDbName(), dest.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionDifferentPartColTypesInTables() throws Exception {

    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema(YEAR_COL_NAME, STRING_COL_TYPE, "year part col"));
    cols.add(new FieldSchema(MONTH_COL_NAME, INT_COL_TYPE, "month part col"));
    cols.add(new FieldSchema(DAY_COL_NAME, STRING_COL_TYPE, "day part col"));
    Table dest = createTable(DB_NAME, "test_dest_table", cols, null);

    Map<String, String> partitionSpecs = getPartitionSpec(partitions[1]);
    client.exchange_partition(partitionSpecs, sourceTable.getDbName(),
        sourceTable.getTableName(), dest.getDbName(), dest.getTableName());
  }

  @Test
  public void testExchangePartitionLessValueInPartSpec() throws Exception {

    Map<String, String> partitionSpecs = new HashMap<>();
    partitionSpecs.put(YEAR_COL_NAME, "2017");
    partitionSpecs.put(MONTH_COL_NAME, "march");
    client.exchange_partition(partitionSpecs, sourceTable.getDbName(),
        sourceTable.getTableName(), destTable.getDbName(), destTable.getTableName());
    checkExchangedPartitions(sourceTable, destTable,
        Lists.newArrayList(partitions[0], partitions[1]));
    checkRemainingPartitions(sourceTable, destTable,
        Lists.newArrayList(partitions[2], partitions[3], partitions[4]));
  }

  @Test
  public void testExchangePartitionMoreValueInPartSpec() throws Exception {

    Map<String, String> partitionSpecs = new HashMap<>();
    partitionSpecs.put(YEAR_COL_NAME, "2017");
    partitionSpecs.put(MONTH_COL_NAME, "march");
    partitionSpecs.put(DAY_COL_NAME, "22");
    partitionSpecs.put("hour", "18");
    client.exchange_partition(partitionSpecs, sourceTable.getDbName(),
        sourceTable.getTableName(), destTable.getDbName(), destTable.getTableName());
    checkExchangedPartitions(sourceTable, destTable, Lists.newArrayList(partitions[1]));
    checkRemainingPartitions(sourceTable, destTable,
        Lists.newArrayList(partitions[0], partitions[2], partitions[3], partitions[4]));
  }

  @Test
  public void testExchangePartitionDifferentValuesInPartSpec() throws Exception {

    Map<String, String> partitionSpecs = new HashMap<>();
    partitionSpecs.put(YEAR_COL_NAME, "2017");
    partitionSpecs.put("honap", "march");
    partitionSpecs.put("nap", "22");
    client.exchange_partition(partitionSpecs, sourceTable.getDbName(),
        sourceTable.getTableName(), destTable.getDbName(), destTable.getTableName());
    checkExchangedPartitions(sourceTable, destTable,
        Lists.newArrayList(partitions[0], partitions[1], partitions[2], partitions[3]));
    checkRemainingPartitions(sourceTable, destTable, Lists.newArrayList(partitions[4]));
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionNonExistingValuesInPartSpec() throws Exception {

    Map<String, String> partitionSpecs = new HashMap<>();
    partitionSpecs.put("ev", "2017");
    partitionSpecs.put("honap", "march");
    partitionSpecs.put("nap", "22");
    client.exchange_partition(partitionSpecs, sourceTable.getDbName(),
        sourceTable.getTableName(), destTable.getDbName(), destTable.getTableName());
  }

  @Test
  public void testExchangePartitionOnlyMonthSetInPartSpec() throws Exception {

    Map<String, String> partitionSpecs = new HashMap<>();
    partitionSpecs.put(YEAR_COL_NAME, "");
    partitionSpecs.put(MONTH_COL_NAME, "march");
    partitionSpecs.put(DAY_COL_NAME, "");
    try {
      client.exchange_partition(partitionSpecs, sourceTable.getDbName(),
          sourceTable.getTableName(), destTable.getDbName(), destTable.getTableName());
      Assert.fail("MetaException should have been thrown.");
    } catch (MetaException e) {
      // Expected exception
    }
    checkRemainingPartitions(sourceTable, destTable, Lists.newArrayList(partitions[0],
        partitions[1], partitions[2], partitions[3], partitions[4]));
    List<Partition> partsInDestTable =
        client.listPartitions(destTable.getDbName(), destTable.getTableName(), MAX);
    Assert.assertTrue(partsInDestTable.isEmpty());
  }

  @Test
  public void testExchangePartitionYearAndDaySetInPartSpec() throws Exception {

    Map<String, String> partitionSpecs = new HashMap<>();
    partitionSpecs.put(YEAR_COL_NAME, "2017");
    partitionSpecs.put(MONTH_COL_NAME, "");
    partitionSpecs.put(DAY_COL_NAME, "22");
    try {
      client.exchange_partition(partitionSpecs, sourceTable.getDbName(),
          sourceTable.getTableName(), destTable.getDbName(), destTable.getTableName());
      Assert.fail("MetaException should have been thrown.");
    } catch (MetaException e) {
      // Expected exception
    }
    checkRemainingPartitions(sourceTable, destTable, Lists.newArrayList(partitions[0],
        partitions[1], partitions[2], partitions[3], partitions[4]));
    List<Partition> partsInDestTable =
        client.listPartitions(destTable.getDbName(), destTable.getTableName(), MAX);
    Assert.assertTrue(partsInDestTable.isEmpty());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionNoPartExists() throws Exception {

    Map<String, String> partitionSpecs =
        getPartitionSpec(Lists.newArrayList("2017", "march", "25"));
    client.exchange_partition(partitionSpecs, sourceTable.getDbName(),
        sourceTable.getTableName(), destTable.getDbName(), destTable.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionNoPartExistsYearAndMonthSet() throws Exception {

    Map<String, String> partitionSpecs = getPartitionSpec(Lists.newArrayList("2017", "august", ""));
    client.exchange_partition(partitionSpecs, sourceTable.getDbName(),
        sourceTable.getTableName(), destTable.getDbName(), destTable.getTableName());
  }

  // Helper methods
  protected void createTestTables() throws Exception {
    createDB(DB_NAME);
    sourceTable = createSourceTable();
    destTable = createDestTable();
    partitions = createTestPartitions();
  }

  private void createDB(String dbName) throws TException {
    new DatabaseBuilder()
        .setName(dbName)
        .create(client, metaStore.getConf());
  }

  private Table createSourceTable() throws Exception {
    return createTable(DB_NAME, "test_part_exch_table_source", getYearMonthAndDayPartCols(), null);
  }

  private Table createDestTable() throws Exception {
    return createTable(DB_NAME, "test_part_exch_table_dest", getYearMonthAndDayPartCols(), null);
  }

  protected Table createTable(String dbName, String tableName, List<FieldSchema> partCols,
      String location) throws Exception {
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("test_id", INT_COL_TYPE, "test col id"));
    cols.add(new FieldSchema("test_value", "string", "test col value"));
    return createTable(dbName, tableName, partCols, cols, location);
  }

  protected Table createTable(String dbName, String tableName, List<FieldSchema> partCols,
      List<FieldSchema> cols, String location) throws Exception {
    new TableBuilder()
        .setDbName(dbName)
        .setTableName(tableName)
        .setType(TableType.EXTERNAL_TABLE.name())
        .setCols(cols)
        .setPartCols(partCols)
        .setLocation(location)
        .create(client, metaStore.getConf());
    return client.getTable(dbName, tableName);
  }

  private Partition[] createTestPartitions() throws Exception {
    Partition partition1 =
        buildPartition(sourceTable, Lists.newArrayList("2017", "march", "15"), null);
    Partition partition2 =
        buildPartition(sourceTable, Lists.newArrayList("2017", "march", "22"), null);
    Partition partition3 =
        buildPartition(sourceTable, Lists.newArrayList("2017", "april", "23"), null);
    Partition partition4 =
        buildPartition(sourceTable, Lists.newArrayList("2017", "may", "23"), null);
    Partition partition5 =
        buildPartition(sourceTable, Lists.newArrayList("2018", "april", "23"), null);
    client.add_partitions(
        Lists.newArrayList(partition1, partition2, partition3, partition4, partition5));

    Partition[] parts = new Partition[5];
    parts[0] =
        client.getPartition(DB_NAME, sourceTable.getTableName(), partition1.getValues());
    parts[1] =
        client.getPartition(DB_NAME, sourceTable.getTableName(), partition2.getValues());
    parts[2] =
        client.getPartition(DB_NAME, sourceTable.getTableName(), partition3.getValues());
    parts[3] =
        client.getPartition(DB_NAME, sourceTable.getTableName(), partition4.getValues());
    parts[4] =
        client.getPartition(DB_NAME, sourceTable.getTableName(), partition5.getValues());
    return parts;
  }

  protected Partition createPartition(Table table, List<String> values, String location)
      throws Exception {
    Partition partition = buildPartition(table, values, location);
    client.add_partition(partition);
    return client.getPartition(DB_NAME, table.getTableName(), partition.getValues());
  }

  private Partition buildPartition(Table table, List<String> values, String location)
      throws MetaException {
    Partition partition = new PartitionBuilder()
        .setDbName(table.getDbName())
        .setTableName(table.getTableName())
        .setValues(values)
        .addPartParam("test_exch_param_key", "test_exch_param_value")
        .setInputFormat("TestInputFormat")
        .setOutputFormat("TestOutputFormat")
        .addStorageDescriptorParam("test_exch_sd_param_key", "test_exch_sd_param_value")
        .setCols(getYearMonthAndDayPartCols())
        .setLocation(location)
        .build(metaStore.getConf());
    return partition;
  }

  protected void checkExchangedPartitions(Table sourceTable, Table destTable,
      List<Partition> partitions) throws Exception {

    for (Partition partition : partitions) {
      // Check if the partitions exist in the destTable
      Partition resultPart = client.getPartition(destTable.getDbName(), destTable.getTableName(),
          partition.getValues());
      Assert.assertNotNull(resultPart);
      Assert.assertEquals(destTable.getDbName(), resultPart.getDbName());
      Assert.assertEquals(destTable.getTableName(), resultPart.getTableName());
      // Check the location of the result partition. It should be located in the destination table
      // folder.
      String partName =
          Warehouse.makePartName(sourceTable.getPartitionKeys(), partition.getValues());
      Assert.assertEquals(destTable.getSd().getLocation() + "/" + partName,
          resultPart.getSd().getLocation());
      Assert.assertTrue(metaStore.isPathExists(new Path(resultPart.getSd().getLocation())));

      // Check if the partitions don't exist in the sourceTable
      try {
        client.getPartition(sourceTable.getDbName(), sourceTable.getTableName(),
            partition.getValues());
        Assert.fail("The partition ' " + partition.getValues().toString()
            + " ' should not exists, therefore NoSuchObjectException should have been thrown.");
      } catch (NoSuchObjectException e) {
        // Expected exception
      }
      Assert.assertFalse(metaStore.isPathExists(new Path(partition.getSd().getLocation())));

      // Reset the location, db and table name and compare the partitions
      partition.getSd().setLocation(resultPart.getSd().getLocation());
      partition.setDbName(destTable.getDbName());
      partition.setTableName(destTable.getTableName());
      Assert.assertEquals(partition, resultPart);
    }
  }

  protected void checkRemainingPartitions(Table sourceTable, Table destTable,
      List<Partition> partitions) throws Exception {

    for (Partition partition : partitions) {
      // Check if the partitions exist in the sourceTable
      Partition resultPart = client.getPartition(sourceTable.getDbName(),
          sourceTable.getTableName(), partition.getValues());
      Assert.assertNotNull(resultPart);
      Assert.assertEquals(partition, resultPart);
      Assert.assertTrue(metaStore.isPathExists(new Path(partition.getSd().getLocation())));

      // Check if the partitions don't exist in the destTable
      try {
        client.getPartition(destTable.getDbName(), destTable.getTableName(), partition.getValues());
        Assert.fail("The partition '" + partition.getValues().toString()
            + "'should not exists, therefore NoSuchObjectException should have been thrown.");
      } catch (NoSuchObjectException e) {
        // Expected exception
      }
      String partName =
          Warehouse.makePartName(sourceTable.getPartitionKeys(), partition.getValues());
      Assert.assertFalse(
          metaStore.isPathExists(new Path(destTable.getSd().getLocation() + "/" + partName)));
    }
  }

  protected static Map<String, String> getPartitionSpec(Partition partition) {
    return getPartitionSpec(partition.getValues());
  }

  protected static Map<String, String> getPartitionSpec(List<String> values) {
    Map<String, String> partitionSpecs = new HashMap<>();
    List<FieldSchema> partCols = getYearMonthAndDayPartCols();
    for (int i = 0; i < partCols.size(); i++) {
      FieldSchema partCol = partCols.get(i);
      String value = values.get(i);
      partitionSpecs.put(partCol.getName(), value);
    }
    return partitionSpecs;
  }

  protected static List<FieldSchema> getYearMonthAndDayPartCols() {
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema(YEAR_COL_NAME, STRING_COL_TYPE, "year part col"));
    cols.add(new FieldSchema(MONTH_COL_NAME, STRING_COL_TYPE, "month part col"));
    cols.add(new FieldSchema(DAY_COL_NAME, STRING_COL_TYPE, "day part col"));
    return cols;
  }
}
