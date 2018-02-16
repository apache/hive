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
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
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
 * Tests for dropping partitions.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestDropPartitions {

  // Needed until there is no junit release with @BeforeParam, @AfterParam (junit 4.13)
  // https://github.com/junit-team/junit4/commit/1bf8438b65858565dbb64736bfe13aae9cfc1b5a
  // Then we should remove our own copy
  private static Set<AbstractMetaStoreService> metaStoreServices = null;
  private AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;

  private static final String DB_NAME = "test_drop_part_db";
  private static final String TABLE_NAME = "test_drop_part_table";
  private static final String DEFAULT_COL_TYPE = "string";
  private static final String YEAR_COL_NAME = "year";
  private static final String MONTH_COL_NAME = "month";
  private static final short MAX = -1;
  private static final Partition[] PARTITIONS = new Partition[3];

  @Parameterized.Parameters(name = "{0}")
  public static List<Object[]> getMetaStoreToTest() throws Exception {
    List<Object[]> result = MetaStoreFactoryForTests.getMetaStores();
    metaStoreServices = result.stream()
                            .map(test -> (AbstractMetaStoreService)test[1])
                            .collect(Collectors.toSet());
    return result;
  }

  public TestDropPartitions(String name, AbstractMetaStoreService metaStore) throws Exception {
    this.metaStore = metaStore;
    Map<MetastoreConf.ConfVars, String> msConf = new HashMap<MetastoreConf.ConfVars, String>();
    // Enable trash, so it can be tested
    Map<String, String> extraConf = new HashMap<String, String>();
    extraConf.put("fs.trash.checkpoint.interval", "30");  // FS_TRASH_CHECKPOINT_INTERVAL_KEY
    extraConf.put("fs.trash.interval", "30");             // FS_TRASH_INTERVAL_KEY (hadoop-2)

    this.metaStore.start(msConf, extraConf);
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

    // Create test tables with 3 partitions
    createTable(TABLE_NAME, getYearAndMonthPartCols(), null);
    PARTITIONS[0] = createPartition(Lists.newArrayList("2017", "march"), getYearAndMonthPartCols());
    PARTITIONS[1] = createPartition(Lists.newArrayList("2017", "april"), getYearAndMonthPartCols());
    PARTITIONS[2] = createPartition(Lists.newArrayList("2018", "march"), getYearAndMonthPartCols());
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

  // Tests for dropPartition(String db_name, String tbl_name, List<String> part_vals,
  // boolean deleteData) method

  @Test
  public void testDropPartition() throws Exception {

    boolean dropSuccessful =
        client.dropPartition(DB_NAME, TABLE_NAME, PARTITIONS[0].getValues(), false);
    Assert.assertTrue(dropSuccessful);
    List<Partition> droppedPartitions = Lists.newArrayList(PARTITIONS[0]);
    List<Partition> remainingPartitions = Lists.newArrayList(PARTITIONS[1], PARTITIONS[2]);
    checkPartitionsAfterDelete(TABLE_NAME, droppedPartitions, remainingPartitions, false, false);
  }

  @Test
  public void testDropPartitionDeleteData() throws Exception {

    client.dropPartition(DB_NAME, TABLE_NAME, PARTITIONS[0].getValues(), true);
    List<Partition> droppedPartitions = Lists.newArrayList(PARTITIONS[0]);
    List<Partition> remainingPartitions = Lists.newArrayList(PARTITIONS[1], PARTITIONS[2]);
    checkPartitionsAfterDelete(TABLE_NAME, droppedPartitions, remainingPartitions, true, false);
  }

  @Test
  public void testDropPartitionDeleteParentDir() throws Exception {

    client.dropPartition(DB_NAME, TABLE_NAME, PARTITIONS[0].getValues(), true);
    client.dropPartition(DB_NAME, TABLE_NAME, PARTITIONS[1].getValues(), true);

    List<Partition> droppedPartitions = Lists.newArrayList(PARTITIONS[0], PARTITIONS[1]);
    List<Partition> remainingPartitions = Lists.newArrayList(PARTITIONS[2]);
    checkPartitionsAfterDelete(TABLE_NAME, droppedPartitions, remainingPartitions, true, false);
    Path parentPath = new Path(PARTITIONS[0].getSd().getLocation()).getParent();
    Assert.assertFalse("The parent path '" + parentPath.toString() + "' should not exist.",
        metaStore.isPathExists(parentPath));
  }

  @Test
  public void testDropPartitionDeleteDataPurge() throws Exception {

    String tableName = "purge_test";
    Map<String, String> tableParams = new HashMap<>();
    tableParams.put("auto.purge", "true");
    createTable(tableName, getYearPartCol(), tableParams);

    Partition partition1 =
        createPartition(tableName, null, Lists.newArrayList("2017"), getYearPartCol(), null);
    Partition partition2 =
        createPartition(tableName, null, Lists.newArrayList("2018"), getYearPartCol(), null);

    client.dropPartition(DB_NAME, tableName, partition1.getValues(), true);
    List<Partition> droppedPartitions = Lists.newArrayList(partition1);
    List<Partition> remainingPartitions = Lists.newArrayList(partition2);
    checkPartitionsAfterDelete(tableName, droppedPartitions, remainingPartitions, true, true);
  }

  @Test
  public void testDropPartitionArchivedPartition() throws Exception {

    String originalLocation = metaStore.getWarehouseRoot() + "/" + TABLE_NAME + "/2016_may";
    String location = metaStore.getWarehouseRoot() + "/" + TABLE_NAME + "/year=2016/month=may";
    metaStore.createFile(new Path(originalLocation), "test");

    Map<String, String> partParams = new HashMap<>();
    partParams.put("is_archived", "true");
    partParams.put("original_location", originalLocation);
    Partition partition = createPartition(TABLE_NAME, location, Lists.newArrayList("2016", "may"),
        getYearAndMonthPartCols(), partParams);

    client.dropPartition(DB_NAME, TABLE_NAME, Lists.newArrayList("2016", "may"), true);
    List<Partition> partitionsAfterDelete = client.listPartitions(DB_NAME, TABLE_NAME, MAX);
    Assert.assertFalse(partitionsAfterDelete.contains(partition));
    Assert.assertTrue("The location '" + location + "' should exist.",
        metaStore.isPathExists(new Path(location)));
    Assert.assertFalse("The original location '" + originalLocation + "' should not exist.",
        metaStore.isPathExists(new Path(originalLocation)));
  }

  @Test
  public void testDropPartitionExternalTable() throws Exception {

    String tableName = "external_table";
    Map<String, String> tableParams = new HashMap<>();
    tableParams.put("EXTERNAL", "true");
    tableParams.put("auto.purge", "true");
    createTable(tableName, getYearPartCol(), tableParams);

    String location = metaStore.getWarehouseRoot() + "/externalTable/year=2017";
    Partition partition =
        createPartition(tableName, location, Lists.newArrayList("2017"), getYearPartCol(), null);

    client.dropPartition(DB_NAME, tableName, partition.getValues(), true);
    List<Partition> partitionsAfterDelete = client.listPartitions(DB_NAME, tableName, MAX);
    Assert.assertTrue(partitionsAfterDelete.isEmpty());
    Assert.assertTrue("The location '" + location + "' should exist.",
        metaStore.isPathExists(new Path(location)));
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDropPartitionNonExistingDB() throws Exception {

    client.dropPartition("nonexistingdb", TABLE_NAME, Lists.newArrayList("2017"), false);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDropPartitionNonExistingTable() throws Exception {

    client.dropPartition(DB_NAME, "nonexistingtable", Lists.newArrayList("2017"), false);
  }

  @Test(expected = MetaException.class)
  public void testDropPartitionNullDB() throws Exception {

    client.dropPartition(null, TABLE_NAME, Lists.newArrayList("2017"), false);
  }

  @Test(expected = MetaException.class)
  public void testDropPartitionNullTable() throws Exception {

    client.dropPartition(DB_NAME, null, Lists.newArrayList("2017"), false);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDropPartitionEmptyDB() throws Exception {

    client.dropPartition("", TABLE_NAME, Lists.newArrayList("2017"), false);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDropPartitionEmptyTable() throws Exception {

    client.dropPartition(DB_NAME, "", Lists.newArrayList("2017"), false);
  }

  @Test(expected = MetaException.class)
  public void testDropPartitionNullPartVals() throws Exception {

    List<String> partVals = null;
    client.dropPartition(DB_NAME, TABLE_NAME, partVals, false);
  }

  @Test(expected = MetaException.class)
  public void testDropPartitionEmptyPartVals() throws Exception {

    List<String> partVals = new ArrayList<>();
    client.dropPartition(DB_NAME, TABLE_NAME, partVals, false);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDropPartitionNonExistingPartVals() throws Exception {

    client.dropPartition(DB_NAME, TABLE_NAME, Lists.newArrayList("2017", "may"), false);
  }

  @Test
  public void testDropPartitionNullVal() throws Exception {

    List<String> partVals = new ArrayList<>();
    partVals.add(null);
    partVals.add(null);
    try {
      client.dropPartition(DB_NAME, TABLE_NAME, partVals, false);
      Assert.fail("NullPointerException or NoSuchObjectException is expected to be thrown");
    } catch (NullPointerException | NoSuchObjectException e) {
      // TODO: Should not throw NPE.
    }
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDropPartitionEmptyVal() throws Exception {

    List<String> partVals = new ArrayList<>();
    partVals.add("");
    partVals.add("");
    client.dropPartition(DB_NAME, TABLE_NAME, partVals, false);
  }

  @Test(expected = MetaException.class)
  public void testDropPartitionMoreValsInList() throws Exception {

    client.dropPartition(DB_NAME, TABLE_NAME, Lists.newArrayList("2017", "march", "12:00"), false);
  }

  @Test(expected = MetaException.class)
  public void testDropPartitionLessValsInList() throws Exception {

    client.dropPartition(DB_NAME, TABLE_NAME, Lists.newArrayList("2017"), false);
  }

  // Tests for dropPartition(String db_name, String tbl_name, List<String> part_vals,
  // PartitionDropOptions options) method

  @Test
  public void testDropPartitionNotDeleteData() throws Exception {

    PartitionDropOptions partDropOptions = PartitionDropOptions.instance();
    partDropOptions.deleteData(false);
    partDropOptions.purgeData(false);

    client.dropPartition(DB_NAME, TABLE_NAME, PARTITIONS[0].getValues(), partDropOptions);
    List<Partition> droppedPartitions = Lists.newArrayList(PARTITIONS[0]);
    List<Partition> remainingPartitions = Lists.newArrayList(PARTITIONS[1], PARTITIONS[2]);
    checkPartitionsAfterDelete(TABLE_NAME, droppedPartitions, remainingPartitions, false, false);
  }

  @Test
  public void testDropPartitionDeleteDataNoPurge() throws Exception {

    PartitionDropOptions partDropOptions = PartitionDropOptions.instance();
    partDropOptions.deleteData(true);
    partDropOptions.purgeData(false);

    client.dropPartition(DB_NAME, TABLE_NAME, PARTITIONS[0].getValues(), partDropOptions);
    List<Partition> droppedPartitions = Lists.newArrayList(PARTITIONS[0]);
    List<Partition> remainingPartitions = Lists.newArrayList(PARTITIONS[1], PARTITIONS[2]);
    checkPartitionsAfterDelete(TABLE_NAME, droppedPartitions, remainingPartitions, true, false);
  }

  @Test
  public void testDropPartitionDeleteDataAndPurge() throws Exception {

    PartitionDropOptions partDropOptions = PartitionDropOptions.instance();
    partDropOptions.deleteData(true);
    partDropOptions.purgeData(true);

    client.dropPartition(DB_NAME, TABLE_NAME, PARTITIONS[0].getValues(), partDropOptions);
    List<Partition> droppedPartitions = Lists.newArrayList(PARTITIONS[0]);
    List<Partition> remainingPartitions = Lists.newArrayList(PARTITIONS[1], PARTITIONS[2]);
    checkPartitionsAfterDelete(TABLE_NAME, droppedPartitions, remainingPartitions, true, true);
  }

  @Test
  public void testDropPartitionDeleteDataAndPurgeExternalTable() throws Exception {

    String tableName = "external_table";
    Map<String, String> tableParams = new HashMap<>();
    tableParams.put("EXTERNAL", "true");
    createTable(tableName, getYearPartCol(), tableParams);

    String location = metaStore.getWarehouseRoot() + "/externalTable/year=2017";
    Partition partition =
        createPartition(tableName, location, Lists.newArrayList("2017"), getYearPartCol(), null);

    PartitionDropOptions partDropOptions = PartitionDropOptions.instance();
    partDropOptions.deleteData(true);
    partDropOptions.purgeData(true);

    client.dropPartition(DB_NAME, tableName, partition.getValues(), partDropOptions);
    List<Partition> partitionsAfterDrop = client.listPartitions(DB_NAME, tableName, MAX);
    Assert.assertTrue(partitionsAfterDrop.isEmpty());
    Assert.assertTrue("The location '" + location + "' should exist.",
        metaStore.isPathExists(new Path(location)));
  }

  @Test
  public void testDropPartitionNotDeleteDataPurge() throws Exception {

    PartitionDropOptions partDropOptions = PartitionDropOptions.instance();
    partDropOptions.deleteData(false);
    partDropOptions.purgeData(true);

    client.dropPartition(DB_NAME, TABLE_NAME, PARTITIONS[0].getValues(), partDropOptions);
    List<Partition> droppedPartitions = Lists.newArrayList(PARTITIONS[0]);
    List<Partition> remainingPartitions = Lists.newArrayList(PARTITIONS[1], PARTITIONS[2]);
    checkPartitionsAfterDelete(TABLE_NAME, droppedPartitions, remainingPartitions, false, false);
  }

  @Test
  public void testDropPartitionPurgeSetInTable() throws Exception {

    PartitionDropOptions partDropOptions = PartitionDropOptions.instance();
    partDropOptions.deleteData(true);
    partDropOptions.purgeData(false);

    String tableName = "purge_test";
    Map<String, String> tableParams = new HashMap<>();
    tableParams.put("auto.purge", "true");
    createTable(tableName, getYearPartCol(), tableParams);

    Partition partition1 =
        createPartition(tableName, null, Lists.newArrayList("2017"), getYearPartCol(), null);
    Partition partition2 =
        createPartition(tableName, null, Lists.newArrayList("2018"), getYearPartCol(), null);

    client.dropPartition(DB_NAME, tableName, partition1.getValues(), true);
    List<Partition> droppedPartitions = Lists.newArrayList(partition1);
    List<Partition> remainingPartitions = Lists.newArrayList(partition2);
    checkPartitionsAfterDelete(tableName, droppedPartitions, remainingPartitions, true, true);
  }

  @Test(expected = NullPointerException.class)
  public void testDropPartitionNullPartDropOptions() throws Exception {
    // TODO: This should not throw NPE
    client.dropPartition(DB_NAME, TABLE_NAME, PARTITIONS[0].getValues(), null);
  }

  // Tests for dropPartition(String db_name, String tbl_name, String name,
  // boolean deleteData) method

  @Test
  public void testDropPartitionByName() throws Exception {

    client.dropPartition(DB_NAME, TABLE_NAME, "year=2017/month=march", false);
    List<Partition> droppedPartitions = Lists.newArrayList(PARTITIONS[0]);
    List<Partition> remainingPartitions = Lists.newArrayList(PARTITIONS[1], PARTITIONS[2]);
    checkPartitionsAfterDelete(TABLE_NAME, droppedPartitions, remainingPartitions, false, false);
  }

  @Test
  public void testDropPartitionByNameLessValue() throws Exception {

    try {
      client.dropPartition(DB_NAME, TABLE_NAME, "year=2017", true);
      Assert.fail("NoSuchObjectException should be thrown.");
    } catch (NoSuchObjectException e) {
      // Expected exception
    }
    List<Partition> droppedPartitions = new ArrayList<>();
    List<Partition> remainingPartitions =
        Lists.newArrayList(PARTITIONS[0], PARTITIONS[1], PARTITIONS[2]);
    checkPartitionsAfterDelete(TABLE_NAME, droppedPartitions, remainingPartitions, false, false);
  }

  @Test
  public void testDropPartitionByNameMoreValue() throws Exception {
    // The extra non existing values will be ignored.
    client.dropPartition(DB_NAME, TABLE_NAME, "year=2017/month=march/day=10", true);
    List<Partition> droppedPartitions = Lists.newArrayList(PARTITIONS[0]);
    List<Partition> remainingPartitions = Lists.newArrayList(PARTITIONS[1], PARTITIONS[2]);
    checkPartitionsAfterDelete(TABLE_NAME, droppedPartitions, remainingPartitions, true, false);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDropPartitionByNameNonExistingPart() throws Exception {

    client.dropPartition(DB_NAME, TABLE_NAME, "year=2017/month=may", true);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDropPartitionByNameNonExistingTable() throws Exception {

    client.dropPartition(DB_NAME, "nonexistingtable", "year=2017/month=may", true);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDropPartitionByNameNonExistingDB() throws Exception {

    client.dropPartition("nonexistingdb", TABLE_NAME, "year=2017/month=may", true);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDropPartitionByNameInvalidName() throws Exception {

    client.dropPartition(DB_NAME, TABLE_NAME, "ev=2017/honap=march", true);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDropPartitionByNameInvalidNameFormat() throws Exception {

    client.dropPartition(DB_NAME, TABLE_NAME, "invalidnameformat", true);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDropPartitionByNameInvalidNameNoValues() throws Exception {

    client.dropPartition(DB_NAME, TABLE_NAME, "year=/month=", true);
  }

  @Test(expected = MetaException.class)
  public void testDropPartitionByNameNullName() throws Exception {

    String name = null;
    client.dropPartition(DB_NAME, TABLE_NAME, name, true);
  }

  @Test(expected = MetaException.class)
  public void testDropPartitionByNameEmptyName() throws Exception {

    client.dropPartition(DB_NAME, TABLE_NAME, "", true);
  }

    // Helper methods

  private Table createTable(String tableName, List<FieldSchema> partCols,
      Map<String, String> tableParams) throws Exception {
    Table table = new TableBuilder()
        .setDbName(DB_NAME)
        .setTableName(tableName)
        .addCol("test_id", "int", "test col id")
        .addCol("test_value", "string", "test col value")
        .setPartCols(partCols)
        .setLocation(metaStore.getWarehouseRoot() + "/" + tableName)
        .setTableParams(tableParams)
        .build();
    client.createTable(table);
    return table;
  }

  private Partition createPartition(List<String> values,
      List<FieldSchema> partCols) throws Exception {
    Partition partition = new PartitionBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .setValues(values)
        .setCols(partCols)
        .build();
    client.add_partition(partition);
    partition = client.getPartition(DB_NAME, TABLE_NAME, values);
    return partition;
  }

  private Partition createPartition(String tableName, String location, List<String> values,
      List<FieldSchema> partCols, Map<String, String> partParams) throws Exception {
    Partition partition = new PartitionBuilder()
        .setDbName(DB_NAME)
        .setTableName(tableName)
        .setValues(values)
        .setCols(partCols)
        .setLocation(location)
        .setPartParams(partParams)
        .build();
    client.add_partition(partition);
    partition = client.getPartition(DB_NAME, tableName, values);
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

  private void checkPartitionsAfterDelete(String tableName, List<Partition> droppedPartitions,
      List<Partition> existingPartitions, boolean deleteData, boolean purge) throws Exception {

    List<Partition> partitions = client.listPartitions(DB_NAME, tableName, MAX);
    Assert
        .assertEquals(
            "The table " + tableName + " has " + partitions.size()
                + " partitions, but it should have " + existingPartitions.size(),
            existingPartitions.size(), partitions.size());
    for (Partition droppedPartition : droppedPartitions) {
      Assert.assertFalse(partitions.contains(droppedPartition));
      Path partitionPath = new Path(droppedPartition.getSd().getLocation());
      if (deleteData) {
        Assert.assertFalse("The location '" + partitionPath.toString() + "' should not exist.",
            metaStore.isPathExists(partitionPath));
        if (purge) {
          Assert.assertFalse(
              "The location '" + partitionPath.toString() + "' should not exist in the trash.",
              metaStore.isPathExistsInTrash(partitionPath));
        } else {
          Assert.assertTrue(
              "The location '" + partitionPath.toString() + "' should exist in the trash.",
              metaStore.isPathExistsInTrash(partitionPath));
        }
      } else {
        Assert.assertTrue("The location '" + partitionPath.toString() + "' should exist.",
            metaStore.isPathExists(partitionPath));
      }
    }

    for (Partition existingPartition : existingPartitions) {
      Assert.assertTrue(partitions.contains(existingPartition));
      Path partitionPath = new Path(existingPartition.getSd().getLocation());
      Assert.assertTrue("The location '" + partitionPath.toString() + "' should exist.",
          metaStore.isPathExists(partitionPath));
    }
  }
}
