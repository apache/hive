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
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionListComposingSpec;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionSpecWithSharedSD;
import org.apache.hadoop.hive.metastore.api.PartitionWithoutSD;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
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
 * Tests for creating partitions from partition spec.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestAddPartitionsFromPartSpec extends MetaStoreClientTest {
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
  private static final int DEFAULT_CREATE_TIME = 123456;
  private static final short MAX = -1;

  public TestAddPartitionsFromPartSpec(String name, AbstractMetaStoreService metaStore) {
    this.metaStore = metaStore;
  }

  @Before
  public void setUp() throws Exception {
    // Get new client
    client = metaStore.getClient();

    // Clean up the database
    client.dropDatabase(DB_NAME, true, true, true);
    metaStore.cleanWarehouseDirs();
    new DatabaseBuilder().
        setName(DB_NAME).
        create(client, metaStore.getConf());
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

  // Tests for int add_partitions_pspec(PartitionSpecProxy partitionSpec) method

  @Test
  public void testAddPartitionSpec() throws Exception {

    Table table = createTable();
    Partition partition1 = buildPartition(Lists.newArrayList("2013"), getYearPartCol(), 1);
    Partition partition2 = buildPartition(Lists.newArrayList("2014"), getYearPartCol(), 2);
    Partition partition3 = buildPartition(Lists.newArrayList("2012"), getYearPartCol(), 3);
    List<Partition> partitions = Lists.newArrayList(partition1, partition2, partition3);

    String rootPath = table.getSd().getLocation() + "/addpartspectest/";
    PartitionSpecProxy partitionSpec =
        buildPartitionSpec(DB_NAME, TABLE_NAME, rootPath, partitions);
    client.add_partitions_pspec(partitionSpec);

    verifyPartition(table, "year=2013", Lists.newArrayList("2013"), 1);
    verifyPartition(table, "year=2014", Lists.newArrayList("2014"), 2);
    verifyPartition(table, "year=2012", Lists.newArrayList("2012"), 3);
  }

  @Test
  public void testAddPartitionSpecWithSharedSD() throws Exception {

    Table table = createTable();
    PartitionWithoutSD partition1 = buildPartitionWithoutSD(Lists.newArrayList("2013"), 1);
    PartitionWithoutSD partition2 = buildPartitionWithoutSD(Lists.newArrayList("2014"), 2);
    PartitionWithoutSD partition3 = buildPartitionWithoutSD(Lists.newArrayList("2012"), 3);
    List<PartitionWithoutSD> partitions = Lists.newArrayList(partition1, partition2, partition3);

    String location = table.getSd().getLocation() + "/sharedSDTest/";
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpecWithSharedSD(partitions, buildSD(location));
    client.add_partitions_pspec(partitionSpecProxy);

    verifyPartitionSharedSD(table, "year=2013", Lists.newArrayList("2013"), 1);
    verifyPartitionSharedSD(table, "year=2014", Lists.newArrayList("2014"), 2);
    verifyPartitionSharedSD(table, "year=2012", Lists.newArrayList("2012"), 3);
  }

  @Test
  public void testAddPartitionSpecsMultipleValues() throws Exception {

    Table table = createTable(DB_NAME, TABLE_NAME, getYearAndMonthPartCols(),
        metaStore.getWarehouseRoot() + "/" + TABLE_NAME);

    Partition partition1 =
        buildPartition(Lists.newArrayList("2002", "march"), getYearAndMonthPartCols(), 1);
    Partition partition2 =
        buildPartition(Lists.newArrayList("2003", "april"), getYearAndMonthPartCols(), 2);
    PartitionWithoutSD partition3 = buildPartitionWithoutSD(Lists.newArrayList("2004", "june"), 3);
    PartitionWithoutSD partition4 = buildPartitionWithoutSD(Lists.newArrayList("2005", "may"), 4);
    List<Partition> partitions = Lists.newArrayList(partition1, partition2);
    List<PartitionWithoutSD> partitionsWithoutSD = Lists.newArrayList(partition3, partition4);

    PartitionSpecProxy partitionSpec = buildPartitionSpec(partitions, partitionsWithoutSD);
    client.add_partitions_pspec(partitionSpec);

    verifyPartition(table, "year=2002/month=march", Lists.newArrayList("2002", "march"), 1);
    verifyPartition(table, "year=2003/month=april", Lists.newArrayList("2003", "april"), 2);
    verifyPartitionSharedSD(table, "year=2004/month=june", Lists.newArrayList("2004", "june"), 3);
    verifyPartitionSharedSD(table, "year=2005/month=may", Lists.newArrayList("2005", "may"), 4);
  }

  // TODO add tests for partitions in other catalogs

  @Test(expected = MetaException.class)
  public void testAddPartitionSpecNullSpec() throws Exception {

    client.add_partitions_pspec(null);
  }

  @Test
  public void testAddPartitionSpecEmptyPartList() throws Exception {

    createTable();
    List<Partition> partitions = new ArrayList<>();
    PartitionSpecProxy partitionSpec = buildPartitionSpec(DB_NAME, TABLE_NAME, null, partitions);
    client.add_partitions_pspec(partitionSpec);
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionSpecNullPartList() throws Exception {

    createTable();
    List<Partition> partitions = null;
    PartitionSpecProxy partitionSpec = buildPartitionSpec(DB_NAME, TABLE_NAME, null, partitions);
    client.add_partitions_pspec(partitionSpec);
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionSpecNoDB() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpec(null, TABLE_NAME, null, Lists.newArrayList(partition));
    client.add_partitions_pspec(partitionSpecProxy);
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionSpecNoTable() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpec(DB_NAME, null, null, Lists.newArrayList(partition));
    client.add_partitions_pspec(partitionSpecProxy);
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionSpecNoDBAndTableInPartition() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    partition.setDbName(null);
    partition.setTableName(null);
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpec(DB_NAME, TABLE_NAME, null, Lists.newArrayList(partition));
    client.add_partitions_pspec(partitionSpecProxy);
  }

  @Test
  public void testAddPartitionSpecDBAndTableSetFromSpecProxy() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    partition.setDbName(null);
    partition.setTableName(null);
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpec(null, null, null, Lists.newArrayList(partition));
    partitionSpecProxy.setDbName(DB_NAME);
    partitionSpecProxy.setTableName(TABLE_NAME);
    client.add_partitions_pspec(partitionSpecProxy);

    Partition resultPart =
        client.getPartition(DB_NAME, TABLE_NAME, Lists.newArrayList(DEFAULT_YEAR_VALUE));
    Assert.assertNotNull(resultPart);
  }

  @Test
  public void testAddPartitionSpecWithSharedSDDBAndTableSetFromSpecProxy() throws Exception {

    createTable();
    PartitionWithoutSD partition =
        buildPartitionWithoutSD(Lists.newArrayList(DEFAULT_YEAR_VALUE), 1);
    String location = metaStore.getWarehouseRoot() + "/" + TABLE_NAME + "/sharedSDTest/";
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpecWithSharedSD(Lists.newArrayList(partition), buildSD(location));
    partitionSpecProxy.setDbName(DB_NAME);
    partitionSpecProxy.setTableName(TABLE_NAME);
    client.add_partitions_pspec(partitionSpecProxy);

    Partition resultPart =
        client.getPartition(DB_NAME, TABLE_NAME, Lists.newArrayList(DEFAULT_YEAR_VALUE));
    Assert.assertNotNull(resultPart);
  }

  @Test(expected = InvalidObjectException.class)
  public void testAddPartitionSpecEmptyDB() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpec("", TABLE_NAME, null, Lists.newArrayList(partition));
    client.add_partitions_pspec(partitionSpecProxy);
  }

  @Test(expected = InvalidObjectException.class)
  public void testAddPartitionSpecEmptyTable() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpec(DB_NAME, "", null, Lists.newArrayList(partition));
    client.add_partitions_pspec(partitionSpecProxy);
  }

  @Test(expected = InvalidObjectException.class)
  public void testAddPartitionSpecNonExistingDB() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpec("nonexistingdb", TABLE_NAME, null, Lists.newArrayList(partition));
    client.add_partitions_pspec(partitionSpecProxy);
  }

  @Test(expected = InvalidObjectException.class)
  public void testAddPartitionSpecNonExistingTable() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpec(DB_NAME, "nonexistingtable", null, Lists.newArrayList(partition));
    client.add_partitions_pspec(partitionSpecProxy);
  }

  @Test
  public void testAddPartitionSpecDiffDBName() throws Exception {

    createDB("NewPartDB");
    createTable();
    createTable("NewPartDB", "NewPartTable", getYearPartCol(), null);
    List<Partition> partitions = new ArrayList<>();
    Partition partition1 = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    Partition partition2 = buildPartition("NewPartDB", "NewPartTable", DEFAULT_YEAR_VALUE);
    partitions.add(partition1);
    partitions.add(partition2);
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpec(DB_NAME, TABLE_NAME, null, partitions);
    try {
      client.add_partitions_pspec(partitionSpecProxy);
      Assert.fail("MetaException should have been thrown.");
    } catch (MetaException e) {
      // Expected exception
    } finally {
      client.dropDatabase("NewPartDB", true, true, true);
    }
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionSpecNullPart() throws Exception {

    createTable();
    List<Partition> partitions = new ArrayList<>();
    Partition partition1 = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    Partition partition2 = null;
    partitions.add(partition1);
    partitions.add(partition2);
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpec(DB_NAME, TABLE_NAME, null, partitions);
    client.add_partitions_pspec(partitionSpecProxy);
  }

  @Test
  public void testAddPartitionSpecUnsupportedPartSpecType() throws Exception {

    createTable();
    PartitionSpec partitionSpec = new PartitionSpec();
    partitionSpec.setDbName(DB_NAME);
    partitionSpec.setTableName(TABLE_NAME);
    partitionSpec.setPartitionList(null);
    partitionSpec.setSharedSDPartitionSpec(null);
    try {
      PartitionSpecProxy bubu = PartitionSpecProxy.Factory.get(partitionSpec);
      client.add_partitions_pspec(bubu);
      Assert.fail("AssertionError should have been thrown.");
    } catch (AssertionError e) {
      // Expected error
    }
  }

  @Test
  public void testAddPartitionSpecBothTypeSet() throws Exception {

    Table table = createTable();
    Partition partition = buildPartition(Lists.newArrayList("2013"), getYearPartCol(), 1);
    PartitionWithoutSD partitionWithoutSD = buildPartitionWithoutSD(Lists.newArrayList("2014"), 0);

    PartitionSpec partitionSpec = new PartitionSpec();
    partitionSpec.setDbName(DB_NAME);
    partitionSpec.setTableName(TABLE_NAME);
    PartitionListComposingSpec partitionListComposingSpec = new PartitionListComposingSpec();
    partitionListComposingSpec.setPartitions(Lists.newArrayList(partition));
    partitionSpec.setPartitionList(partitionListComposingSpec);

    PartitionSpecWithSharedSD partitionSpecWithSharedSD = new PartitionSpecWithSharedSD();
    partitionSpecWithSharedSD.setPartitions(Lists.newArrayList(partitionWithoutSD));
    partitionSpecWithSharedSD.setSd(buildSD(table.getSd().getLocation() + "/sharedSDTest/"));
    partitionSpec.setSharedSDPartitionSpec(partitionSpecWithSharedSD);

    PartitionSpecProxy partitionSpecProxy = PartitionSpecProxy.Factory.get(partitionSpec);
    client.add_partitions_pspec(partitionSpecProxy);

    List<String> partitionNames = client.listPartitionNames(DB_NAME, TABLE_NAME, MAX);
    Assert.assertNotNull(partitionNames);
    Assert.assertTrue(partitionNames.size() == 1);
    Assert.assertEquals("year=2013", partitionNames.get(0));
  }

  @Test
  public void testAddPartitionSpecSetRootPath() throws Exception {

    Table table = createTable();
    String rootPath = table.getSd().getLocation() + "/addPartSpecRootPath/";
    String rootPath1 = table.getSd().getLocation() + "/someotherpath/";

    Partition partition = buildPartition(DB_NAME, TABLE_NAME, "2007", rootPath + "part2007/");
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpec(DB_NAME, TABLE_NAME, rootPath1, Lists.newArrayList(partition));
    client.add_partitions_pspec(partitionSpecProxy);

    Partition resultPart = client.getPartition(DB_NAME, TABLE_NAME, Lists.newArrayList("2007"));
    Assert.assertEquals(rootPath + "part2007", resultPart.getSd().getLocation());
  }

  @Test
  public void testAddPartitionSpecChangeRootPath() throws Exception {

    Table table = createTable();
    String rootPath = table.getSd().getLocation() + "/addPartSpecRootPath/";
    String rootPath1 = table.getSd().getLocation() + "/someotherpath/";

    Partition partition = buildPartition(DB_NAME, TABLE_NAME, "2007", rootPath + "part2007/");
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpec(DB_NAME, TABLE_NAME, rootPath, Lists.newArrayList(partition));
    partitionSpecProxy.setRootLocation(rootPath1);
    client.add_partitions_pspec(partitionSpecProxy);

    Partition resultPart = client.getPartition(DB_NAME, TABLE_NAME, Lists.newArrayList("2007"));
    Assert.assertEquals(rootPath1 + "part2007", resultPart.getSd().getLocation());
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionSpecChangeRootPathFromNull() throws Exception {

    Table table = createTable();
    String rootPath = table.getSd().getLocation() + "/addPartSpecRootPath/";
    String rootPath1 = table.getSd().getLocation() + "/someotherpath/";

    Partition partition = buildPartition(DB_NAME, TABLE_NAME, "2007", rootPath + "part2007/");
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpec(DB_NAME, TABLE_NAME, null, Lists.newArrayList(partition));
    partitionSpecProxy.setRootLocation(rootPath1);
    client.add_partitions_pspec(partitionSpecProxy);
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionSpecChangeRootPathToNull() throws Exception {

    Table table = createTable();
    String rootPath = table.getSd().getLocation() + "/addPartSpecRootPath/";
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, "2007", rootPath + "part2007/");
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpec(DB_NAME, TABLE_NAME, rootPath, Lists.newArrayList(partition));
    partitionSpecProxy.setRootLocation(null);
    client.add_partitions_pspec(partitionSpecProxy);
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionSpecChangeRootPathDiffInSd() throws Exception {

    Table table = createTable();
    String rootPath = table.getSd().getLocation() + "/addPartSpecRootPath/";
    String rootPath1 = table.getSd().getLocation() + "/addPartSdPath/";
    String rootPath2 = table.getSd().getLocation() + "/someotherpath/";
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, "2007", rootPath1 + "part2007/");
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpec(DB_NAME, TABLE_NAME, rootPath, Lists.newArrayList(partition));
    partitionSpecProxy.setRootLocation(rootPath2);
    client.add_partitions_pspec(partitionSpecProxy);
  }

  @Test
  public void testAddPartitionSpecWithSharedSDChangeRootPath() throws Exception {

    Table table = createTable();
    String rootPath = table.getSd().getLocation() + "/addPartSpecRootPath/";
    String rootPath1 = table.getSd().getLocation() + "/someotherpath/";

    PartitionWithoutSD partition = buildPartitionWithoutSD(Lists.newArrayList("2014"), 0);
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpecWithSharedSD(Lists.newArrayList(partition), buildSD(rootPath));
    partitionSpecProxy.setRootLocation(rootPath1);
    client.add_partitions_pspec(partitionSpecProxy);

    Partition resultPart = client.getPartition(DB_NAME, TABLE_NAME, Lists.newArrayList("2014"));
    Assert.assertEquals(rootPath1 + "partwithoutsd0", resultPart.getSd().getLocation());
  }

  @Test
  public void testAddPartitionSpecWithSharedSDWithoutRelativePath() throws Exception {

    Table table = createTable();
    PartitionWithoutSD partition = buildPartitionWithoutSD(Lists.newArrayList("2014"), 0);
    partition.setRelativePath(null);
    String location = table.getSd().getLocation() + "/sharedSDTest/";
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpecWithSharedSD(Lists.newArrayList(partition), buildSD(location));
    client.add_partitions_pspec(partitionSpecProxy);

    Partition part = client.getPartition(DB_NAME, TABLE_NAME, "year=2014");
    Assert.assertNotNull(part);
    Assert.assertEquals(table.getSd().getLocation() + "/sharedSDTest/null",
        part.getSd().getLocation());
    Assert.assertTrue(metaStore.isPathExists(new Path(part.getSd().getLocation())));
  }

  @Test(expected = AlreadyExistsException.class)
  public void testAddPartitionSpecPartAlreadyExists() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    client.add_partition(partition);

    Partition newPartition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpec(DB_NAME, TABLE_NAME, null, Lists.newArrayList(newPartition));
    client.add_partitions_pspec(partitionSpecProxy);
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionSpecPartDuplicateInSpec() throws Exception {

    createTable();
    Partition partition1 = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    Partition partition2 = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    List<Partition> partitions = new ArrayList<>();
    partitions.add(partition1);
    partitions.add(partition2);
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpec(DB_NAME, TABLE_NAME, null, partitions);
    client.add_partitions_pspec(partitionSpecProxy);
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionSpecPartDuplicateInSpecs() throws Exception {

    createTable(DB_NAME, TABLE_NAME, getYearPartCol(),
        metaStore.getWarehouseRoot() + "/" + TABLE_NAME);

    Partition partition = buildPartition(Lists.newArrayList("2002"), getYearPartCol(), 1);
    PartitionWithoutSD partitionWithoutSD = buildPartitionWithoutSD(Lists.newArrayList("2002"), 0);
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpec(Lists.newArrayList(partition), Lists.newArrayList(partitionWithoutSD));
    client.add_partitions_pspec(partitionSpecProxy);
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionSpecNullSd() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    partition.setSd(null);
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpec(DB_NAME, TABLE_NAME, null, Lists.newArrayList(partition));
    client.add_partitions_pspec(partitionSpecProxy);
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionSpecWithSharedSDNullSd() throws Exception {

    createTable();
    PartitionWithoutSD partition = buildPartitionWithoutSD(Lists.newArrayList("2002"), 0);
    StorageDescriptor sd = null;
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpecWithSharedSD(Lists.newArrayList(partition), sd);
    client.add_partitions_pspec(partitionSpecProxy);
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionSpecWithSharedSDNullLocation() throws Exception {

    createTable();
    PartitionWithoutSD partition = buildPartitionWithoutSD(Lists.newArrayList("2002"), 0);
    partition.setRelativePath("year2002");
    String location = null;
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpecWithSharedSD(Lists.newArrayList(partition), buildSD(location));
    client.add_partitions_pspec(partitionSpecProxy);
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionSpecWithSharedSDEmptyLocation() throws Exception {

    createTable();
    PartitionWithoutSD partition = buildPartitionWithoutSD(Lists.newArrayList("2002"), 0);
    partition.setRelativePath("year2002");
    PartitionSpecProxy partitionSpecProxy = buildPartitionSpecWithSharedSD(Lists.newArrayList(partition), buildSD(""));
    client.add_partitions_pspec(partitionSpecProxy);
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionSpecWithSharedSDInvalidSD() throws Exception {

    Table table = createTable();
    PartitionWithoutSD partition = buildPartitionWithoutSD(Lists.newArrayList("2002"), 0);
    partition.setRelativePath("year2002");
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(table.getSd().getLocation() + "/nullLocationTest/");
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpecWithSharedSD(Lists.newArrayList(partition), sd);
    client.add_partitions_pspec(partitionSpecProxy);
  }

  @Test
  public void testAddPartitionSpecNullLocation() throws Exception {

    Table table = createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE, null);
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpec(DB_NAME, TABLE_NAME, null, Lists.newArrayList(partition));
    client.add_partitions_pspec(partitionSpecProxy);

    Partition resultPart =
        client.getPartition(DB_NAME, TABLE_NAME, Lists.newArrayList(DEFAULT_YEAR_VALUE));
    Assert.assertEquals(table.getSd().getLocation() + "/year=2017",
        resultPart.getSd().getLocation());
    Assert.assertTrue(metaStore.isPathExists(new Path(resultPart.getSd().getLocation())));
  }

  @Test
  public void testAddPartitionSpecEmptyLocation() throws Exception {

    Table table = createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE, "");
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpec(DB_NAME, TABLE_NAME, null, Lists.newArrayList(partition));
    client.add_partitions_pspec(partitionSpecProxy);

    Partition resultPart =
        client.getPartition(DB_NAME, TABLE_NAME, Lists.newArrayList(DEFAULT_YEAR_VALUE));
    Assert.assertEquals(table.getSd().getLocation() + "/year=2017",
        resultPart.getSd().getLocation());
    Assert.assertTrue(metaStore.isPathExists(new Path(resultPart.getSd().getLocation())));
  }

  @Test
  public void testAddPartitionSpecEmptyLocationInTableToo() throws Exception {

    Table table = createTable(DB_NAME, TABLE_NAME, getYearPartCol(), null);
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE, "");
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpec(DB_NAME, TABLE_NAME, null, Lists.newArrayList(partition));
    client.add_partitions_pspec(partitionSpecProxy);

    Partition resultPart =
        client.getPartition(DB_NAME, TABLE_NAME, Lists.newArrayList(DEFAULT_YEAR_VALUE));
    Assert.assertEquals(table.getSd().getLocation() + "/year=2017",
        resultPart.getSd().getLocation());
    Assert.assertTrue(metaStore.isPathExists(new Path(resultPart.getSd().getLocation())));
  }

  @Test(expected=MetaException.class)
  public void testAddPartitionSpecForView() throws Exception {

    Table table = new TableBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .setType("VIRTUAL_VIEW")
        .addCol("test_id", "int", "test col id")
        .addCol("test_value", DEFAULT_COL_TYPE, "test col value")
        .addPartCol(YEAR_COL_NAME, DEFAULT_COL_TYPE)
        .setLocation(null)
        .create(client, metaStore.getConf());

    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE);
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpec(DB_NAME, TABLE_NAME, null, Lists.newArrayList(partition));
    client.add_partitions_pspec(partitionSpecProxy);
  }

  @Test(expected=MetaException.class)
  public void testAddPartitionSpecWithSharedSDNoValue() throws Exception {

    Table table = createTable();
    PartitionWithoutSD partition = new PartitionWithoutSD();
    partition.setRelativePath("addpartspectest");
    String location = table.getSd().getLocation() + "/nullValueTest/";
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpecWithSharedSD(Lists.newArrayList(partition), buildSD(location));
    client.add_partitions_pspec(partitionSpecProxy);
  }

  @Test(expected=MetaException.class)
  public void testAddPartitionSpecNoValue() throws Exception {

    createTable();
    Partition partition = new PartitionBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .addCol(YEAR_COL_NAME, DEFAULT_COL_TYPE)
        .setLocation(metaStore.getWarehouseRoot() + "/addpartspectest")
        .build(metaStore.getConf());

    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpec(DB_NAME, TABLE_NAME, null, Lists.newArrayList(partition));
    client.add_partitions_pspec(partitionSpecProxy);
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionSpecNullValues() throws Exception {

    createTable();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, null);
    partition.setValues(null);
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpec(DB_NAME, TABLE_NAME, null, Lists.newArrayList(partition));
    client.add_partitions_pspec(partitionSpecProxy);
  }

  @Test
  public void testAddPartitionSpecWithSharedSDEmptyValue() throws Exception {

    Table table = createTable();
    PartitionWithoutSD partition = new PartitionWithoutSD();
    partition.setRelativePath("addpartspectest");
    partition.setValues(Lists.newArrayList(""));
    String location = table.getSd().getLocation() + "/nullValueTest/";
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpecWithSharedSD(Lists.newArrayList(partition), buildSD(location));
    client.add_partitions_pspec(partitionSpecProxy);

    List<String> partitionNames = client.listPartitionNames(DB_NAME, TABLE_NAME, MAX);
    Assert.assertNotNull(partitionNames);
    Assert.assertTrue(partitionNames.size() == 1);
    Assert.assertEquals("year=__HIVE_DEFAULT_PARTITION__", partitionNames.get(0));
  }

  @Test(expected = MetaException.class)
  public void testAddPartitionSpecMoreValues() throws Exception {

    createTable();
    Partition partition =
        buildPartition(Lists.newArrayList("2017", "march"), getYearAndMonthPartCols(), 1);
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpec(DB_NAME, TABLE_NAME, null, Lists.newArrayList(partition));
    client.add_partitions_pspec(partitionSpecProxy);
  }

  @Test
  public void testAddPartitionSpecWithSharedSDNoRelativePath() throws Exception {

    Table table = createTable();
    PartitionWithoutSD partition1 = buildPartitionWithoutSD(Lists.newArrayList("2007"), 0);
    PartitionWithoutSD partition2 = buildPartitionWithoutSD(Lists.newArrayList("2008"), 0);
    partition1.setRelativePath(null);
    partition2.setRelativePath(null);
    String location = table.getSd().getLocation() + "/noRelativePath/";
    PartitionSpecProxy partitionSpecProxy = buildPartitionSpecWithSharedSD(
        Lists.newArrayList(partition1, partition2), buildSD(location));
    client.add_partitions_pspec(partitionSpecProxy);

    Partition resultPart1 = client.getPartition(DB_NAME, TABLE_NAME, Lists.newArrayList("2007"));
    Assert.assertEquals(location + "null", resultPart1.getSd().getLocation());
    Assert.assertTrue(metaStore.isPathExists(new Path(resultPart1.getSd().getLocation())));

    Partition resultPart2 = client.getPartition(DB_NAME, TABLE_NAME, Lists.newArrayList("2008"));
    Assert.assertEquals(location + "null", resultPart2.getSd().getLocation());
    Assert.assertTrue(metaStore.isPathExists(new Path(resultPart2.getSd().getLocation())));
  }

  @Test
  public void testAddPartitionSpecOneInvalid() throws Exception {

    createTable();
    Partition partition1 = buildPartition(DB_NAME, TABLE_NAME, "2016");
    Partition partition2 = buildPartition(DB_NAME, TABLE_NAME, "2017");
    Partition partition3 =
        buildPartition(Lists.newArrayList("2015", "march"), getYearAndMonthPartCols(), 1);
    partition3.getSd().setLocation(metaStore.getWarehouseRoot() + "/addparttest");
    List<Partition> partitions = Lists.newArrayList(partition1, partition2, partition3);
    PartitionSpecProxy partitionSpecProxy =
        buildPartitionSpec(DB_NAME, TABLE_NAME, null, partitions);
    try {
      client.add_partitions_pspec(partitionSpecProxy);
      Assert.fail("MetaException should have occurred.");
    } catch (MetaException e) {
      // This is expected
    }

    List<Partition> parts = client.listPartitions(DB_NAME, TABLE_NAME, MAX);
    Assert.assertNotNull(parts);
    Assert.assertTrue(parts.isEmpty());
    // TODO: This does not work correctly. None of the partitions is created, but the folder
    // for the first two is created. It is because in HiveMetaStore.add_partitions_core when
    // going through the partitions, the first two are already put and started in the thread
    // pool when the exception occurs in the third one.
    // When the exception occurs, we go to the finally part, but the map can be empty
    // (it depends on the progress of the other threads) so the folders won't be deleted.
    // Assert.assertTrue(metaStore.isPathExists(new Path(partition1.getSd().getLocation())));
    // Assert.assertTrue(metaStore.isPathExists(new Path(partition2.getSd().getLocation())));
    // Assert.assertTrue(metaStore.isPathExists(new Path(partition3.getSd().getLocation())));
  }

  // Helper methods
  private void createDB(String dbName) throws TException {
    new DatabaseBuilder().setName(dbName).create(client, metaStore.getConf());
  }

  private Table createTable() throws Exception {
    return createTable(DB_NAME, TABLE_NAME, getYearPartCol(),
        metaStore.getWarehouseRoot() + "/" + TABLE_NAME);
  }

  private Table createTable(String dbName, String tableName, List<FieldSchema> partCols,
      String location) throws Exception {
    new TableBuilder()
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
        .create(client, metaStore.getConf());
    return client.getTable(dbName, tableName);
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
        .build(metaStore.getConf());
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
        .setCreateTime(DEFAULT_CREATE_TIME)
        .setLastAccessTime(DEFAULT_CREATE_TIME)
        .addCol("test_id", "int", "test col id")
        .addCol("test_value", "string", "test col value")
        .build(metaStore.getConf());
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
    Assert.assertEquals("The last access time is not correct.", DEFAULT_CREATE_TIME,
        part.getLastAccessTime());
    Assert.assertNotEquals(DEFAULT_CREATE_TIME, part.getCreateTime());
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

  private PartitionWithoutSD buildPartitionWithoutSD(List<String> values, int index)
      throws MetaException {
    PartitionWithoutSD partition = new PartitionWithoutSD();
    partition.setCreateTime(DEFAULT_CREATE_TIME);
    partition.setLastAccessTime(DEFAULT_CREATE_TIME);
    partition.setValues(values);
    Map<String, String> parameters = new HashMap<>();
    parameters.put(DEFAULT_PARAM_KEY + index, DEFAULT_PARAM_VALUE + index);
    partition.setParameters(parameters);
    partition.setRelativePath("partwithoutsd" + index);
    return partition;
  }

  private PartitionSpecProxy buildPartitionSpec(String dbName, String tableName, String rootPath,
      List<Partition> partitions) throws MetaException {

    PartitionSpec partitionSpec = new PartitionSpec();
    partitionSpec.setDbName(dbName);
    partitionSpec.setRootPath(rootPath);
    partitionSpec.setTableName(tableName);

    PartitionListComposingSpec partitionListComposingSpec = new PartitionListComposingSpec();
    partitionListComposingSpec.setPartitions(partitions);
    partitionSpec.setPartitionList(partitionListComposingSpec);

    return PartitionSpecProxy.Factory.get(partitionSpec);
  }

  private StorageDescriptor buildSD(String location) {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setInputFormat("TestInputFormat");
    sd.setOutputFormat("TestOutputFormat");
    sd.setCols(getYearPartCol());
    sd.setCompressed(false);
    Map<String, String> parameters = new HashMap<>();
    parameters.put("testSDParamKey", "testSDParamValue");
    sd.setParameters(parameters);
    sd.setLocation(location);
    SerDeInfo serdeInfo = new SerDeInfo();
    serdeInfo.setName("sharedSDPartSerde");
    sd.setSerdeInfo(serdeInfo);
    return sd;
  }

  private PartitionSpecProxy buildPartitionSpecWithSharedSD(List<PartitionWithoutSD> partitions,
      StorageDescriptor sd) throws MetaException {

    PartitionSpec partitionSpec = new PartitionSpec();
    partitionSpec.setDbName(DB_NAME);
    partitionSpec.setTableName(TABLE_NAME);
    PartitionSpecWithSharedSD partitionList = new PartitionSpecWithSharedSD();
    partitionList.setPartitions(partitions);
    partitionList.setSd(sd);
    partitionSpec.setSharedSDPartitionSpec(partitionList);
    return PartitionSpecProxy.Factory.get(partitionSpec);
  }

  private PartitionSpecProxy buildPartitionSpec(List<Partition> partitions,
      List<PartitionWithoutSD> partitionsWithoutSD) throws MetaException {

    List<PartitionSpec> partitionSpecs = new ArrayList<>();
    PartitionSpec partitionSpec = new PartitionSpec();
    partitionSpec.setDbName(DB_NAME);
    partitionSpec.setTableName(TABLE_NAME);
    PartitionListComposingSpec partitionListComposingSpec = new PartitionListComposingSpec();
    partitionListComposingSpec.setPartitions(partitions);
    partitionSpec.setPartitionList(partitionListComposingSpec);

    PartitionSpec partitionSpecSharedSD = new PartitionSpec();
    partitionSpecSharedSD.setDbName(DB_NAME);
    partitionSpecSharedSD.setTableName(TABLE_NAME);
    PartitionSpecWithSharedSD partitionSpecWithSharedSD = new PartitionSpecWithSharedSD();
    partitionSpecWithSharedSD.setPartitions(partitionsWithoutSD);
    partitionSpecWithSharedSD
        .setSd(buildSD(metaStore.getWarehouseRoot() + "/" + TABLE_NAME + "/sharedSDTest/"));
    partitionSpecSharedSD.setSharedSDPartitionSpec(partitionSpecWithSharedSD);

    partitionSpecs.add(partitionSpec);
    partitionSpecs.add(partitionSpecSharedSD);
    return PartitionSpecProxy.Factory.get(partitionSpecs);
  }

  private void verifyPartitionSharedSD(Table table, String name, List<String> values, int index)
      throws Exception {

    Partition part = client.getPartition(table.getDbName(), table.getTableName(), name);
    Assert.assertNotNull(part);
    Assert.assertEquals(table.getTableName(), part.getTableName());
    List<String> partValues = part.getValues();
    Assert.assertEquals(values.size(), partValues.size());
    Assert.assertTrue(partValues.containsAll(values));
    Assert.assertEquals(table.getDbName(), part.getDbName());
    Assert.assertEquals(DEFAULT_CREATE_TIME, part.getLastAccessTime());
    Assert.assertEquals(DEFAULT_PARAM_VALUE + index,
        part.getParameters().get(DEFAULT_PARAM_KEY + index));
    Assert.assertFalse(part.getParameters().keySet().contains(table.getParameters().keySet()));
    StorageDescriptor sd = part.getSd();
    Assert.assertNotNull(sd);
    Assert.assertEquals("TestInputFormat", sd.getInputFormat());
    Assert.assertEquals("TestOutputFormat", sd.getOutputFormat());
    Assert.assertEquals("sharedSDPartSerde", sd.getSerdeInfo().getName());
    Assert.assertEquals("testSDParamValue", sd.getParameters().get("testSDParamKey"));
    Assert.assertEquals(
        metaStore.getWarehouseRoot() + "/" + TABLE_NAME + "/sharedSDTest/partwithoutsd" + index,
        sd.getLocation());
    Assert.assertTrue(metaStore.isPathExists(new Path(sd.getLocation())));
  }
}
