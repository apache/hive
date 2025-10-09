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
package org.apache.hadoop.hive.ql.metadata;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.client.CustomIgnoreRule;
import org.apache.hadoop.hive.metastore.client.TestAddPartitions;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test class for adding partitions related methods on temporary tables.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestSessionHiveMetastoreClientAddPartitionsTempTable
    extends TestAddPartitions {

  private HiveConf conf;

  public TestSessionHiveMetastoreClientAddPartitionsTempTable(String name, AbstractMetaStoreService metaStore) {
    super(name, metaStore);
    ignoreRule = new CustomIgnoreRule();
  }

  @Before
  public void setUp() throws Exception {
    initHiveConf();
    SessionState.start(conf);
    setClient(Hive.get(conf).getMSC());
    getClient().dropDatabase(DB_NAME, true, true, true);
    getMetaStore().cleanWarehouseDirs();
    new DatabaseBuilder().
        setName(DB_NAME).
        create(getClient(), conf);
  }

  private void initHiveConf() throws HiveException {
    conf = new HiveConfForTest(Hive.get().getConf(), getClass());
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_FASTPATH, true);
  }

  @Override
  protected Table createTable(String dbName, String tableName, List<FieldSchema> partCols, String location)
      throws Exception {
    new TableBuilder().setDbName(dbName).setTableName(tableName).addCol("test_id", "int", "test col id")
        .addCol("test_value", "string", "test col value")
        .addTableParam("partTestTableParamKey", "partTestTableParamValue").setPartCols(partCols)
        .addStorageDescriptorParam("partTestSDParamKey", "partTestSDParamValue").setSerdeName(tableName)
        .setStoredAsSubDirectories(false).addSerdeParam("partTestSerdeParamKey", "partTestSerdeParamValue")
        .setLocation(location).setTemporary(true).create(getClient(), conf);
    return getClient().getTable(dbName, tableName);
  }

  protected void createExternalTable(String tableName, String location) throws Exception {
    new TableBuilder().setDbName(DB_NAME).setTableName(tableName).addCol("test_id", "int", "test col id")
        .addCol("test_value", DEFAULT_COL_TYPE, "test col value").addPartCol(YEAR_COL_NAME, DEFAULT_COL_TYPE)
        .addTableParam("EXTERNAL", "TRUE").setLocation(location).setTemporary(true).create(getClient(), conf);
  }

  @Override
  protected void verifyPartition(Table table, String name, List<String> values, int index) throws Exception {

    Partition part = getClient().getPartition(table.getDbName(), table.getTableName(), name);
    Assert.assertNotNull("The partition should not be null.", part);
    assertEquals("The table name in the partition is not correct.", table.getTableName(), part.getTableName());
    List<String> partValues = part.getValues();
    assertEquals(values.size(), partValues.size());
    Assert.assertTrue("The partition has wrong values.", partValues.containsAll(values));
    assertEquals("The DB name in the partition is not correct.", table.getDbName(), part.getDbName());
    assertEquals("The last access time is not correct.", 123456, part.getLastAccessTime());
    assertEquals("The partition's parameter map should contain the partparamkey - partparamvalue pair.",
        DEFAULT_PARAM_VALUE + index, part.getParameters().get(DEFAULT_PARAM_KEY + index));
    StorageDescriptor sd = part.getSd();
    Assert.assertNotNull("The partition's storage descriptor must not be null.", sd);
    assertEquals("The input format is not correct.", "TestInputFormat" + index, sd.getInputFormat());
    assertEquals("The output format is not correct.", "TestOutputFormat" + index, sd.getOutputFormat());
    assertEquals("The serdeInfo name is not correct.", "partserde" + index, sd.getSerdeInfo().getName());
    assertEquals(
        "The parameter map of the partition's storage descriptor should contain the partsdkey - partsdvalue pair.",
        "partsdvalue" + index, sd.getParameters().get("partsdkey" + index));
    assertEquals("The parameter's location is not correct.",
        getMetaStore().getWarehouseRoot() + "/" + TABLE_NAME + "/" + name, sd.getLocation());
    Assert.assertTrue("The parameter's location should exist on the file system.",
        getMetaStore().isPathExists(new Path(sd.getLocation())));
    Assert.assertFalse("The partition should not inherit the table parameters.",
        part.getParameters().keySet().contains(table.getParameters().keySet()));
  }

  @Override
  protected void verifyPartitionAttributesDefaultValues(Partition partition, String tableLocation) {
    Assert.assertNotEquals("The partition's last access time should be set.", 0, partition.getLastAccessTime());
    Assert.assertNotEquals("The partition's create time should be set.", 0, partition.getCreateTime());
    StorageDescriptor sd = partition.getSd();
    Assert.assertNotNull("The storage descriptor of the partition must not be null.", sd);
    assertEquals("The partition location is not correct.", tableLocation + "/year=2017", sd.getLocation());
    assertEquals("The input format doesn't have the default value.", "org.apache.hadoop.hive.ql.io.HiveInputFormat",
        sd.getInputFormat());
    assertEquals("The output format doesn't have the default value.", "org.apache.hadoop.hive.ql.io.HiveOutputFormat",
        sd.getOutputFormat());
    Assert.assertFalse("The compressed attribute doesn't have the default value.", sd.isCompressed());
    Assert.assertFalse("The storedAsSubDirectories attribute doesn't have the default value.",
        sd.isStoredAsSubDirectories());
    assertEquals("The numBuckets attribute doesn't have the default value.", 0, sd.getNumBuckets());
    Assert.assertTrue("The default value of the attribute 'bucketCols' should be an empty list.",
        sd.getBucketCols().isEmpty());
    Assert.assertTrue("The default value of the attribute 'sortCols' should be an empty list.",
        sd.getSortCols().isEmpty());
    Assert.assertTrue("Per default the storage descriptor parameters should be empty.", sd.getParameters().isEmpty());
    SerDeInfo serdeInfo = sd.getSerdeInfo();
    Assert.assertNotNull("The serdeInfo attribute should not be null.", serdeInfo);
    Assert.assertNull("The default value of the serde's name attribute should be null.", serdeInfo.getName());
    assertEquals("The serde's 'serializationLib' attribute doesn't have the default value.",
        "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", serdeInfo.getSerializationLib());
    Assert.assertTrue("Per default the serde info parameters should be empty.", serdeInfo.getParameters().isEmpty());
    SkewedInfo skewedInfo = sd.getSkewedInfo();
    Assert.assertTrue("Per default the skewedInfo column names list should be empty.",
        skewedInfo.getSkewedColNames().isEmpty());
    Assert.assertTrue("Per default the skewedInfo column value list should be empty.",
        skewedInfo.getSkewedColValues().isEmpty());
    Assert.assertTrue("Per default the skewedInfo column value location map should be empty.",
        skewedInfo.getSkewedColValueLocationMaps().isEmpty());
  }

  @Test
  @Override
  public void testAddPartitionNullLocationInTableToo() throws Exception {
    createTable(DB_NAME, TABLE_NAME, null);
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE, null);
    getClient().add_partition(partition);
    Partition part = getClient().getPartition(DB_NAME, TABLE_NAME, "year=2017");
    Assert.assertTrue(getMetaStore().isPathExists(new Path(part.getSd().getLocation())));
    assertEquals(SessionState.get().getTempTables().get(DB_NAME).get(TABLE_NAME).getSd().getLocation() + "/year=2017",
        part.getSd().getLocation());
  }

  @Test
  @Override
  public void testAddPartitionForExternalTableNullLocation() throws Exception {
    String tableName = "part_add_ext_table";
    createExternalTable(tableName, null);
    Partition partition = buildPartition(DB_NAME, tableName, DEFAULT_YEAR_VALUE, null);
    getClient().add_partition(partition);
    Partition resultPart = getClient().getPartition(DB_NAME, tableName, Lists.newArrayList(DEFAULT_YEAR_VALUE));
    Assert.assertNotNull(resultPart);
    Assert.assertNotNull(resultPart.getSd());
    assertEquals(SessionState.get().getTempTables().get(DB_NAME).get(tableName).getSd().getLocation() + "/year=2017",
        resultPart.getSd().getLocation());
  }

  @Test
  @Override
  public void testAddPartitionsNullLocationInTableToo() throws Exception {
    createTable(DB_NAME, TABLE_NAME, null);
    List<Partition> partitions = new ArrayList<>();
    Partition partition = buildPartition(DB_NAME, TABLE_NAME, DEFAULT_YEAR_VALUE, null);
    partitions.add(partition);
    getClient().add_partitions(partitions);

    Partition part = getClient().getPartition(DB_NAME, TABLE_NAME, "year=2017");
    assertEquals(SessionState.get().getTempTables().get(DB_NAME).get(TABLE_NAME).getSd().getLocation() + "/year=2017",
        part.getSd().getLocation());
    Assert.assertTrue(getMetaStore().isPathExists(new Path(part.getSd().getLocation())));
  }

  @Test
  @Override
  public void testAddPartitionsForExternalTableNullLocation() throws Exception {
    String tableName = "part_add_ext_table";
    createExternalTable(tableName, null);
    Partition partition1 = buildPartition(DB_NAME, tableName, "2017", null);
    Partition partition2 = buildPartition(DB_NAME, tableName, "2018", null);
    List<Partition> partitions = Lists.newArrayList(partition1, partition2);
    getClient().add_partitions(partitions);

    List<Partition> resultParts =
        getClient().getPartitionsByNames(DB_NAME, tableName, Lists.newArrayList("year=2017", "year=2018"));
    Assert.assertNotNull(resultParts);
    Assert.assertEquals(2, resultParts.size());
    String defaultTableLocation = SessionState.get().getTempTables().get(DB_NAME).get(tableName).getSd().getLocation();
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
}
