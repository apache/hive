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
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.TestExchangePartitions;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test class for exchange partitions related methods on temporary tables.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestSessionHiveMetastoreClientExchangePartitionsTempTable extends TestExchangePartitions {

  private HiveConf conf;
  private Warehouse wh;

  public TestSessionHiveMetastoreClientExchangePartitionsTempTable(String name, AbstractMetaStoreService metaStore) {
    super(name, metaStore);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    initHiveConf();
    wh = new Warehouse(conf);
    SessionState.start(conf);
    setClient(Hive.get(conf).getMSC());
    getClient().dropDatabase(DB_NAME, true, true, true);
    getMetaStore().cleanWarehouseDirs();
    createTestTables();
  }

  private void initHiveConf() throws Exception{
    conf = new HiveConfForTest(Hive.get().getConf(), getClass());
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_FASTPATH, true);
  }

  @Override
  protected Table createTable(String dbName, String tableName, List<FieldSchema> partCols,
      List<FieldSchema> cols, String location) throws Exception {
    new TableBuilder()
        .setDbName(dbName)
        .setTableName(tableName)
        .setCols(cols)
        .setPartCols(partCols)
        .setLocation(location)
        .setTemporary(true)
        .create(getClient(), getMetaStore().getConf());
    return getClient().getTable(dbName, tableName);
  }

  @Override
  @Test(expected = MetaException.class)
  public void testExchangePartitionsNonExistingPartLocation() throws Exception {
    Map<String, String> partitionSpecs = getPartitionSpec(getPartitions()[1]);
    getMetaStore().cleanWarehouseDirs();
    cleanTempTableDir(getSourceTable());
    cleanTempTableDir(getDestTable());
    getClient().exchange_partitions(partitionSpecs, getSourceTable().getDbName(),
        getSourceTable().getTableName(), getDestTable().getDbName(), getDestTable().getTableName());
  }

  @Override
  @Test
  public void testExchangePartitionsCustomTableAndPartLocation() throws Exception {
    Table source = createTable(DB_NAME, "test_source_table_cust_loc",
        getYearMonthAndDayPartCols(), getMetaStore().getWarehouseRoot() + "/sourceTable");
    Table dest = createTable(DB_NAME, "test_dest_table_cust_loc", getYearMonthAndDayPartCols(),
        getMetaStore().getWarehouseRoot() + "/destTable");
    org.apache.hadoop.hive.metastore.api.Partition[] parts = new Partition[2];
    parts[0] = createPartition(source, Lists.newArrayList("2019", "may", "11"),
        source.getSd().getLocation() + "/2019m11");
    parts[1] = createPartition(source, Lists.newArrayList("2019", "july", "23"),
        source.getSd().getLocation() + "/2019j23");

    Map<String, String> partitionSpecs = getPartitionSpec(parts[1]);
    getClient().exchange_partitions(partitionSpecs, source.getDbName(),
        source.getTableName(), dest.getDbName(), dest.getTableName());

    checkRemainingPartitions(source, dest, Lists.newArrayList(parts[0]));
    List<Partition> destTablePartitions = getClient().listPartitions(dest.getDbName(), dest.getTableName(), (short) -1);
    Assert.assertEquals(1, destTablePartitions.size());
    checkExchangedPartitions(source, dest, Lists.newArrayList(parts[1]));
  }

  @Override
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
    getClient().exchange_partitions(partitionSpecs, source.getDbName(),
        source.getTableName(), dest.getDbName(), dest.getTableName());

    checkRemainingPartitions(source, dest, Lists.newArrayList(parts[0]));
    List<Partition> destTablePartitions = getClient().listPartitions(dest.getDbName(), dest.getTableName(), (short) -1);
    Assert.assertEquals(1, destTablePartitions.size());
    checkExchangedPartitions(source, dest, Lists.newArrayList(parts[1]));
  }

  @Override
  @Test
  public void testExchangePartitionsOnlyMonthSetInPartSpec() throws Exception {
    Map<String, String> partitionSpecs = new HashMap<>();
    partitionSpecs.put(YEAR_COL_NAME, "");
    partitionSpecs.put(MONTH_COL_NAME, "march");
    partitionSpecs.put(DAY_COL_NAME, "");

    getClient().exchange_partitions(partitionSpecs, getSourceTable().getDbName(),
        getSourceTable().getTableName(), getDestTable().getDbName(), getDestTable().getTableName());
    checkRemainingPartitions(getSourceTable(), getDestTable(), Lists.newArrayList(getPartitions()[2],
        getPartitions()[3], getPartitions()[4]));
    List<Partition> exchangedPartitions = getClient().listPartitions(getDestTable().getDbName(),
        getDestTable().getTableName(), MAX);
    Assert.assertEquals(2, exchangedPartitions.size());
    checkExchangedPartitions(getSourceTable(), getDestTable(), Lists.newArrayList(getPartitions()[0],
        getPartitions()[1]));
  }

  @Override
  @Test
  public void testExchangePartitionsYearAndDaySetInPartSpec() throws Exception {
    Map<String, String> partitionSpecs = new HashMap<>();
    partitionSpecs.put(YEAR_COL_NAME, "2017");
    partitionSpecs.put(MONTH_COL_NAME, "");
    partitionSpecs.put(DAY_COL_NAME, "22");
    getClient().exchange_partitions(partitionSpecs, getSourceTable().getDbName(),
        getSourceTable().getTableName(), getDestTable().getDbName(), getDestTable().getTableName());
    checkRemainingPartitions(getSourceTable(), getDestTable(), Lists.newArrayList(getPartitions()[0],
        getPartitions()[2], getPartitions()[3], getPartitions()[4]));
    List<Partition> exchangedPartitions = getClient().listPartitions(getDestTable().getDbName(),
        getDestTable().getTableName(), MAX);
    Assert.assertEquals(1, exchangedPartitions.size());
    checkExchangedPartitions(getSourceTable(), getDestTable(), Lists.newArrayList(getPartitions()[1]));
  }

  @Override
  @Test
  public void testExchangePartition() throws Exception {
    Map<String, String> partitionSpecs = getPartitionSpec(getPartitions()[1]);
    Partition exchangedPartition =
        getClient().exchange_partition(partitionSpecs, getSourceTable().getDbName(),
            getSourceTable().getTableName(), getDestTable().getDbName(), getDestTable().getTableName());
    Assert.assertNotNull(exchangedPartition);
    checkExchangedPartitions(getSourceTable(), getDestTable(), Lists.newArrayList(getPartitions()[1]));
    checkRemainingPartitions(getSourceTable(), getDestTable(),
        Lists.newArrayList(getPartitions()[0], getPartitions()[2], getPartitions()[3], getPartitions()[4]));
  }

  @Override
  @Test
  public void testExchangePartitionYearSet() throws Exception {
    Map<String, String> partitionSpecs = getPartitionSpec(Lists.newArrayList("2017", "", ""));
    Partition exchangedPartition =
        getClient().exchange_partition(partitionSpecs, getSourceTable().getDbName(),
            getSourceTable().getTableName(), getDestTable().getDbName(), getDestTable().getTableName());
    Assert.assertNotNull(exchangedPartition);
    checkExchangedPartitions(getSourceTable(), getDestTable(),
        Lists.newArrayList(getPartitions()[0], getPartitions()[1], getPartitions()[2], getPartitions()[3]));
    checkRemainingPartitions(getSourceTable(), getDestTable(), Lists.newArrayList(getPartitions()[4]));
  }

  @Override
  @Test
  public void testExchangePartitionCustomTableAndPartLocation() throws Exception {
    Table source = createTable(DB_NAME, "test_source_table_cust_loc",
        getYearMonthAndDayPartCols(), getMetaStore().getWarehouseRoot() + "/sourceTable");
    Table dest = createTable(DB_NAME, "test_dest_table_cust_loc", getYearMonthAndDayPartCols(),
        getMetaStore().getWarehouseRoot() + "/destTable");
    Partition[] parts = new Partition[2];
    parts[0] = createPartition(source, Lists.newArrayList("2019", "may", "11"),
        source.getSd().getLocation() + "/2019m11");
    parts[1] = createPartition(source, Lists.newArrayList("2019", "july", "23"),
        source.getSd().getLocation() + "/2019j23");

    Map<String, String> partitionSpecs = getPartitionSpec(parts[1]);
    getClient().exchange_partition(partitionSpecs, source.getDbName(),
        source.getTableName(), dest.getDbName(), dest.getTableName());

    checkRemainingPartitions(source, dest, Lists.newArrayList(parts[0]));
    List<Partition> destTablePartitions =
        getClient().listPartitions(dest.getDbName(), dest.getTableName(), (short) -1);
    Assert.assertEquals(1, destTablePartitions.size());
    checkExchangedPartitions(source, dest, Lists.newArrayList(parts[1]));
  }

  @Override
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
    getClient().exchange_partition(partitionSpecs, source.getDbName(),
        source.getTableName(), dest.getDbName(), dest.getTableName());
    checkRemainingPartitions(source, dest, Lists.newArrayList(parts[0]));
    List<Partition> destTablePartitions =
        getClient().listPartitions(dest.getDbName(), dest.getTableName(), (short) -1);
    Assert.assertEquals(1, destTablePartitions.size());
    checkExchangedPartitions(source, dest, Lists.newArrayList(parts[1]));
  }

  @Override
  @Test(expected = MetaException.class)
  public void testExchangePartitionNonExistingPartLocation() throws Exception {
    Map<String, String> partitionSpecs = getPartitionSpec(getPartitions()[1]);
    cleanTempTableDir(getSourceTable());
    cleanTempTableDir(getDestTable());
    getClient().exchange_partition(partitionSpecs, getSourceTable().getDbName(),
        getSourceTable().getTableName(), getDestTable().getDbName(), getDestTable().getTableName());
  }

  @Override
  @Test
  public void testExchangePartitionOnlyMonthSetInPartSpec() throws Exception {
    Map<String, String> partitionSpecs = new HashMap<>();
    partitionSpecs.put(YEAR_COL_NAME, "");
    partitionSpecs.put(MONTH_COL_NAME, "march");
    partitionSpecs.put(DAY_COL_NAME, "");
    getClient().exchange_partitions(partitionSpecs, getSourceTable().getDbName(),
        getSourceTable().getTableName(), getDestTable().getDbName(), getDestTable().getTableName());
    checkRemainingPartitions(getSourceTable(), getDestTable(),
        Lists.newArrayList(getPartitions()[2], getPartitions()[3], getPartitions()[4]));
    List<Partition> exchangedPartitions = getClient().listPartitions(getDestTable().getDbName(),
        getDestTable().getTableName(), MAX);
    Assert.assertEquals(2, exchangedPartitions.size());
    checkExchangedPartitions(getSourceTable(), getDestTable(),
        Lists.newArrayList(getPartitions()[0], getPartitions()[1]));
  }

  @Override
  @Test
  public void testExchangePartitionYearAndDaySetInPartSpec() throws Exception {
    Map<String, String> partitionSpecs = new HashMap<>();
    partitionSpecs.put(YEAR_COL_NAME, "2017");
    partitionSpecs.put(MONTH_COL_NAME, "");
    partitionSpecs.put(DAY_COL_NAME, "22");
    getClient().exchange_partition(partitionSpecs, getSourceTable().getDbName(),
        getSourceTable().getTableName(), getDestTable().getDbName(), getDestTable().getTableName());
    checkRemainingPartitions(getSourceTable(), getDestTable(),
        Lists.newArrayList(getPartitions()[0], getPartitions()[2], getPartitions()[3], getPartitions()[4]));
    List<Partition> exchangedPartitions = getClient().listPartitions(getDestTable().getDbName(),
        getDestTable().getTableName(), MAX);
    Assert.assertEquals(1, exchangedPartitions.size());
    checkExchangedPartitions(getSourceTable(), getDestTable(), Lists.newArrayList(getPartitions()[1]));
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionBetweenTempAndNonTemp() throws Exception {
    Table nonTempTable = createNonTempTable(DB_NAME, "nonTempTable", getYearMonthAndDayPartCols(), null);
    Map<String, String> partitionSpecs = new HashMap<>();
    partitionSpecs.put(YEAR_COL_NAME, "2017");
    partitionSpecs.put(MONTH_COL_NAME, "march");
    partitionSpecs.put(DAY_COL_NAME, "22");
    getClient().exchange_partition(partitionSpecs, getSourceTable().getDbName(), getSourceTable().getTableName(),
        nonTempTable.getDbName(), nonTempTable.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionBetweenNonTempAndTemp() throws Exception {
    Table nonTempTable = createNonTempTable(DB_NAME, "nonTempTable", getYearMonthAndDayPartCols(), null);
    Map<String, String> partitionSpecs = new HashMap<>();
    partitionSpecs.put(YEAR_COL_NAME, "2017");
    partitionSpecs.put(MONTH_COL_NAME, "march");
    partitionSpecs.put(DAY_COL_NAME, "22");
    getClient().exchange_partition(partitionSpecs, nonTempTable.getDbName(), nonTempTable.getTableName(),
        getDestTable().getDbName(), getDestTable().getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionsBetweenTempAndNonTemp() throws Exception {
    Table nonTempTable = createNonTempTable(DB_NAME, "nonTempTable", getYearMonthAndDayPartCols(), null);
    Map<String, String> partitionSpecs = new HashMap<>();
    partitionSpecs.put(YEAR_COL_NAME, "2017");
    partitionSpecs.put(MONTH_COL_NAME, "");
    partitionSpecs.put(DAY_COL_NAME, "23");
    getClient().exchange_partitions(partitionSpecs, getSourceTable().getDbName(), getSourceTable().getTableName(),
        nonTempTable.getDbName(), nonTempTable.getTableName());
  }

  @Test(expected = MetaException.class)
  public void testExchangePartitionsBetweenNonTempAndTemp() throws Exception {
    Table nonTempTable = createNonTempTable(DB_NAME, "nonTempTable", getYearMonthAndDayPartCols(), null);
    Map<String, String> partitionSpecs = new HashMap<>();
    partitionSpecs.put(YEAR_COL_NAME, "2017");
    partitionSpecs.put(MONTH_COL_NAME, "");
    partitionSpecs.put(DAY_COL_NAME, "23");
    getClient().exchange_partitions(partitionSpecs, nonTempTable.getDbName(), nonTempTable.getTableName(),
        getDestTable().getDbName(), getDestTable().getTableName());
  }

  private Table createNonTempTable(String dbName, String tableName, List<FieldSchema> partCols,
      String location) throws Exception {
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("test_id", INT_COL_TYPE, "test col id"));
    cols.add(new FieldSchema("test_value", "string", "test col value"));
    new TableBuilder()
        .setDbName(dbName)
        .setTableName(tableName)
        .setCols(cols)
        .setPartCols(partCols)
        .setLocation(location)
        .setTemporary(false)
        .create(getClient(), getMetaStore().getConf());
    return getClient().getTable(dbName, tableName);
  }

  private void cleanTempTableDir(Table table) throws MetaException {
    wh.deleteDir(new Path(table.getSd().getLocation()), true, false, false);
  }

}
