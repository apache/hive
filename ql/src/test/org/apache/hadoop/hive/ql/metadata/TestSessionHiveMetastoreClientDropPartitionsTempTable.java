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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.CustomIgnoreRule;
import org.apache.hadoop.hive.metastore.client.TestDropPartitions;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.Map;

/**
 * Test class for delete partitions related methods on temporary tables.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestSessionHiveMetastoreClientDropPartitionsTempTable
    extends TestDropPartitions {

  private HiveConf conf;

  public TestSessionHiveMetastoreClientDropPartitionsTempTable(String name, AbstractMetaStoreService metaStore) {
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

    // Create test tables with 3 partitions
    createTable(TABLE_NAME, getYearAndMonthPartCols(), null);
    createPartitions();
  }

  private void initHiveConf() throws HiveException {
    conf = new HiveConfForTest(Hive.get().getConf(), getClass());
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_FASTPATH, true);
  }

  @Override
  protected Table createTable(String tableName, List<FieldSchema> partCols, Map<String, String> tableParams)
      throws Exception {
    TableBuilder builder =
        new TableBuilder().setDbName(DB_NAME).setTableName(tableName).addCol("test_id", "int", "test col id")
            .addCol("test_value", "string", "test col value").setPartCols(partCols)
            .setLocation(getMetaStore().getWarehouseRoot() + "/" + tableName).setTemporary(true);
    if (tableParams != null) {
      builder.setTableParams(tableParams);
    }
    return builder.create(getClient(), conf);
  }

  @Override
  protected Partition createPartition(List<String> values, List<FieldSchema> partCols) throws Exception {
    new PartitionBuilder().setDbName(DB_NAME).setTableName(TABLE_NAME).setValues(values).setCols(partCols)
        .addToTable(getClient(), conf);
    org.apache.hadoop.hive.metastore.api.Partition partition = getClient().getPartition(DB_NAME, TABLE_NAME, values);
    return partition;
  }

  @Override
  protected Partition createPartition(String tableName, String location, List<String> values,
      List<FieldSchema> partCols, Map<String, String> partParams) throws Exception {
    new PartitionBuilder().setDbName(DB_NAME).setTableName(tableName).setValues(values).setCols(partCols)
        .setLocation(location).setPartParams(partParams).addToTable(getClient(), conf);
    Partition partition = getClient().getPartition(DB_NAME, tableName, values);
    return partition;
  }

  @Override
  protected void checkPartitionsAfterDelete(String tableName, List<Partition> droppedPartitions,
      List<Partition> existingPartitions, boolean deleteData, boolean purge) throws Exception {

    List<Partition> partitions = getClient().listPartitions(DB_NAME, tableName, MAX);
    Assert.assertEquals(
        "The table " + tableName + " has " + partitions.size() + " partitions, but it should have " + existingPartitions
            .size(), existingPartitions.size(), partitions.size());
    for (Partition droppedPartition : droppedPartitions) {
      Assert.assertFalse(partitions.contains(droppedPartition));
      Path partitionPath = new Path(droppedPartition.getSd().getLocation());
      if (deleteData) {
        Assert.assertFalse("The location '" + partitionPath.toString() + "' should not exist.",
            getMetaStore().isPathExists(partitionPath));
      } else {
        Assert.assertTrue("The location '" + partitionPath.toString() + "' should exist.",
            getMetaStore().isPathExists(partitionPath));
      }
    }

    for (Partition existingPartition : existingPartitions) {
      Assert.assertTrue(partitions.contains(existingPartition));
      Path partitionPath = new Path(existingPartition.getSd().getLocation());
      Assert.assertTrue("The location '" + partitionPath.toString() + "' should exist.",
          getMetaStore().isPathExists(partitionPath));
    }
  }

}
