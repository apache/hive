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
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.CustomIgnoreRule;
import org.apache.hadoop.hive.metastore.client.TestAppendPartitions;
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
 * Test class for append partitions related methods on temporary tables.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestSessionHiveMetastoreClientAppendPartitionTempTable extends TestAppendPartitions {

  private HiveConf conf;

  public TestSessionHiveMetastoreClientAppendPartitionTempTable(String name, AbstractMetaStoreService metaStore) {
    super(name, metaStore);
    ignoreRule = new CustomIgnoreRule();
  }

  @Before
  public void setUp() throws Exception {
    initHiveConf();
    SessionState.start(conf);
    setClient(Hive.get(conf).getMSC());
    cleanUpDatabase();
    createTables();
  }

  private void initHiveConf() throws HiveException {
    conf = new HiveConfForTest(Hive.get().getConf(), getClass());
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_FASTPATH, true);
  }

  @Override
  protected Table createTable(String tableName, List<FieldSchema> partCols, Map<String, String> tableParams,
      String tableType, String location) throws Exception {
    TableBuilder builder =
        new TableBuilder().setDbName(DB_NAME).setTableName(tableName).addCol("test_id", "int", "test col id")
            .addCol("test_value", "string", "test col value").setPartCols(partCols)
            .setType(tableType).setLocation(location).setTemporary(true);
    if (tableParams != null) {
      builder.setTableParams(tableParams);
    }
    builder.create(getClient(), conf);
    return getClient().getTable(DB_NAME, tableName);
  }

  @Override
  protected void verifyPartition(Partition partition, Table table, List<String> expectedPartValues,
      String partitionName) throws Exception {
    Assert.assertEquals(table.getTableName(), partition.getTableName());
    Assert.assertEquals(table.getDbName(), partition.getDbName());
    Assert.assertEquals(expectedPartValues, partition.getValues());
    Assert.assertNotEquals(0, partition.getCreateTime());
    Assert.assertEquals(0, partition.getParameters().size());
    StorageDescriptor partitionSD = partition.getSd();
    Assert.assertEquals(table.getSd().getLocation() + "/" + partitionName,
        partitionSD.getLocation());
    Assert.assertTrue(getMetaStore().isPathExists(new Path(partitionSD.getLocation())));
  }
}
