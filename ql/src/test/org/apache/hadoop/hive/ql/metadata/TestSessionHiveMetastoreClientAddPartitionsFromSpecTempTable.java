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
import org.apache.hadoop.hive.metastore.client.TestAddPartitionsFromPartSpec;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * Test class for adding partitions from partition spec related methods on temporary tables.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestSessionHiveMetastoreClientAddPartitionsFromSpecTempTable
    extends TestAddPartitionsFromPartSpec {

  private HiveConf conf;

  public TestSessionHiveMetastoreClientAddPartitionsFromSpecTempTable(String name, AbstractMetaStoreService metaStore) {
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

  @Override
  protected void verifyPartition(Table table, String name, List<String> values, int index) throws Exception {

    Partition part = getClient().getPartition(table.getDbName(), table.getTableName(), name);
    Assert.assertNotNull("The partition should not be null.", part);
    Assert.assertEquals("The table name in the partition is not correct.", table.getTableName(), part.getTableName());
    List<String> partValues = part.getValues();
    Assert.assertEquals(values.size(), partValues.size());
    Assert.assertTrue("The partition has wrong values.", partValues.containsAll(values));
    Assert.assertEquals("The DB name in the partition is not correct.", table.getDbName(), part.getDbName());
    Assert.assertEquals("The last access time is not correct.", DEFAULT_CREATE_TIME, part.getLastAccessTime());
    Assert.assertEquals("The partition's parameter map should contain the partparamkey - partparamvalue pair.",
        DEFAULT_PARAM_VALUE + index, part.getParameters().get(DEFAULT_PARAM_KEY + index));
    StorageDescriptor sd = part.getSd();
    Assert.assertNotNull("The partition's storage descriptor must not be null.", sd);
    Assert.assertEquals("The input format is not correct.", "TestInputFormat" + index, sd.getInputFormat());
    Assert.assertEquals("The output format is not correct.", "TestOutputFormat" + index, sd.getOutputFormat());
    Assert.assertEquals("The serdeInfo name is not correct.", "partserde" + index, sd.getSerdeInfo().getName());
    Assert.assertEquals(
        "The parameter map of the partition's storage descriptor should contain the partsdkey - partsdvalue pair.",
        "partsdvalue" + index, sd.getParameters().get("partsdkey" + index));
    Assert.assertEquals("The parameter's location is not correct.",
        getMetaStore().getWarehouseRoot() + "/" + TABLE_NAME + "/" + name, sd.getLocation());
    Assert.assertTrue("The parameter's location should exist on the file system.",
        getMetaStore().isPathExists(new Path(sd.getLocation())));
    // If the 'metastore.partition.inherit.table.properties' property is set in the metastore
    // config, the partition inherits the listed table parameters.
    // This property is not set in this test, therefore the partition doesn't inherit the table
    // parameters.
    Assert.assertFalse("The partition should not inherit the table parameters.",
        part.getParameters().keySet().contains(table.getParameters().keySet()));
  }
}
