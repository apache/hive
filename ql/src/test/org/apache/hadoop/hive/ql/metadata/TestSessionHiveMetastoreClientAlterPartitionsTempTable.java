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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.CustomIgnoreRule;
import org.apache.hadoop.hive.metastore.client.TestAlterPartitions;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Test class for alter/rename partitions related methods on temporary tables.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestSessionHiveMetastoreClientAlterPartitionsTempTable extends TestAlterPartitions {

  private HiveConf conf;

  private static final String PART_PRIV = "PARTITION_LEVEL_PRIVILEGE";

  public TestSessionHiveMetastoreClientAlterPartitionsTempTable(String name, AbstractMetaStoreService metaStore) {
    super(name, metaStore);
    ignoreRule = new CustomIgnoreRule();
  }

  @Before
  public void setUp() throws Exception {
    initHiveConf();
    SessionState.start(conf);
    setClient(Hive.get(conf).getMSC());
    cleanDB();
    createDB(DB_NAME);
  }

  private void initHiveConf() throws HiveException {
    conf = new HiveConfForTest(Hive.get().getConf(), getClass());
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_FASTPATH, true);
  }

  @Override
  protected Table createTestTable(IMetaStoreClient client, String dbName, String tableName,
      List<String> partCols, boolean setPartitionLevelPrivilages)
      throws Exception {
    TableBuilder builder = new TableBuilder()
        .setDbName(dbName)
        .setTableName(tableName)
        .addCol("id", "int")
        .addCol("name", "string")
        .setTemporary(true);

    partCols.forEach(col -> builder.addPartCol(col, "string"));
    Table table = builder.build(getMetaStore().getConf());

    if (setPartitionLevelPrivilages) {
      table.putToParameters(PART_PRIV, "true");
    }

    client.createTable(table);
    return table;
  }

  @Override
  protected void addPartition(IMetaStoreClient client, Table table, List<String> values)
      throws TException {
    PartitionBuilder builder = new PartitionBuilder().inTable(table);
    values.forEach(builder::addValue);
    Partition partition = builder.build(conf);
    getClient().add_partition(partition);
  }

  @Override
  protected void addPartitions(IMetaStoreClient client, Table table, List<String> values) throws Exception {
    List<Partition> partitions = new ArrayList<>();
    for (int i = 0; i < values.size(); i++) {
      partitions.add(new PartitionBuilder().inTable(table)
          .addValue(values.get(i))
          .setLocation(MetaStoreTestUtils.getTestWarehouseDir(values.get(i) + i))
          .build(conf));
    }
    client.add_partitions(partitions);
  }

  @Override
  protected void assertPartitionUnchanged(Partition partition, List<String> testValues,
      List<String> partCols) throws Exception {
    assertFalse(partition.getParameters().containsKey("hmsTestParam001"));

    List<String> expectedKVPairs = new ArrayList<>();
    for (int i = 0; i < partCols.size(); ++i) {
      expectedKVPairs.add(partCols.get(i) + "=" + testValues.get(i));
    }
    Table table = getClient().getTable(partition.getDbName(), partition.getTableName());
    String partPath = String.join("/", expectedKVPairs);
    assertEquals(partition.getSd().getLocation(), table.getSd().getLocation() + "/" + partPath);
    assertEquals(2, partition.getSd().getCols().size());
  }

  @Override
  protected void assertPartitionChanged(Partition partition, List<String> testValues,
      List<String> partCols) throws Exception {
    assertEquals("testValue001", partition.getParameters().get("hmsTestParam001"));

    List<String> expectedKVPairs = new ArrayList<>();
    for (int i = 0; i < partCols.size(); ++i) {
      expectedKVPairs.add(partCols.get(i) + "=" + testValues.get(i));
    }
    Table table = getClient().getTable(partition.getDbName(), partition.getTableName());
    String partPath = String.join("/", expectedKVPairs);
    assertEquals(partition.getSd().getLocation(), table.getSd().getLocation() + "/" + partPath + "/hh=01");
    assertEquals(NEW_CREATE_TIME, partition.getCreateTime());
    assertEquals(NEW_CREATE_TIME, partition.getLastAccessTime());
    assertEquals(3, partition.getSd().getCols().size());
  }

  @Override
  @Test(expected = InvalidOperationException.class)
  public void testRenamePartitionNullNewPart() throws Exception {
    super.testRenamePartitionNullNewPart();
  }

  @Override
  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionsNullPartition() throws Exception {
    super.testAlterPartitionsNullPartition();
  }

  @Override
  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionsWithEnvironmentCtxNullPartition() throws Exception {
    super.testAlterPartitionsWithEnvironmentCtxNullPartition();
  }

  @Override
  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionsNullPartitions() throws Exception {
    super.testAlterPartitionsNullPartitions();
  }

  @Override
  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionsWithEnvironmentCtxNullPartitions() throws Exception {
    super.testAlterPartitionsWithEnvironmentCtxNullPartitions();
  }

  @Test
  public void testAlterPartitionsCheckRollbackNullPartition() throws Exception {
    createTable4PartColsParts(getClient());
    List<Partition> oldParts = getClient().listPartitions(DB_NAME, TABLE_NAME, (short) -1);
    assertPartitionRollback(oldParts, Lists.newArrayList(oldParts.get(0), null, oldParts.get(1)));
  }

  @Test
  public void testAlterPartitionsCheckRollbackNullPartitions() throws Exception {
    createTable4PartColsParts(getClient());
    assertPartitionRollback(getClient().listPartitions(DB_NAME, TABLE_NAME, (short) -1),
        Lists.newArrayList(null, null));
  }

  @Test
  public void testAlterPartitionsCheckRollbackPartValsNull() throws Exception {
    createTable4PartColsParts(getClient());
    List<Partition> oldParts = getClient().listPartitions(DB_NAME, TABLE_NAME, (short) -1);
    Partition partition = new Partition(oldParts.get(0));
    partition.setValues(null);
    assertPartitionRollback(oldParts, Lists.newArrayList(partition));
  }

  @Test
  public void testAlterPartitionsCheckRollbackUnknownPartition() throws Exception {
    createTable4PartColsParts(getClient());
    Table table = getClient().getTable(DB_NAME, TABLE_NAME);
    Partition newPart1 =
        new PartitionBuilder().inTable(table).addValue("1111").addValue("1111").addValue("11").build(conf);
    List<Partition> oldPartitions = getClient().listPartitions(DB_NAME, TABLE_NAME, (short) -1);
    Partition newPart2 = new Partition(oldPartitions.get(0));
    makeTestChangesOnPartition(newPart2);
    assertPartitionRollback(oldPartitions, Lists.newArrayList(newPart2, newPart1));
  }

  @Test
  public void testAlterPartitionsCheckRollbackChangeDBName() throws Exception {
    createTable4PartColsParts(getClient());
    List<Partition> oldPartitions = getClient().listPartitions(DB_NAME, TABLE_NAME, (short) -1);
    Partition newPart1 = new Partition(oldPartitions.get(3));
    newPart1.setDbName(DB_NAME + "_changed");
    assertPartitionRollback(oldPartitions, Lists.newArrayList(oldPartitions.get(0), oldPartitions.get(1), newPart1,
        oldPartitions.get(2)));
  }

  @Test
  public void testAlterPartitionsCheckRollbackChangeTableName() throws Exception {
    createTable4PartColsParts(getClient());
    List<Partition> oldPartitions = getClient().listPartitions(DB_NAME, TABLE_NAME, (short) -1);
    Partition newPart1 = new Partition(oldPartitions.get(3));
    newPart1.setTableName(TABLE_NAME + "_changed");
    assertPartitionRollback(oldPartitions, Lists.newArrayList(oldPartitions.get(0), oldPartitions.get(1), newPart1,
        oldPartitions.get(2)));
  }

  @SuppressWarnings("deprecation")
  private void assertPartitionRollback(List<Partition> oldParts, List<Partition> alterParts) throws TException {
    try {
      getClient().alter_partitions(DB_NAME, TABLE_NAME, alterParts);
    } catch (MetaException | InvalidOperationException e) {
      assertEquals(oldParts, getClient().listPartitions(DB_NAME, TABLE_NAME, (short) -1));
      return;
    }
    fail("Exception should have been thrown.");
  }

}
