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
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import com.google.common.collect.Lists;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * API tests for HMS client's  alterPartitions methods.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestAlterPartitions {

  public static final int NEW_CREATE_TIME = 123456789;
  // Needed until there is no junit release with @BeforeParam, @AfterParam (junit 4.13)
  // https://github.com/junit-team/junit4/commit/1bf8438b65858565dbb64736bfe13aae9cfc1b5a
  // Then we should remove our own copy
  private static Set<AbstractMetaStoreService> metaStoreServices = null;
  private AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;

  private static final String DB_NAME = "testpartdb";
  private static final String TABLE_NAME = "testparttable";
  private static final List<String> PARTCOL_SCHEMA = Lists.newArrayList("yyyy", "mm", "dd");


  @Parameterized.Parameters(name = "{0}")
  public static List<Object[]> getMetaStoreToTest() throws Exception {
    List<Object[]> result = MetaStoreFactoryForTests.getMetaStores();
    metaStoreServices = result.stream()
            .map(test -> (AbstractMetaStoreService)test[1])
            .collect(Collectors.toSet());
    return result;
  }

  public TestAlterPartitions(String name, AbstractMetaStoreService metaStore) throws Exception {
    this.metaStore = metaStore;
    this.metaStore.start();
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
    createDB(DB_NAME);
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

  private void createDB(String dbName) throws TException {
    Database db = new DatabaseBuilder().
            setName(dbName).
            build();
    client.createDatabase(db);
  }

  private static Table createTestTable(IMetaStoreClient client, String dbName, String tableName,
                                       List<String> partCols, boolean setPartitionLevelPrivilages)
          throws Exception {
    TableBuilder builder = new TableBuilder()
            .setDbName(dbName)
            .setTableName(tableName)
            .addCol("id", "int")
            .addCol("name", "string");

    partCols.forEach(col -> builder.addPartCol(col, "string"));
    Table table = builder.build();

    if (setPartitionLevelPrivilages) {
      table.putToParameters("PARTITION_LEVEL_PRIVILEGE", "true");
    }

    client.createTable(table);
    return table;
  }

  private static void addPartition(IMetaStoreClient client, Table table, List<String> values)
          throws TException {
    PartitionBuilder partitionBuilder = new PartitionBuilder().fromTable(table);
    values.forEach(val -> partitionBuilder.addValue(val));
    client.add_partition(partitionBuilder.build());
  }

  private static List<List<String>> createTable4PartColsParts(IMetaStoreClient client) throws
          Exception {
    Table t = createTestTable(client, DB_NAME, TABLE_NAME, PARTCOL_SCHEMA, false);
    List<List<String>> testValues = Lists.newArrayList(
            Lists.newArrayList("1999", "01", "02"),
            Lists.newArrayList("2009", "02", "10"),
            Lists.newArrayList("2017", "10", "26"),
            Lists.newArrayList("2017", "11", "27"));

    for(List<String> vals : testValues){
      addPartition(client, t, vals);
    }

    return testValues;
  }

  private static void assertPartitionsHaveCorrectValues(List<Partition> partitions,
                                    List<List<String>> testValues) throws Exception {
    assertEquals(testValues.size(), partitions.size());
    for (int i = 0; i < partitions.size(); ++i) {
      assertEquals(testValues.get(i), partitions.get(i).getValues());
    }
  }

  private static void makeTestChangesOnPartition(Partition partition) {
    partition.getParameters().put("hmsTestParam001", "testValue001");
    partition.setCreateTime(NEW_CREATE_TIME);
    partition.setLastAccessTime(NEW_CREATE_TIME);
    partition.getSd().setLocation(partition.getSd().getLocation()+"/hh=01");
    partition.getSd().getCols().add(new FieldSchema("newcol", "string", ""));
  }

  private static void assertPartitionUnchanged(Partition partition, List<String> testValues,
                                               List<String> partCols) {
    assertFalse(partition.getParameters().containsKey("hmsTestParam001"));

    List<String> expectedKVPairs = new ArrayList<>();
    for (int i = 0; i < partCols.size(); ++i) {
      expectedKVPairs.add(partCols.get(i) + "=" + testValues.get(i));
    }
    String partPath = expectedKVPairs.stream().collect(joining("/"));
    assertTrue(partition.getSd().getLocation().endsWith("warehouse/testpartdb" +
            ".db/testparttable/" + partPath));
    assertNotEquals(NEW_CREATE_TIME, partition.getCreateTime());
    assertNotEquals(NEW_CREATE_TIME, partition.getLastAccessTime());
    assertEquals(2, partition.getSd().getCols().size());
  }

  private static void assertPartitionChanged(Partition partition, List<String> testValues,
                                             List<String> partCols) {
    assertEquals("testValue001", partition.getParameters().get("hmsTestParam001"));

    List<String> expectedKVPairs = new ArrayList<>();
    for (int i = 0; i < partCols.size(); ++i) {
      expectedKVPairs.add(partCols.get(i) + "=" + testValues.get(i));
    }
    String partPath = expectedKVPairs.stream().collect(joining("/"));
    assertTrue(partition.getSd().getLocation().endsWith("warehouse/testpartdb" +
            ".db/testparttable/" + partPath + "/hh=01"));
    assertEquals(NEW_CREATE_TIME, partition.getCreateTime());
    assertEquals(NEW_CREATE_TIME, partition.getLastAccessTime());
    assertEquals(3, partition.getSd().getCols().size());
  }



  /**
   * Testing alter_partition(String,String,Partition) ->
   *         alter_partition_with_environment_context(String,String,Partition,null).
   * @throws Exception
   */
  @Test
  public void testAlterPartition() throws Exception {
    List<List<String>> testValues = createTable4PartColsParts(client);
    List<Partition> oldParts = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    Partition oldPart = oldParts.get(3);

    assertPartitionUnchanged(oldPart, testValues.get(3), PARTCOL_SCHEMA);
    makeTestChangesOnPartition(oldPart);

    client.alter_partition(DB_NAME, TABLE_NAME, oldPart);

    List<Partition> newParts = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    Partition newPart = newParts.get(3);
    assertPartitionChanged(newPart, testValues.get(3), PARTCOL_SCHEMA);
    assertPartitionsHaveCorrectValues(newParts, testValues);

  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionUnknownPartition() throws Exception {
    createTable4PartColsParts(client);
    Table t = client.getTable(DB_NAME, TABLE_NAME);
    PartitionBuilder builder = new PartitionBuilder();
    Partition part = builder.fromTable(t).addValue("1111").addValue("11").addValue("11").build();
    client.alter_partition(DB_NAME, TABLE_NAME, part);
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionIncompletePartitionVals() throws Exception {
    createTable4PartColsParts(client);
    Table t = client.getTable(DB_NAME, TABLE_NAME);
    PartitionBuilder builder = new PartitionBuilder();
    Partition part = builder.fromTable(t).addValue("2017").build();
    client.alter_partition(DB_NAME, TABLE_NAME, part);
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionMissingPartitionVals() throws Exception {
    createTable4PartColsParts(client);
    Table t = client.getTable(DB_NAME, TABLE_NAME);
    PartitionBuilder builder = new PartitionBuilder();
    Partition part = builder.fromTable(t).build();
    client.alter_partition(DB_NAME, TABLE_NAME, part);
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionNoDbName() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    client.alter_partition("", TABLE_NAME, partitions.get(3));
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionNullDbName() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    client.alter_partition(null, TABLE_NAME, partitions.get(3));
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionNoTblName() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    client.alter_partition(DB_NAME, "", partitions.get(3));
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionNullTblName() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    client.alter_partition(DB_NAME, null, partitions.get(3));
  }

  @Test
  public void testAlterPartitionNullPartition() throws Exception {
    try {
      createTable4PartColsParts(client);
      List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
      client.alter_partition(DB_NAME, TABLE_NAME, null);
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionChangeDbName() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    Partition partition = partitions.get(3);
    partition.setDbName(DB_NAME+"_changed");
    client.alter_partition(DB_NAME, TABLE_NAME, partition);
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionChangeTableName() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    Partition partition = partitions.get(3);
    partition.setTableName(TABLE_NAME+"_changed");
    client.alter_partition(DB_NAME, TABLE_NAME, partition);
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionChangeValues() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    Partition partition = partitions.get(3);
    partition.setValues(Lists.newArrayList("1", "2", "3"));
    client.alter_partition(DB_NAME, TABLE_NAME, partition);
  }


  /**
   * Testing alter_partition(String,String,Partition,EnvironmentContext) ->
   *         alter_partition_with_environment_context(String,String,Partition,EnvironmentContext).
   * @throws Exception
   */
  @Test
  public void testAlterPartitionWithEnvironmentCtx() throws Exception {
    EnvironmentContext context = new EnvironmentContext();
    context.setProperties(new HashMap<String, String>(){
      {
        put("TestKey", "TestValue");
      }
    });

    List<List<String>> testValues = createTable4PartColsParts(client);
    List<Partition> oldParts = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    Partition partition = oldParts.get(3);

    assertPartitionUnchanged(partition, testValues.get(3), PARTCOL_SCHEMA);
    makeTestChangesOnPartition(partition);

    client.alter_partition(DB_NAME, TABLE_NAME, partition, context);

    List<Partition> newParts = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    partition = newParts.get(3);
    assertPartitionChanged(partition, testValues.get(3), PARTCOL_SCHEMA);
    assertPartitionsHaveCorrectValues(newParts, testValues);

    client.alter_partition(DB_NAME, TABLE_NAME, partition, new EnvironmentContext());
    client.alter_partition(DB_NAME, TABLE_NAME, partition, null);
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionWithEnvironmentCtxUnknownPartition() throws Exception {
    createTable4PartColsParts(client);
    Table t = client.getTable(DB_NAME, TABLE_NAME);
    PartitionBuilder builder = new PartitionBuilder();
    Partition part = builder.fromTable(t).addValue("1111").addValue("11").addValue("11").build();
    client.alter_partition(DB_NAME, TABLE_NAME, part, new EnvironmentContext());
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionWithEnvironmentCtxIncompletePartitionVals() throws Exception {
    createTable4PartColsParts(client);
    Table t = client.getTable(DB_NAME, TABLE_NAME);
    PartitionBuilder builder = new PartitionBuilder();
    Partition part = builder.fromTable(t).addValue("2017").build();
    client.alter_partition(DB_NAME, TABLE_NAME, part, new EnvironmentContext());
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionWithEnvironmentCtxMissingPartitionVals() throws Exception {
    createTable4PartColsParts(client);
    Table t = client.getTable(DB_NAME, TABLE_NAME);
    PartitionBuilder builder = new PartitionBuilder();
    Partition part = builder.fromTable(t).build();
    client.alter_partition(DB_NAME, TABLE_NAME, part, new EnvironmentContext());
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionWithEnvironmentCtxNoDbName() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    client.alter_partition("", TABLE_NAME, partitions.get(3), new EnvironmentContext());
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionWithEnvironmentCtxNullDbName() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    client.alter_partition(null, TABLE_NAME, partitions.get(3), new EnvironmentContext());
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionWithEnvironmentCtxNoTblName() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    client.alter_partition(DB_NAME, "", partitions.get(3), new EnvironmentContext());
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionWithEnvironmentCtxNullTblName() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    client.alter_partition(DB_NAME, null, partitions.get(3), new EnvironmentContext());
  }

  @Test
  public void testAlterPartitionWithEnvironmentCtxNullPartition() throws Exception {
    try {
      createTable4PartColsParts(client);
      List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short) -1);
      client.alter_partition(DB_NAME, TABLE_NAME, null, new EnvironmentContext());
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionWithEnvironmentCtxChangeDbName() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    Partition partition = partitions.get(3);
    partition.setDbName(DB_NAME+"_changed");
    client.alter_partition(DB_NAME, TABLE_NAME, partition, new EnvironmentContext());
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionWithEnvironmentCtxChangeTableName() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    Partition partition = partitions.get(3);
    partition.setTableName(TABLE_NAME+"_changed");
    client.alter_partition(DB_NAME, TABLE_NAME, partition, new EnvironmentContext());
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionWithEnvironmentCtxChangeValues() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    Partition partition = partitions.get(3);
    partition.setValues(Lists.newArrayList("1", "2", "3"));
    client.alter_partition(DB_NAME, TABLE_NAME, partition, new EnvironmentContext());
  }



  /**
   * Testing
   *    alter_partitions(String,String,List(Partition)) ->
   *    alter_partitions_with_environment_context(String,String,List(Partition),null).
   * @throws Exception
   */
  @Test
  public void testAlterPartitions() throws Exception {
    List<List<String>> testValues = createTable4PartColsParts(client);
    List<Partition> oldParts = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);

    for (int i = 0; i < testValues.size(); ++i) {
      assertPartitionUnchanged(oldParts.get(i), testValues.get(i), PARTCOL_SCHEMA);
    }
    oldParts.forEach(p -> makeTestChangesOnPartition(p));

    client.alter_partitions(DB_NAME, TABLE_NAME, oldParts);

    List<Partition> newParts = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);

    for (int i = 0; i < testValues.size(); ++i) {
      assertPartitionChanged(oldParts.get(i), testValues.get(i), PARTCOL_SCHEMA);
    }
    assertPartitionsHaveCorrectValues(newParts, testValues);
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionsEmptyPartitionList() throws Exception {
    client.alter_partitions(DB_NAME, TABLE_NAME, Lists.newArrayList());
  }

  @Test
  public void testAlterPartitionsUnknownPartition() throws Exception {
    Partition part1 = null;
    try {
      createTable4PartColsParts(client);
      Table t = client.getTable(DB_NAME, TABLE_NAME);
      PartitionBuilder builder = new PartitionBuilder();
      Partition part = builder.fromTable(t).addValue("1111").addValue("11").addValue("11").build();
      part1 = client.listPartitions(DB_NAME, TABLE_NAME, (short) -1).get(0);
      makeTestChangesOnPartition(part1);
      client.alter_partitions(DB_NAME, TABLE_NAME, Lists.newArrayList(part, part1));
      fail("Should have thrown InvalidOperationException");
    } catch (InvalidOperationException e) {
      part1 = client.listPartitions(DB_NAME, TABLE_NAME, (short) -1).get(0);
      assertPartitionUnchanged(part1, part1.getValues(), PARTCOL_SCHEMA);
    }
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionsIncompletePartitionVals() throws Exception {
    createTable4PartColsParts(client);
    Table t = client.getTable(DB_NAME, TABLE_NAME);
    PartitionBuilder builder = new PartitionBuilder();
    Partition part = builder.fromTable(t).addValue("2017").build();
    Partition part1 = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    client.alter_partitions(DB_NAME, TABLE_NAME, Lists.newArrayList(part, part1));
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionsMissingPartitionVals() throws Exception {
    createTable4PartColsParts(client);
    Table t = client.getTable(DB_NAME, TABLE_NAME);
    PartitionBuilder builder = new PartitionBuilder();
    Partition part = builder.fromTable(t).build();
    Partition part1 = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    client.alter_partitions(DB_NAME, TABLE_NAME, Lists.newArrayList(part, part1));
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionsNoDbName() throws Exception {
    createTable4PartColsParts(client);
    Partition part = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    client.alter_partitions("", TABLE_NAME, Lists.newArrayList(part));
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionsNullDbName() throws Exception {
    createTable4PartColsParts(client);
    Partition part = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    client.alter_partitions(null, TABLE_NAME, Lists.newArrayList(part));
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionsNoTblName() throws Exception {
    createTable4PartColsParts(client);
    Partition part = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    client.alter_partitions(DB_NAME, "", Lists.newArrayList(part));
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionsNullTblName() throws Exception {
    createTable4PartColsParts(client);
    Partition part = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    client.alter_partitions(DB_NAME, null, Lists.newArrayList(part));
  }

  @Test(expected = NullPointerException.class)
  public void testAlterPartitionsNullPartition() throws Exception {
    createTable4PartColsParts(client);
    Partition part = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    client.alter_partitions(DB_NAME, TABLE_NAME, Lists.newArrayList(part, null));
  }

  @Test(expected = NullPointerException.class)
  public void testAlterPartitionsNullPartitions() throws Exception {
    createTable4PartColsParts(client);
    Partition part = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    client.alter_partitions(DB_NAME, TABLE_NAME, Lists.newArrayList(null, null));
  }

  @Test
  public void testAlterPartitionsNullPartitionList() throws Exception {
    try {
      createTable4PartColsParts(client);
      Partition part = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
      client.alter_partitions(DB_NAME, TABLE_NAME, null);
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionsChangeDbName() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    Partition p = partitions.get(3);
    p.setDbName(DB_NAME+"_changed");
    client.alter_partitions(DB_NAME, TABLE_NAME, Lists.newArrayList(p));
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionsChangeTableName() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    Partition p = partitions.get(3);
    p.setTableName(TABLE_NAME+"_changed");
    client.alter_partitions(DB_NAME, TABLE_NAME, Lists.newArrayList(p));
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionsChangeValues() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    Partition p = partitions.get(3);
    p.setValues(Lists.newArrayList("1", "2", "3"));
    client.alter_partitions(DB_NAME, TABLE_NAME, Lists.newArrayList(p));
  }



  /**
   * Testing
   *    alter_partitions(String,String,List(Partition),EnvironmentContext) ->
   *    alter_partitions_with_environment_context(String,String,List(Partition),EnvironmentContext).
   * @throws Exception
   */
  @Test
  public void testAlterPartitionsWithEnvironmentCtx() throws Exception {
    EnvironmentContext context = new EnvironmentContext();
    context.setProperties(new HashMap<String, String>(){
      {
        put("TestKey", "TestValue");
      }
    });

    List<List<String>> testValues = createTable4PartColsParts(client);
    List<Partition> oldParts = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);

    for (int i = 0; i < testValues.size(); ++i) {
      assertPartitionUnchanged(oldParts.get(i), testValues.get(i), PARTCOL_SCHEMA);
    }
    oldParts.forEach(p -> makeTestChangesOnPartition(p));

    client.alter_partitions(DB_NAME, TABLE_NAME, oldParts, context);

    List<Partition> newParts = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);

    for (int i = 0; i < testValues.size(); ++i) {
      assertPartitionChanged(oldParts.get(i), testValues.get(i), PARTCOL_SCHEMA);
    }
    assertPartitionsHaveCorrectValues(newParts, testValues);

    client.alter_partitions(DB_NAME, TABLE_NAME, newParts, new EnvironmentContext());
    client.alter_partitions(DB_NAME, TABLE_NAME, newParts, null);

    for (int i = 0; i < testValues.size(); ++i) {
      assertPartitionChanged(oldParts.get(i), testValues.get(i), PARTCOL_SCHEMA);
    }
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionsWithEnvironmentCtxEmptyPartitionList() throws Exception {
    client.alter_partitions(DB_NAME, TABLE_NAME, Lists.newArrayList(), new EnvironmentContext());
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionsWithEnvironmentCtxUnknownPartition() throws Exception {
    createTable4PartColsParts(client);
    Table t = client.getTable(DB_NAME, TABLE_NAME);
    PartitionBuilder builder = new PartitionBuilder();
    Partition part = builder.fromTable(t).addValue("1111").addValue("11").addValue("11").build();
    Partition part1 = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    client.alter_partitions(DB_NAME, TABLE_NAME, Lists.newArrayList(part, part1),
            new EnvironmentContext());
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionsWithEnvironmentCtxIncompletePartitionVals() throws Exception {
    createTable4PartColsParts(client);
    Table t = client.getTable(DB_NAME, TABLE_NAME);
    PartitionBuilder builder = new PartitionBuilder();
    Partition part = builder.fromTable(t).addValue("2017").build();
    Partition part1 = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    client.alter_partitions(DB_NAME, TABLE_NAME, Lists.newArrayList(part, part1),
            new EnvironmentContext());
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionsWithEnvironmentCtxMissingPartitionVals() throws Exception {
    createTable4PartColsParts(client);
    Table t = client.getTable(DB_NAME, TABLE_NAME);
    PartitionBuilder builder = new PartitionBuilder();
    Partition part = builder.fromTable(t).build();
    Partition part1 = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    client.alter_partitions(DB_NAME, TABLE_NAME, Lists.newArrayList(part, part1),
            new EnvironmentContext());
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionsWithEnvironmentCtxNoDbName() throws Exception {
    createTable4PartColsParts(client);
    Partition part = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    client.alter_partitions("", TABLE_NAME, Lists.newArrayList(part), new EnvironmentContext());
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionsWithEnvironmentCtxNullDbName() throws Exception {
    createTable4PartColsParts(client);
    Partition part = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    client.alter_partitions(null, TABLE_NAME, Lists.newArrayList(part), new EnvironmentContext());
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionsWithEnvironmentCtxNoTblName() throws Exception {
    createTable4PartColsParts(client);
    Partition part = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    client.alter_partitions(DB_NAME, "", Lists.newArrayList(part), new EnvironmentContext());
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionsWithEnvironmentCtxNullTblName() throws Exception {
    createTable4PartColsParts(client);
    Partition part = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    client.alter_partitions(DB_NAME, null, Lists.newArrayList(part), new EnvironmentContext());
  }

  @Test(expected = NullPointerException.class)
  public void testAlterPartitionsWithEnvironmentCtxNullPartition() throws Exception {
    createTable4PartColsParts(client);
    Partition part = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    client.alter_partitions(DB_NAME, TABLE_NAME, Lists.newArrayList(part, null),
            new EnvironmentContext());
  }

  @Test(expected = NullPointerException.class)
  public void testAlterPartitionsWithEnvironmentCtxNullPartitions() throws Exception {
    createTable4PartColsParts(client);
    Partition part = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    client.alter_partitions(DB_NAME, TABLE_NAME, Lists.newArrayList(null, null),
            new EnvironmentContext());
  }

  @Test
  public void testAlterPartitionsWithEnvironmentCtxNullPartitionList() throws Exception {
    try {
      createTable4PartColsParts(client);
      Partition part = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
      client.alter_partitions(DB_NAME, TABLE_NAME, null, new EnvironmentContext());
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionsWithEnvironmentCtxChangeDbName() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    Partition p = partitions.get(3);
    p.setDbName(DB_NAME+"_changed");
    client.alter_partitions(DB_NAME, TABLE_NAME, Lists.newArrayList(p), new EnvironmentContext());
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionsWithEnvironmentCtxChangeTableName() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    Partition p = partitions.get(3);
    p.setTableName(TABLE_NAME+"_changed");
    client.alter_partitions(DB_NAME, TABLE_NAME, Lists.newArrayList(p), new EnvironmentContext());
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionsWithEnvironmentCtxChangeValues() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    Partition p = partitions.get(3);
    p.setValues(Lists.newArrayList("1", "2", "3"));
    client.alter_partitions(DB_NAME, TABLE_NAME, Lists.newArrayList(p), new EnvironmentContext());
  }

  /**
   * Testing
   *    renamePartition(String,String,List(String),Partition) ->
   *    renamePartition(String,String,List(String),Partition).
   * @throws Exception
   */
  @Test
  public void testRenamePartition() throws Exception {

    List<List<String>> oldValues = createTable4PartColsParts(client);
    List<List<String>> newValues = new ArrayList<>();

    List<String> newVal = Lists.newArrayList("2018", "01", "16");
    newValues.addAll(oldValues.subList(0, 3));
    newValues.add(newVal);

    List<Partition> oldParts = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);

    Partition partToRename = oldParts.get(3);
    partToRename.setValues(newVal);
    makeTestChangesOnPartition(partToRename);
    client.renamePartition(DB_NAME, TABLE_NAME, oldValues.get(3), partToRename);

    List<Partition> newParts = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    assertPartitionsHaveCorrectValues(newParts, newValues);


    //Asserting other partition parameters can also be changed, but not the location
    assertFalse(newParts.get(3).getSd().getLocation().endsWith("hh=01"));
    assertEquals(3, newParts.get(3).getSd().getCols().size());
    assertEquals("testValue001", newParts.get(3).getParameters().get("hmsTestParam001"));
    assertEquals(NEW_CREATE_TIME, newParts.get(3).getCreateTime());
    assertEquals(NEW_CREATE_TIME, newParts.get(3).getLastAccessTime());



    assertTrue(client.listPartitions(DB_NAME, TABLE_NAME, oldValues.get(3), (short)-1).isEmpty());
  }

  @Test(expected = InvalidOperationException.class)
  public void testRenamePartitionTargetAlreadyExisting() throws Exception {
    List<List<String>> oldValues = createTable4PartColsParts(client);
    List<Partition> oldParts = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);

    Partition partToRename = oldParts.get(3);
    partToRename.setValues(Lists.newArrayList("2018", "01", "16"));
    client.renamePartition(DB_NAME, TABLE_NAME, oldValues.get(3), oldParts.get(2));
  }

  @Test(expected = InvalidOperationException.class)
  public void testRenamePartitionNoSuchOldPartition() throws Exception {
    List<List<String>> oldValues = createTable4PartColsParts(client);
    List<Partition> oldParts = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);

    Partition partToRename = oldParts.get(3);
    partToRename.setValues(Lists.newArrayList("2018", "01", "16"));
    client.renamePartition(DB_NAME, TABLE_NAME, Lists.newArrayList("1", "2", ""), partToRename);
  }

  @Test(expected = MetaException.class)
  public void testRenamePartitionNullTableInPartition() throws Exception {
    List<List<String>> oldValues = createTable4PartColsParts(client);
    List<Partition> oldParts = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);

    Partition partToRename = oldParts.get(3);
    partToRename.setValues(Lists.newArrayList("2018", "01", "16"));
    partToRename.setTableName(null);
    client.renamePartition(DB_NAME, TABLE_NAME, Lists.newArrayList("2017", "11", "27"),
            partToRename);
  }

  @Test(expected = MetaException.class)
  public void testRenamePartitionNullDbInPartition() throws Exception {
    List<List<String>> oldValues = createTable4PartColsParts(client);
    List<Partition> oldParts = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);

    Partition partToRename = oldParts.get(3);
    partToRename.setValues(Lists.newArrayList("2018", "01", "16"));
    partToRename.setDbName(null);
    client.renamePartition(DB_NAME, TABLE_NAME, Lists.newArrayList("2017", "11", "27"),
            partToRename);
  }

  @Test(expected = InvalidOperationException.class)
  public void testRenamePartitionEmptyOldPartList() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> oldParts = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);

    Partition partToRename = oldParts.get(3);
    partToRename.setValues(Lists.newArrayList("2018", "01", "16"));
    client.renamePartition(DB_NAME, TABLE_NAME, Lists.newArrayList(), partToRename);
  }

  @Test(expected = InvalidOperationException.class)
  public void testRenamePartitionNullOldPartList() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> oldParts = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);

    Partition partToRename = oldParts.get(3);
    partToRename.setValues(Lists.newArrayList("2018", "01", "16"));
    client.renamePartition(DB_NAME, TABLE_NAME, null, partToRename);
  }

  @Test
  public void testRenamePartitionNullNewPart() throws Exception {
    try {
      List<List<String>> oldValues = createTable4PartColsParts(client);
      List<Partition> oldParts = client.listPartitions(DB_NAME, TABLE_NAME, (short) -1);

      Partition partToRename = oldParts.get(3);
      partToRename.setValues(Lists.newArrayList("2018", "01", "16"));
      client.renamePartition(DB_NAME, TABLE_NAME, oldValues.get(3), null);
    } catch (NullPointerException | TTransportException e) {
    }
  }

  @Test(expected = InvalidOperationException.class)
  public void testRenamePartitionNoDbName() throws Exception {
    List<List<String>> oldValues = createTable4PartColsParts(client);
    List<Partition> oldParts = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);

    Partition partToRename = oldParts.get(3);
    partToRename.setValues(Lists.newArrayList("2018", "01", "16"));
    client.renamePartition("", TABLE_NAME, oldValues.get(3), partToRename);
  }

  @Test(expected = InvalidOperationException.class)
  public void testRenamePartitionNoTblName() throws Exception {
    List<List<String>> oldValues = createTable4PartColsParts(client);
    List<Partition> oldParts = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);

    Partition partToRename = oldParts.get(3);
    partToRename.setValues(Lists.newArrayList("2018", "01", "16"));
    client.renamePartition(DB_NAME, "", oldValues.get(3), partToRename);
  }

  @Test(expected = MetaException.class)
  public void testRenamePartitionNullDbName() throws Exception {
    List<List<String>> oldValues = createTable4PartColsParts(client);
    List<Partition> oldParts = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);

    Partition partToRename = oldParts.get(3);
    partToRename.setValues(Lists.newArrayList("2018", "01", "16"));
    client.renamePartition(null, TABLE_NAME, oldValues.get(3), partToRename);
  }

  @Test(expected = MetaException.class)
  public void testRenamePartitionNullTblName() throws Exception {
    List<List<String>> oldValues = createTable4PartColsParts(client);
    List<Partition> oldParts = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);

    Partition partToRename = oldParts.get(3);
    partToRename.setValues(Lists.newArrayList("2018", "01", "16"));
    client.renamePartition(DB_NAME, null, oldValues.get(3), partToRename);
  }

  @Test(expected = MetaException.class)
  public void testRenamePartitionChangeTblName() throws Exception {
    List<List<String>> oldValues = createTable4PartColsParts(client);
    List<Partition> oldParts = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);

    Partition partToRename = oldParts.get(3);
    partToRename.setValues(Lists.newArrayList("2018", "01", "16"));
    partToRename.setTableName(TABLE_NAME + "_2");
    client.renamePartition(DB_NAME, TABLE_NAME, oldValues.get(3), partToRename);
  }

  @Test(expected = MetaException.class)
  public void testRenamePartitionChangeDbName() throws Exception {
    List<List<String>> oldValues = createTable4PartColsParts(client);
    List<Partition> oldParts = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);

    Partition partToRename = oldParts.get(3);
    partToRename.setValues(Lists.newArrayList("2018", "01", "16"));
    partToRename.setDbName(DB_NAME + "_2");
    client.renamePartition(DB_NAME, TABLE_NAME, oldValues.get(3), partToRename);
  }

  @Test(expected = InvalidOperationException.class)
  public void testRenamePartitionNoTable() throws Exception {
    client.renamePartition(DB_NAME, TABLE_NAME, Lists.newArrayList("2018", "01", "16"),
            new Partition());
  }

}
