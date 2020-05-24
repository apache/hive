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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransportException;

import com.google.common.collect.Lists;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.stream.Collectors.joining;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;
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
public class TestAlterPartitions extends MetaStoreClientTest {
  protected static final int NEW_CREATE_TIME = 123456789;
  private AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;

  protected static final String DB_NAME = "testpartdb";
  protected static final String TABLE_NAME = "testparttable";
  private static final List<String> PARTCOL_SCHEMA = Lists.newArrayList("yyyy", "mm", "dd");

  public TestAlterPartitions(String name, AbstractMetaStoreService metaStore) {
    this.metaStore = metaStore;
  }

  @Before
  public void setUp() throws Exception {
    // Get new client
    client = metaStore.getClient();

    // Clean up the database
    cleanDB();
    createDB(DB_NAME);
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

  public AbstractMetaStoreService getMetaStore() {
    return metaStore;
  }

  public void setMetaStore(AbstractMetaStoreService metaStore) {
    this.metaStore = metaStore;
  }

  protected IMetaStoreClient getClient() {
    return client;
  }

  protected void setClient(IMetaStoreClient client) {
    this.client = client;
  }

  protected void cleanDB() throws Exception{
    client.dropDatabase(DB_NAME, true, true, true);
    metaStore.cleanWarehouseDirs();
  }

  protected void createDB(String dbName) throws TException {
    new DatabaseBuilder().
            setName(dbName).
            create(client, metaStore.getConf());
  }

  protected Table createTestTable(IMetaStoreClient client, String dbName, String tableName,
                                       List<String> partCols, boolean setPartitionLevelPrivilages)
          throws Exception {
    TableBuilder builder = new TableBuilder()
            .setDbName(dbName)
            .setTableName(tableName)
            .addCol("id", "int")
            .addCol("name", "string");

    partCols.forEach(col -> builder.addPartCol(col, "string"));
    Table table = builder.build(metaStore.getConf());

    if (setPartitionLevelPrivilages) {
      table.putToParameters("PARTITION_LEVEL_PRIVILEGE", "true");
    }

    client.createTable(table);
    return table;
  }

  protected void addPartition(IMetaStoreClient client, Table table, List<String> values)
          throws TException {
    PartitionBuilder partitionBuilder = new PartitionBuilder().inTable(table);
    values.forEach(val -> partitionBuilder.addValue(val));
    client.add_partition(partitionBuilder.build(metaStore.getConf()));
  }

  protected void addPartitions(IMetaStoreClient client, Table table, List<String> values) throws Exception {
    List<Partition> partitions = new ArrayList<>();
    for (int i = 0; i < values.size(); i++) {
      partitions.add(new PartitionBuilder().inTable(table)
          .addValue(values.get(i))
          .setLocation(MetaStoreTestUtils.getTestWarehouseDir(values.get(i) + i))
          .build(metaStore.getConf()));
    }
    client.add_partitions(partitions);
  }

  protected List<List<String>> createTable4PartColsParts(IMetaStoreClient client) throws
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

  protected static void makeTestChangesOnPartition(Partition partition) {
    partition.getParameters().put("hmsTestParam001", "testValue001");
    partition.setCreateTime(NEW_CREATE_TIME);
    partition.setLastAccessTime(NEW_CREATE_TIME);
    partition.getSd().setLocation(partition.getSd().getLocation()+"/hh=01");
    partition.getSd().getCols().add(new FieldSchema("newcol", "string", ""));
  }

  protected void assertPartitionUnchanged(Partition partition, List<String> testValues,
                                               List<String> partCols) throws Exception {
    assertFalse(partition.getParameters().containsKey("hmsTestParam001"));

    List<String> expectedKVPairs = new ArrayList<>();
    for (int i = 0; i < partCols.size(); ++i) {
      expectedKVPairs.add(partCols.get(i) + "=" + testValues.get(i));
    }
    String partPath = expectedKVPairs.stream().collect(joining("/"));
    assertTrue(partition.getSd().getLocation().equals(metaStore.getExternalWarehouseRoot()
        + "/testpartdb.db/testparttable/" + partPath));
    assertNotEquals(NEW_CREATE_TIME, partition.getCreateTime());
    assertNotEquals(NEW_CREATE_TIME, partition.getLastAccessTime());
    assertEquals(2, partition.getSd().getCols().size());
  }

  protected void assertPartitionChanged(Partition partition, List<String> testValues,
                                      List<String> partCols) throws Exception {
    assertEquals("testValue001", partition.getParameters().get("hmsTestParam001"));

    List<String> expectedKVPairs = new ArrayList<>();
    for (int i = 0; i < partCols.size(); ++i) {
      expectedKVPairs.add(partCols.get(i) + "=" + testValues.get(i));
    }
    String partPath = expectedKVPairs.stream().collect(joining("/"));
    assertTrue(partition.getSd().getLocation().equals(metaStore.getExternalWarehouseRoot()
        + "/testpartdb.db/testparttable/" + partPath + "/hh=01"));
    assertEquals(NEW_CREATE_TIME, partition.getCreateTime());
    assertEquals(NEW_CREATE_TIME, partition.getLastAccessTime());
    assertEquals(3, partition.getSd().getCols().size());
  }



  /**
   * Testing alter_partition(String,String,Partition) ->
   *         alter_partition_with_environment_context(String,String,Partition,null).
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

  @Test
  @ConditionalIgnoreOnSessionHiveMetastoreClient
  public void otherCatalog() throws TException {
    String catName = "alter_partition_catalog";
    Catalog cat = new CatalogBuilder()
        .setName(catName)
        .setLocation(MetaStoreTestUtils.getTestWarehouseDir(catName))
        .build();
    client.createCatalog(cat);

    String dbName = "alter_partition_database_in_other_catalog";
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .setCatalogName(catName)
        .create(client, metaStore.getConf());

    String tableName = "table_in_other_catalog";
    Table table = new TableBuilder()
        .inDb(db)
        .setTableName(tableName)
        .addCol("id", "int")
        .addCol("name", "string")
        .addPartCol("partcol", "string")
        .create(client, metaStore.getConf());

    Partition[] parts = new Partition[5];
    for (int i = 0; i < 5; i++) {
      parts[i] = new PartitionBuilder()
          .inTable(table)
          .addValue("a" + i)
          .setLocation(MetaStoreTestUtils.getTestWarehouseDir("b" + i))
          .build(metaStore.getConf());
    }
    client.add_partitions(Arrays.asList(parts));

    Partition newPart =
        client.getPartition(catName, dbName, tableName, Collections.singletonList("a0"));
    newPart.getParameters().put("test_key", "test_value");
    client.alter_partition(catName, dbName, tableName, newPart);

    Partition fetched =
        client.getPartition(catName, dbName, tableName, Collections.singletonList("a0"));
    Assert.assertEquals(catName, fetched.getCatName());
    Assert.assertEquals("test_value", fetched.getParameters().get("test_key"));

    newPart =
        client.getPartition(catName, dbName, tableName, Collections.singletonList("a1"));
    newPart.setLastAccessTime(3);
    Partition newPart1 =
        client.getPartition(catName, dbName, tableName, Collections.singletonList("a2"));
    newPart1.getSd().setLocation(MetaStoreTestUtils.getTestWarehouseDir("somewhere"));
    client.alter_partitions(catName, dbName, tableName, Arrays.asList(newPart, newPart1));
    fetched =
        client.getPartition(catName, dbName, tableName, Collections.singletonList("a1"));
    Assert.assertEquals(catName, fetched.getCatName());
    Assert.assertEquals(3L, fetched.getLastAccessTime());
    fetched =
        client.getPartition(catName, dbName, tableName, Collections.singletonList("a2"));
    Assert.assertEquals(catName, fetched.getCatName());
    Assert.assertTrue(fetched.getSd().getLocation().contains("somewhere"));

    newPart =
        client.getPartition(catName, dbName, tableName, Collections.singletonList("a4"));
    newPart.getParameters().put("test_key", "test_value");
    EnvironmentContext ec = new EnvironmentContext();
    ec.setProperties(Collections.singletonMap("a", "b"));
    client.alter_partition(catName, dbName, tableName, newPart, ec);
    fetched =
        client.getPartition(catName, dbName, tableName, Collections.singletonList("a4"));
    Assert.assertEquals(catName, fetched.getCatName());
    Assert.assertEquals("test_value", fetched.getParameters().get("test_key"));


    client.dropDatabase(catName, dbName, true, true, true);
    client.dropCatalog(catName);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void deprecatedCalls() throws Exception {
    String tableName = "deprecated_table";
    Table table = createTestTable(getClient(), DEFAULT_DATABASE_NAME, tableName, Arrays.asList("partcol"), false);
    addPartitions(getClient(), table, Arrays.asList("a0", "a1", "a2", "a3", "a4"));

    Partition newPart =
        client.getPartition(DEFAULT_DATABASE_NAME, tableName, Collections.singletonList("a0"));
    newPart.getParameters().put("test_key", "test_value");
    client.alter_partition(DEFAULT_DATABASE_NAME, tableName, newPart);

    Partition fetched =
        client.getPartition(DEFAULT_DATABASE_NAME, tableName, Collections.singletonList("a0"));
    Assert.assertEquals("test_value", fetched.getParameters().get("test_key"));

    newPart =
        client.getPartition(DEFAULT_DATABASE_NAME, tableName, Collections.singletonList("a1"));
    newPart.setLastAccessTime(3);
    Partition newPart1 =
        client.getPartition(DEFAULT_DATABASE_NAME, tableName, Collections.singletonList("a2"));
    newPart1.getSd().setLocation("somewhere");
    client.alter_partitions(DEFAULT_DATABASE_NAME, tableName, Arrays.asList(newPart, newPart1));
    fetched =
        client.getPartition(DEFAULT_DATABASE_NAME, tableName, Collections.singletonList("a1"));
    Assert.assertEquals(3L, fetched.getLastAccessTime());
    fetched =
        client.getPartition(DEFAULT_DATABASE_NAME, tableName, Collections.singletonList("a2"));
    Assert.assertTrue(fetched.getSd().getLocation().contains("somewhere"));

    newPart =
        client.getPartition(DEFAULT_DATABASE_NAME, tableName, Collections.singletonList("a3"));
    newPart.setValues(Collections.singletonList("b3"));
    client.renamePartition(DEFAULT_DATABASE_NAME, tableName, Collections.singletonList("a3"), newPart);
    fetched =
        client.getPartition(DEFAULT_DATABASE_NAME, tableName, Collections.singletonList("b3"));
    Assert.assertEquals(1, fetched.getValuesSize());
    Assert.assertEquals("b3", fetched.getValues().get(0));

    newPart =
        client.getPartition(DEFAULT_DATABASE_NAME, tableName, Collections.singletonList("a4"));
    newPart.getParameters().put("test_key", "test_value");
    EnvironmentContext ec = new EnvironmentContext();
    ec.setProperties(Collections.singletonMap("a", "b"));
    client.alter_partition(DEFAULT_DATABASE_NAME, tableName, newPart, ec);
    fetched =
        client.getPartition(DEFAULT_DATABASE_NAME, tableName, Collections.singletonList("a4"));
    Assert.assertEquals("test_value", fetched.getParameters().get("test_key"));
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionUnknownPartition() throws Exception {
    createTable4PartColsParts(client);
    Table t = client.getTable(DB_NAME, TABLE_NAME);
    PartitionBuilder builder = new PartitionBuilder();
    Partition part = builder.inTable(t).addValue("1111").addValue("11").addValue("11").build(metaStore.getConf());
    client.alter_partition(DB_NAME, TABLE_NAME, part);
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionIncompletePartitionVals() throws Exception {
    createTable4PartColsParts(client);
    Table t = client.getTable(DB_NAME, TABLE_NAME);
    PartitionBuilder builder = new PartitionBuilder();
    Partition part = builder.inTable(t).addValue("2017").build(metaStore.getConf());
    client.alter_partition(DB_NAME, TABLE_NAME, part);
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionMissingPartitionVals() throws Exception {
    createTable4PartColsParts(client);
    Table t = client.getTable(DB_NAME, TABLE_NAME);
    PartitionBuilder builder = new PartitionBuilder();
    Partition part = builder.inTable(t).build(metaStore.getConf());
    client.alter_partition(DB_NAME, TABLE_NAME, part);
  }

  @Test(expected = InvalidOperationException.class)
  @ConditionalIgnoreOnSessionHiveMetastoreClient
  public void testAlterPartitionBogusCatalogName() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    client.alter_partition("nosuch", DB_NAME, TABLE_NAME, partitions.get(3));
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionNoDbName() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    client.alter_partition("", TABLE_NAME, partitions.get(3));
  }

  @Test
  public void testAlterPartitionNullDbName() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    try {
      client.alter_partition(null, TABLE_NAME, partitions.get(3));
      Assert.fail("Expected exception");
    } catch (MetaException | TProtocolException ex) {
    }
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionNoTblName() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    client.alter_partition(DB_NAME, "", partitions.get(3));
  }

  @Test
  public void testAlterPartitionNullTblName() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    try {
      client.alter_partition(DB_NAME, null, partitions.get(3));
      Assert.fail("Expected exception");
    } catch (MetaException | TProtocolException ex) {
    }
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
    Partition part = builder.inTable(t).addValue("1111").addValue("11").addValue("11").build(metaStore.getConf());
    client.alter_partition(DB_NAME, TABLE_NAME, part, new EnvironmentContext());
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionWithEnvironmentCtxIncompletePartitionVals() throws Exception {
    createTable4PartColsParts(client);
    Table t = client.getTable(DB_NAME, TABLE_NAME);
    PartitionBuilder builder = new PartitionBuilder();
    Partition part = builder.inTable(t).addValue("2017").build(metaStore.getConf());
    client.alter_partition(DB_NAME, TABLE_NAME, part, new EnvironmentContext());
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionWithEnvironmentCtxMissingPartitionVals() throws Exception {
    createTable4PartColsParts(client);
    Table t = client.getTable(DB_NAME, TABLE_NAME);
    PartitionBuilder builder = new PartitionBuilder();
    Partition part = builder.inTable(t).build(metaStore.getConf());
    client.alter_partition(DB_NAME, TABLE_NAME, part, new EnvironmentContext());
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionWithEnvironmentCtxNoDbName() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    client.alter_partition("", TABLE_NAME, partitions.get(3), new EnvironmentContext());
  }

  @Test
  public void testAlterPartitionWithEnvironmentCtxNullDbName() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    try {
      client.alter_partition(null, TABLE_NAME, partitions.get(3), new EnvironmentContext());
      Assert.fail("Expected exception");
    } catch (MetaException | TProtocolException ex) {
    }
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionWithEnvironmentCtxNoTblName() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    client.alter_partition(DB_NAME, "", partitions.get(3), new EnvironmentContext());
  }

  @Test
  public void testAlterPartitionWithEnvironmentCtxNullTblName() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    try {
      client.alter_partition(DB_NAME, null, partitions.get(3), new EnvironmentContext());
      Assert.fail("Expected exception");
    } catch (MetaException | TProtocolException ex) {
    }
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
      Partition part = builder.inTable(t).addValue("1111").addValue("11").addValue("11").build(metaStore.getConf());
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
    Partition part = builder.inTable(t).addValue("2017").build(metaStore.getConf());
    Partition part1 = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    client.alter_partitions(DB_NAME, TABLE_NAME, Lists.newArrayList(part, part1));
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionsMissingPartitionVals() throws Exception {
    createTable4PartColsParts(client);
    Table t = client.getTable(DB_NAME, TABLE_NAME);
    PartitionBuilder builder = new PartitionBuilder();
    Partition part = builder.inTable(t).build(metaStore.getConf());
    Partition part1 = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    client.alter_partitions(DB_NAME, TABLE_NAME, Lists.newArrayList(part, part1));
  }

  @Test(expected = InvalidOperationException.class)
  @ConditionalIgnoreOnSessionHiveMetastoreClient
  public void testAlterPartitionsBogusCatalogName() throws Exception {
    createTable4PartColsParts(client);
    Partition part = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    client.alter_partitions("nosuch", DB_NAME, TABLE_NAME, Lists.newArrayList(part));
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionsNoDbName() throws Exception {
    createTable4PartColsParts(client);
    Partition part = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    client.alter_partitions("", TABLE_NAME, Lists.newArrayList(part));
  }

  @Test
  public void testAlterPartitionsNullDbName() throws Exception {
    createTable4PartColsParts(client);
    Partition part = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    try {
      client.alter_partitions(null, TABLE_NAME, Lists.newArrayList(part));
      Assert.fail("Expected exception");
    } catch (MetaException | TProtocolException ex) {
    }
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionsNoTblName() throws Exception {
    createTable4PartColsParts(client);
    Partition part = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    client.alter_partitions(DB_NAME, "", Lists.newArrayList(part));
  }

  @Test
  public void testAlterPartitionsNullTblName() throws Exception {
    createTable4PartColsParts(client);
    Partition part = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    try {
      client.alter_partitions(DB_NAME, null, Lists.newArrayList(part));
      Assert.fail("didn't throw");
    } catch (TProtocolException | MetaException e) {
      // By design
    }
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
    } catch (NullPointerException | TTransportException | TProtocolException e) {
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
    client.alter_partitions(DB_NAME, TABLE_NAME, newParts);

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
    Partition part = builder.inTable(t).addValue("1111").addValue("11").addValue("11").build(metaStore.getConf());
    Partition part1 = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    client.alter_partitions(DB_NAME, TABLE_NAME, Lists.newArrayList(part, part1),
            new EnvironmentContext());
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionsWithEnvironmentCtxIncompletePartitionVals() throws Exception {
    createTable4PartColsParts(client);
    Table t = client.getTable(DB_NAME, TABLE_NAME);
    PartitionBuilder builder = new PartitionBuilder();
    Partition part = builder.inTable(t).addValue("2017").build(metaStore.getConf());
    Partition part1 = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    client.alter_partitions(DB_NAME, TABLE_NAME, Lists.newArrayList(part, part1),
            new EnvironmentContext());
  }

  @Test(expected = MetaException.class)
  public void testAlterPartitionsWithEnvironmentCtxMissingPartitionVals() throws Exception {
    createTable4PartColsParts(client);
    Table t = client.getTable(DB_NAME, TABLE_NAME);
    PartitionBuilder builder = new PartitionBuilder();
    Partition part = builder.inTable(t).build(metaStore.getConf());
    Partition part1 = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    client.alter_partitions(DB_NAME, TABLE_NAME, Lists.newArrayList(part, part1),
            new EnvironmentContext());
  }

  @Test(expected = InvalidOperationException.class)
  @ConditionalIgnoreOnSessionHiveMetastoreClient
  public void testAlterPartitionsWithEnvironmentCtxBogusCatalogName() throws Exception {
    createTable4PartColsParts(client);
    Partition part = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    client.alter_partitions("nosuch", DB_NAME, TABLE_NAME, Lists.newArrayList(part), new EnvironmentContext(),
        null, -1);
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionsWithEnvironmentCtxNoDbName() throws Exception {
    createTable4PartColsParts(client);
    Partition part = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    client.alter_partitions("", TABLE_NAME, Lists.newArrayList(part), new EnvironmentContext());
  }

  @Test
  public void testAlterPartitionsWithEnvironmentCtxNullDbName() throws Exception {
    createTable4PartColsParts(client);
    Partition part = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    try {
      client.alter_partitions(null, TABLE_NAME, Lists.newArrayList(part), new EnvironmentContext());
      Assert.fail("Expected exception");
    } catch (MetaException | TProtocolException ex) {
    }
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterPartitionsWithEnvironmentCtxNoTblName() throws Exception {
    createTable4PartColsParts(client);
    Partition part = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    client.alter_partitions(DB_NAME, "", Lists.newArrayList(part), new EnvironmentContext());
  }

  @Test
  public void testAlterPartitionsWithEnvironmentCtxNullTblName() throws Exception {
    createTable4PartColsParts(client);
    Partition part = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1).get(0);
    try {
      client.alter_partitions(DB_NAME, null, Lists.newArrayList(part), new EnvironmentContext());
      Assert.fail("didn't throw");
    } catch (MetaException | TProtocolException ex) {
      // By design.
    }
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
    } catch (NullPointerException | TTransportException | TProtocolException e) {
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

  @Test
  public void testRenamePartitionNullOldPartList() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> oldParts = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);

    Partition partToRename = oldParts.get(3);
    partToRename.setValues(Lists.newArrayList("2018", "01", "16"));
    try {
      client.renamePartition(DB_NAME, TABLE_NAME, null, partToRename);
      Assert.fail("should throw");
    } catch (InvalidOperationException | TProtocolException ex) {
    }
  }

  @Test
  public void testRenamePartitionNullNewPart() throws Exception {
    try {
      List<List<String>> oldValues = createTable4PartColsParts(client);
      List<Partition> oldParts = client.listPartitions(DB_NAME, TABLE_NAME, (short) -1);

      Partition partToRename = oldParts.get(3);
      partToRename.setValues(Lists.newArrayList("2018", "01", "16"));
      client.renamePartition(DB_NAME, TABLE_NAME, oldValues.get(3), null);
    } catch (NullPointerException | TProtocolException e) {
    }
  }

  @Test(expected = InvalidOperationException.class)
  @ConditionalIgnoreOnSessionHiveMetastoreClient
  public void testRenamePartitionBogusCatalogName() throws Exception {
    List<List<String>> oldValues = createTable4PartColsParts(client);
    List<Partition> oldParts = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);

    Partition partToRename = oldParts.get(3);
    partToRename.setValues(Lists.newArrayList("2018", "01", "16"));
    client.renamePartition("nosuch", DB_NAME, TABLE_NAME, oldValues.get(3), partToRename, null);
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

  @Test
  public void testRenamePartitionNullDbName() throws Exception {
    List<List<String>> oldValues = createTable4PartColsParts(client);
    List<Partition> oldParts = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);

    Partition partToRename = oldParts.get(3);
    partToRename.setValues(Lists.newArrayList("2018", "01", "16"));
    try {
      client.renamePartition(null, TABLE_NAME, oldValues.get(3), partToRename);
      Assert.fail("should throw");
    } catch (MetaException | TProtocolException ex) {
    }
  }

  @Test
  public void testRenamePartitionNullTblName() throws Exception {
    List<List<String>> oldValues = createTable4PartColsParts(client);
    List<Partition> oldParts = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);

    Partition partToRename = oldParts.get(3);
    partToRename.setValues(Lists.newArrayList("2018", "01", "16"));
    try {
      client.renamePartition(DB_NAME, null, oldValues.get(3), partToRename);
      Assert.fail("should throw");
    } catch (MetaException | TProtocolException ex) {
    }
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
