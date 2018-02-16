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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.IndexBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.thrift.TException;
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
 * Tests for getting and listing indexes.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestGetListIndexes {
  // Needed until there is no junit release with @BeforeParam, @AfterParam (junit 4.13)
  // https://github.com/junit-team/junit4/commit/1bf8438b65858565dbb64736bfe13aae9cfc1b5a
  // Then we should remove our own copy
  private static Set<AbstractMetaStoreService> metaStoreServices = null;
  private AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;

  private static final String DB_NAME_1 = "testindexdb_1";
  private static final String DB_NAME_2 = "testindexdb_2";
  private static final String ORIG_TABLE_NAME_1 = "testindextable_1";
  private static final String ORIG_TABLE_NAME_2 = "testindextable_2";
  private static final String ORIG_TABLE_NAME_3 = "testindextable_3";
  private static final String INDEX_NAME_1 = "testindexname_1";
  private static final String INDEX_NAME_2 = "testindexname_2";
  private static final String INDEX_NAME_3 = "testindexname_3";
  private static final String INDEX_NAME_4 = "testindexname_4";
  private static final String INDEX_NAME_5 = "testindexname_4";
  private static final Index[] INDEXES = new Index[5];
  private static final short MAX = -1;

  @Parameterized.Parameters(name = "{0}")
  public static List<Object[]> getMetaStoreToTest() throws Exception {
    List<Object[]> result = MetaStoreFactoryForTests.getMetaStores();
    metaStoreServices = result.stream()
        .map(test -> (AbstractMetaStoreService)test[1])
        .collect(Collectors.toSet());
    return result;
  }

  public TestGetListIndexes(String name, AbstractMetaStoreService metaStore) throws Exception {
    this.metaStore = metaStore;
    this.metaStore.start();
  }

  // Needed until there is no junit release with @BeforeParam, @AfterParam (junit 4.13)
  // https://github.com/junit-team/junit4/commit/1bf8438b65858565dbb64736bfe13aae9cfc1b5a
  // Then we should move this to @AfterParam
  @AfterClass
  public static void stopMetaStores() throws Exception {
    for (AbstractMetaStoreService metaStoreService : metaStoreServices) {
      metaStoreService.stop();
    }
  }

  @Before
  public void setUp() throws Exception {
    // Get new client
    client = metaStore.getClient();

    // Clean up the database
    client.dropDatabase(DB_NAME_1, true, true, true);
    client.dropDatabase(DB_NAME_2, true, true, true);
    metaStore.cleanWarehouseDirs();

    createDB(DB_NAME_1);
    createDB(DB_NAME_2);

    Table origTable1 = createTable(DB_NAME_1, ORIG_TABLE_NAME_1);
    Table origTable2 = createTable(DB_NAME_1, ORIG_TABLE_NAME_2);
    Table origTable3 = createTable(DB_NAME_2, ORIG_TABLE_NAME_1);
    createTable(DB_NAME_1, ORIG_TABLE_NAME_3);

    INDEXES[0] = createIndex(origTable1, INDEX_NAME_1);
    INDEXES[1] = createIndex(origTable1, INDEX_NAME_2);
    INDEXES[2] = createIndex(origTable1, INDEX_NAME_3);
    INDEXES[3] = createIndex(origTable2, INDEX_NAME_4);
    INDEXES[4] = createIndex(origTable3, INDEX_NAME_5);
  }

  @After
  public void tearDown() throws Exception {
    try {
      for (Index index : INDEXES) {
        client.dropIndex(index.getDbName(), index.getOrigTableName(), index.getIndexName(), true);
      }

      if (client != null) {
        client.close();
      }
    } finally {
      client = null;
    }
  }

  // Get index tests

  public void testGetIndex() throws Exception {

    Index indexToGet = INDEXES[0];
    Index index = client.getIndex(indexToGet.getDbName(), indexToGet.getOrigTableName(),
        indexToGet.getIndexName());
    Assert.assertNotNull(index);
    Assert.assertEquals(indexToGet, index);

    indexToGet = INDEXES[4];
    index = client.getIndex(indexToGet.getDbName(), indexToGet.getOrigTableName(),
        indexToGet.getIndexName());
    Assert.assertNotNull(index);
    Assert.assertEquals(indexToGet, index);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetNonExistingIndex() throws Exception {

    Index index = INDEXES[0];
    client.getIndex(index.getDbName(), index.getOrigTableName(), "nonexisingindex");
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetIndexNonExistingTable() throws Exception {

    Index index = INDEXES[0];
    client.getIndex(index.getDbName(), "nonexistingtable", index.getIndexName());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetIndexNonExistingDatabase() throws Exception {

    Index index = INDEXES[0];
    client.getIndex("nonexistingdb", index.getOrigTableName(), index.getIndexName());
  }

  @Test(expected = MetaException.class)
  public void testGetIndexNullName() throws Exception {

    Index index = INDEXES[0];
    client.getIndex(index.getDbName(), index.getOrigTableName(), null);
  }

  @Test(expected = MetaException.class)
  public void testGetIndexNullTableName() throws Exception {

    Index index = INDEXES[0];
    client.getIndex(index.getDbName(), null, index.getIndexName());
  }

  @Test(expected = MetaException.class)
  public void testGetIndexNullDBName() throws Exception {

    Index index = INDEXES[0];
    client.getIndex(null, index.getOrigTableName(), index.getIndexName());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetIndexEmptyName() throws Exception {
    Index index = INDEXES[0];
    client.getIndex(index.getDbName(), index.getOrigTableName(), "");
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetIndexEmptyTableName() throws Exception {
    Index index = INDEXES[0];
    client.getIndex(index.getDbName(), "", index.getIndexName());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetIndexEmptyDBName() throws Exception {
    Index index = INDEXES[0];
    client.getIndex("", index.getOrigTableName(), index.getIndexName());
  }

  // List index tests

  @Test
  public void testListIndexes() throws Exception {

    List<Index> indexes = client.listIndexes(DB_NAME_1, ORIG_TABLE_NAME_1, MAX);
    Assert.assertNotNull(indexes);
    Assert.assertEquals(3, indexes.size());
    for (Index index : indexes) {
      if (INDEX_NAME_1.equals(index.getIndexName())) {
        Assert.assertEquals(INDEXES[0], index);
      } else if (INDEX_NAME_2.equals(index.getIndexName())) {
        Assert.assertEquals(INDEXES[1], index);
      } else {
        Assert.assertEquals(INDEXES[2], index);
      }
    }

    indexes = client.listIndexes(DB_NAME_1, ORIG_TABLE_NAME_2, MAX);
    Assert.assertNotNull(indexes);
    Assert.assertEquals(1, indexes.size());
    Assert.assertEquals(INDEXES[3], indexes.get(0));

    indexes = client.listIndexes(DB_NAME_2, ORIG_TABLE_NAME_1, MAX);
    Assert.assertNotNull(indexes);
    Assert.assertEquals(1, indexes.size());
    Assert.assertEquals(INDEXES[4], indexes.get(0));
  }

  @Test
  public void testListIndexesEmptyList() throws Exception {

    List<Index> indexes = client.listIndexes(DB_NAME_1, ORIG_TABLE_NAME_3, MAX);
    Assert.assertNotNull(indexes);
    Assert.assertTrue(indexes.isEmpty());
  }

  @Test
  public void testListIndexesInvalidDb() throws Exception {

    List<Index> indexes = client.listIndexes("nonexistingdb", INDEXES[0].getOrigTableName(), MAX);
    Assert.assertNotNull(indexes);
    Assert.assertTrue(indexes.isEmpty());
  }

  @Test
  public void testListIndexesInvalidTable() throws Exception {

    List<Index> indexes = client.listIndexes(INDEXES[0].getDbName(), "nonexsitingtable", MAX);
    Assert.assertNotNull(indexes);
    Assert.assertTrue(indexes.isEmpty());
  }

  @Test(expected = MetaException.class)
  public void testListIndexesNullDb() throws Exception {

    client.listIndexes(null, INDEXES[0].getOrigTableName(), MAX);
  }

  @Test(expected = MetaException.class)
  public void testListIndexesNullTable() throws Exception {

    client.listIndexes(INDEXES[0].getDbName(), null, MAX);
  }

  @Test
  public void testListIndexesEmptyDb() throws Exception {

    List<Index> indexes = client.listIndexes("", INDEXES[0].getOrigTableName(), MAX);
    Assert.assertNotNull(indexes);
    Assert.assertTrue(indexes.isEmpty());
  }

  @Test
  public void testListIndexesEmptyTable() throws Exception {

    List<Index> indexes = client.listIndexes(INDEXES[0].getDbName(), "", MAX);
    Assert.assertNotNull(indexes);
    Assert.assertTrue(indexes.isEmpty());
  }

  @Test
  public void testListIndexesWithDifferentNums() throws Exception {

    Index index = INDEXES[0];
    checkListIndexes(index.getDbName(), index.getOrigTableName(), (short) 2);
    checkListIndexes(index.getDbName(), index.getOrigTableName(), (short) 1);
    checkListIndexes(index.getDbName(), index.getOrigTableName(), (short) 0);
    checkListIndexes(index.getDbName(), index.getOrigTableName(), (short) -1);
  }

  // List index names tests

  @Test
  public void testListIndexNames() throws Exception {

    List<String> indexNames = client.listIndexNames(DB_NAME_1, ORIG_TABLE_NAME_1, MAX);
    Assert.assertNotNull(indexNames);
    Assert.assertEquals(3, indexNames.size());
    List<String> expectedIndexNames = Lists.newArrayList(INDEXES[0].getIndexName(),
        INDEXES[1].getIndexName(), INDEXES[2].getIndexName());
    Assert.assertEquals(expectedIndexNames, indexNames);

    indexNames = client.listIndexNames(DB_NAME_1, ORIG_TABLE_NAME_2, MAX);
    Assert.assertNotNull(indexNames);
    Assert.assertEquals(1, indexNames.size());
    Assert.assertEquals(INDEXES[3].getIndexName(), indexNames.get(0));

    indexNames = client.listIndexNames(DB_NAME_2, ORIG_TABLE_NAME_1, MAX);
    Assert.assertNotNull(indexNames);
    Assert.assertEquals(1, indexNames.size());
    Assert.assertEquals(INDEXES[4].getIndexName(), indexNames.get(0));
  }

  @Test
  public void testListIndexNamesEmptyList() throws Exception {

    List<String> indexes = client.listIndexNames(DB_NAME_1, ORIG_TABLE_NAME_3, MAX);
    Assert.assertNotNull(indexes);
    Assert.assertTrue(indexes.isEmpty());
  }

  @Test
  public void testListIndexNamesInvalidDb() throws Exception {

    List<String> indexes =
        client.listIndexNames("nonexistingdb", INDEXES[0].getOrigTableName(), MAX);
    Assert.assertNotNull(indexes);
    Assert.assertTrue(indexes.isEmpty());
  }

  @Test
  public void testListIndexNamesInvalidTable() throws Exception {

    List<String> indexes = client.listIndexNames(INDEXES[0].getDbName(), "nonexsitingtable", MAX);
    Assert.assertNotNull(indexes);
    Assert.assertTrue(indexes.isEmpty());
  }

  @Test(expected = MetaException.class)
  public void testListIndexNamesNullDb() throws Exception {

    client.listIndexNames(null, INDEXES[0].getOrigTableName(), MAX);
  }

  @Test(expected = MetaException.class)
  public void testListIndexNamesNullTable() throws Exception {

    client.listIndexNames(INDEXES[0].getDbName(), null, MAX);
  }

  @Test
  public void testListIndexNamesEmptyDb() throws Exception {

    List<String> indexes = client.listIndexNames("", INDEXES[0].getOrigTableName(), MAX);
    Assert.assertNotNull(indexes);
    Assert.assertTrue(indexes.isEmpty());
  }

  @Test
  public void testListIndexNamesEmptyTable() throws Exception {

    List<String> indexes = client.listIndexNames(INDEXES[0].getDbName(), "", MAX);
    Assert.assertNotNull(indexes);
    Assert.assertTrue(indexes.isEmpty());
  }

  @Test
  public void testListIndexNamesWithDifferentNums() throws Exception {

    Index index = INDEXES[0];
    checkListIndexNames(index.getDbName(), index.getOrigTableName(), (short) 2);
    checkListIndexNames(index.getDbName(), index.getOrigTableName(), (short) 1);
    checkListIndexNames(index.getDbName(), index.getOrigTableName(), (short) 0);
    checkListIndexNames(index.getDbName(), index.getOrigTableName(), (short) -1);
  }

  // Helper methods

  private Table createTable(String dbName, String tableName) throws Exception {
    Table table = buildTable(dbName, tableName, null);
    client.createTable(table);
    return table;
  }

  private Table buildIndexTable(String dbName, String tableName) throws Exception {
    return buildTable(dbName, tableName, TableType.INDEX_TABLE);
  }

  private Table buildTable(String dbName, String tableName, TableType tableType) throws Exception {
    TableBuilder tableBuilder = new TableBuilder()
        .setDbName(dbName)
        .setTableName(tableName)
        .addCol("id", "int", "test col id")
        .addCol("value", "string", "test col value")
        .addStorageDescriptorParam("testSDParamKey", "testSDParamValue")
        .setSerdeName(tableName)
        .setStoredAsSubDirectories(false)
        .addSerdeParam("testSerdeParamKey", "testSerdeParamValue")
        .setLocation(metaStore.getWarehouseRoot() + "/" + tableName);

    if (tableType != null){
      tableBuilder.setType(tableType.name());
    }

    return tableBuilder.build();
  }

  private Index createIndex(Table origTable, String indexName) throws Exception {

    String dbName = origTable.getDbName();
    String origTableName = origTable.getTableName();
    String indexTableName = origTableName + "__" + indexName + "__";
    Index index = buildIndex(dbName, origTableName, indexName, indexTableName);
    client.createIndex(index, buildTable(dbName, indexTableName, TableType.INDEX_TABLE));
    return client.getIndex(dbName, origTableName, indexName);
  }

  private Index buildIndex(String dbName, String origTableName, String indexName,
      String indexTableName) throws MetaException {
    Index index = new IndexBuilder()
        .setDbName(dbName)
        .setTableName(origTableName)
        .setIndexName(indexName)
        .setIndexTableName(indexTableName)
        .addCol("id", "int", "test col id")
        .addCol("value", "string", "test col value")
        .addIndexParam("test_get_index_param_key", "test_get_index_param_value")
        .setDeferredRebuild(false)
        .build();
    return index;
  }

  private void createDB(String dbName) throws TException {
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .build();
    client.createDatabase(db);
  }

  private void checkListIndexNames(String dbName, String origTableName, short num)
      throws Exception {
    List<String> indexNames = client.listIndexNames(dbName, origTableName, num);
    Assert.assertNotNull(indexNames);
    // TODO: The num parameter doesn't have any effect
    Assert.assertEquals(3, indexNames.size());
  }

  private void checkListIndexes(String dbName, String origTableName, short num) throws Exception {
    List<Index> indexes = client.listIndexes(dbName, origTableName, num);
    Assert.assertNotNull(indexes);
    // TODO: The num parameter doesn't have any effect
    Assert.assertEquals(3, indexes.size());
  }
}
