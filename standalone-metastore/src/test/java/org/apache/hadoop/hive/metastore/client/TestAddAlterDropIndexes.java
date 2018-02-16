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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.IndexBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests for creating, altering and dropping indexes.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestAddAlterDropIndexes {
  // Needed until there is no junit release with @BeforeParam, @AfterParam (junit 4.13)
  // https://github.com/junit-team/junit4/commit/1bf8438b65858565dbb64736bfe13aae9cfc1b5a
  // Then we should remove our own copy
  private static Set<AbstractMetaStoreService> metaStoreServices = null;
  private AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;

  private static final String DB_NAME = "testindexdb";
  private static final String TABLE_NAME = "testindextable";
  private static final String INDEX_NAME = "testcreateindex";
  private static final String INDEX_TABLE_NAME = TABLE_NAME + "__" + INDEX_NAME + "__";
  private static final short MAX = -1;

  @Parameterized.Parameters(name = "{0}")
  public static List<Object[]> getMetaStoreToTest() throws Exception {
    List<Object[]> result = MetaStoreFactoryForTests.getMetaStores();
    metaStoreServices = result.stream()
                            .map(test -> (AbstractMetaStoreService)test[1])
                            .collect(Collectors.toSet());
    return result;
  }

  public TestAddAlterDropIndexes(String name, AbstractMetaStoreService metaStore) throws Exception {
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

  @Test
  public void testCreateGetAndDropIndex() throws Exception {

    String indexHandlerName = "TestIndexHandlerClass";
    String inputFormatName = "TestInputFormat";
    String outputFormatName = "TestOutputFormat";
    String indexParamKey = "indexParamKey";
    String indexParamValue = "indexParamValue";
    String sdParamKey = "indexSdParamKey";
    String sdParamValue = "indexSdParamValue";
    int createTime = (int) (System.currentTimeMillis() / 1000);
    Map<String, String> params = new HashMap<String, String>();
    params.put(indexParamKey, indexParamValue);

    createTable(DB_NAME, TABLE_NAME);

    Index index = new IndexBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .setIndexName(INDEX_NAME)
        .setIndexTableName(INDEX_TABLE_NAME)
        .setCreateTime(createTime)
        .setLastAccessTime(createTime)
        .addCol("id", "int", "test col id")
        .addCol("value", "string", "test col value")
        .setDeferredRebuild(false)
        .setIndexParams(params)
        .setHandlerClass(indexHandlerName)
        .setInputFormat(inputFormatName)
        .setOutputFormat(outputFormatName)
        .setSerdeName(INDEX_TABLE_NAME)
        .addStorageDescriptorParam(sdParamKey, sdParamValue)
        .build();
    client.createIndex(index, buildIndexTable(DB_NAME, INDEX_TABLE_NAME));

    Index resultIndex = client.getIndex(DB_NAME, TABLE_NAME, INDEX_NAME);
    Assert.assertNotNull(resultIndex);
    Assert.assertEquals(DB_NAME, resultIndex.getDbName());
    Assert.assertEquals(TABLE_NAME, resultIndex.getOrigTableName());
    Assert.assertEquals(INDEX_NAME, resultIndex.getIndexName());
    Assert.assertEquals(INDEX_TABLE_NAME, resultIndex.getIndexTableName());
    Assert.assertEquals(createTime, resultIndex.getLastAccessTime());
    Assert.assertNotNull(resultIndex.getParameters());
    Assert.assertEquals(indexParamValue, resultIndex.getParameters().get(indexParamKey));
    Assert.assertEquals(indexHandlerName, resultIndex.getIndexHandlerClass());
    StorageDescriptor sd = resultIndex.getSd();
    Assert.assertNotNull(sd);
    Map<String, FieldSchema> cols = new HashMap<>();
    for (FieldSchema col : sd.getCols()) {
      cols.put(col.getName(), col);
    }
    Assert.assertEquals(2, cols.size());
    Assert.assertNotNull(cols.get("id"));
    Assert.assertNotNull(cols.get("value"));
    Assert.assertEquals("int", cols.get("id").getType());
    Assert.assertEquals("string", cols.get("value").getType());
    Assert.assertEquals(inputFormatName, sd.getInputFormat());
    Assert.assertEquals(outputFormatName, sd.getOutputFormat());
    Assert.assertEquals(INDEX_TABLE_NAME, sd.getSerdeInfo().getName());
    Assert.assertNotNull(sd.getParameters());
    Assert.assertEquals(sdParamValue, sd.getParameters().get(sdParamKey));

    Table indexTable = client.getTable(DB_NAME, INDEX_TABLE_NAME);
    Assert.assertNotNull(indexTable);
    Assert.assertTrue(metaStore.isPathExists(new Path(indexTable.getSd().getLocation())));

    client.dropIndex(DB_NAME, TABLE_NAME, INDEX_NAME, true);
    List<String> indexNames = client.listIndexNames(DB_NAME, TABLE_NAME, MAX);
    Assert.assertNotNull(indexNames);
    Assert.assertEquals(0, indexNames.size());
    List<String> tableNames = client.listTableNamesByFilter(DB_NAME, "", MAX);
    Assert.assertNotNull(tableNames);
    Assert.assertFalse(tableNames.contains(INDEX_TABLE_NAME));
    Assert.assertFalse(metaStore.isPathExists(new Path(indexTable.getSd().getLocation())));
  }

  // Create index tests

  @Test
  public void testCreateIndexesSameNameDifferentOrigAndIndexTables() throws Exception {

    String origTableName1 = TABLE_NAME + "1";
    String origTableName2 = TABLE_NAME + "2";
    String indexNameTableName1 = origTableName1 + "__" + INDEX_NAME;
    String indexNameTableName2 = origTableName2 + "__" + INDEX_NAME;
    Table origTable1 = createTable(DB_NAME, origTableName1);
    Table origTable2 = createTable(DB_NAME, origTableName2);

    createIndex(DB_NAME, origTable1, INDEX_NAME, buildIndexTable(DB_NAME, indexNameTableName1));
    createIndex(DB_NAME, origTable2, INDEX_NAME, buildIndexTable(DB_NAME, indexNameTableName2));

    verifyIndex(DB_NAME, origTableName1, INDEX_NAME, indexNameTableName1);
    verifyIndex(DB_NAME, origTableName2, INDEX_NAME, indexNameTableName2);

    client.dropIndex(DB_NAME, origTableName1, INDEX_NAME, true);
    client.dropIndex(DB_NAME, origTableName2, INDEX_NAME, true);
  }

  @Test
  public void testCreateIndexStrangeCharsInName() throws Exception {

    // TODO: Special character should not be allowed in index name.
    Table origTable = createTable(DB_NAME, TABLE_NAME);
    Table indexTable = buildIndexTable(DB_NAME, INDEX_TABLE_NAME);
    String indexName = "§±!;@#$%^&*()_+}{|-=[]\':|?><";
    createIndex(DB_NAME, origTable, indexName, indexTable);
    verifyIndex(DB_NAME, TABLE_NAME, indexName, INDEX_TABLE_NAME);
    client.dropIndex(DB_NAME, TABLE_NAME, indexName, true);
  }

  @Test
  public void testCreateIndexWithUpperCaseName() throws Exception {

    Table origTable = createTable(DB_NAME, TABLE_NAME);
    String indexName = "UPPERCASE_INDEX_NAME";
    String indexTableName = "UPPERCASE_INDEX_TABLE_NAME";
    Table indexTable = buildIndexTable(DB_NAME, indexTableName);
    createIndex(DB_NAME, origTable, indexName, indexTable);
    verifyIndex(DB_NAME, TABLE_NAME, indexName.toLowerCase(), indexTableName.toLowerCase());
    client.dropIndex(DB_NAME, TABLE_NAME, indexName, true);
  }

  @Test(expected = AlreadyExistsException.class)
  public void testCreateIndexesWithSameOrigAndIndexTable() throws Exception {

    Table origTable = createTable(DB_NAME, TABLE_NAME);
    Table indexTable = buildIndexTable(DB_NAME, INDEX_TABLE_NAME);
    createIndex(DB_NAME, origTable, INDEX_NAME, indexTable);
    createIndex(DB_NAME, origTable, INDEX_NAME, indexTable);
    checkIfIndexExists(DB_NAME, TABLE_NAME, INDEX_NAME);
    client.dropIndex(DB_NAME, TABLE_NAME, INDEX_NAME, true);
  }

  @Test(expected = AlreadyExistsException.class)
  public void testCreateIndexesWithSameNameAndOrigTableDifferentIndexTable() throws Exception {

    Table origTable = createTable(DB_NAME, TABLE_NAME);
    String indexTableName1 = origTable.getTableName() + "__" + INDEX_NAME + "_1";
    String indexTableName2 = origTable.getTableName() + "__" + INDEX_NAME + "_2";
    createIndex(DB_NAME, origTable, INDEX_NAME, buildIndexTable(DB_NAME, indexTableName1));
    createIndex(DB_NAME, origTable, INDEX_NAME, buildIndexTable(DB_NAME, indexTableName2));
    checkIfIndexExists(DB_NAME, TABLE_NAME, INDEX_NAME);
    client.dropIndex(DB_NAME, TABLE_NAME, INDEX_NAME, true);
  }

  @Test(expected = InvalidObjectException.class)
  public void testCreateIndexWithExistingIndexTable() throws Exception {

    Table origTable = createTable(DB_NAME, TABLE_NAME);
    Table indexTable = createTable(DB_NAME, INDEX_TABLE_NAME);
    createIndex(DB_NAME, origTable, INDEX_NAME, indexTable);
    checkIfIndexListEmpty(DB_NAME, TABLE_NAME);
  }

  @Test(expected = MetaException.class)
  public void testCreateIndexNullIndexTableName() throws Exception {

    Table origTable = createTable(DB_NAME, TABLE_NAME);
    Table indexTable = buildIndexTable(DB_NAME, null);
    createIndex(DB_NAME, origTable, INDEX_NAME, indexTable);
    checkIfIndexExists(DB_NAME, TABLE_NAME, INDEX_NAME);
  }

  @Test(expected = InvalidObjectException.class)
  public void testCreateIndexEmptyIndexTableName() throws Exception {

    Table origTable = createTable(DB_NAME, TABLE_NAME);
    Table indexTable = buildIndexTable(DB_NAME, "");
    createIndex(DB_NAME, origTable, INDEX_NAME, indexTable);
    checkIfIndexListEmpty(DB_NAME, TABLE_NAME);
  }

  @Test(expected = InvalidObjectException.class)
  public void testCreateIndexesWithSameIndexTable() throws Exception {

    String origTableName1 = "testindextable1";
    String origTableName2 = "testindextable2";
    String indexName1 = "testindex1";
    String indexName2 = "testindex2";
    String indexTableName = origTableName1 + "__" + indexName1;
    Table origTable1 = createTable(DB_NAME, origTableName1);
    Table origTable2 = createTable(DB_NAME, origTableName2);
    Table indexTable = buildIndexTable(DB_NAME, indexTableName);

    createIndex(DB_NAME, origTable1, indexName1, indexTable);
    createIndex(DB_NAME, origTable2, indexName2, indexTable);

    checkIfIndexExists(DB_NAME, origTableName1, indexName1);
    checkIfIndexListEmpty(DB_NAME, origTableName2);
    client.dropIndex(DB_NAME, origTableName1, indexName1, true);
  }

  @Test(expected = InvalidObjectException.class)
  public void testCreateIndexWithNonExistingDB() throws Exception {

    Table origTable = createTable(DB_NAME, TABLE_NAME);
    Table indexTable = buildIndexTable(DB_NAME, INDEX_TABLE_NAME);
    createIndex("nonexistingdb", origTable, INDEX_NAME, indexTable);
    checkIfIndexListEmpty(DB_NAME, TABLE_NAME);
  }

  @Test(expected = MetaException.class)
  public void testCreateIndexWithNullDBName() throws Exception {

    Table origTable = createTable(DB_NAME, TABLE_NAME);
    Table indexTable = buildIndexTable(DB_NAME, INDEX_TABLE_NAME);
    createIndex(null, origTable, INDEX_NAME, indexTable);
  }

  @Test(expected = InvalidObjectException.class)
  public void testCreateIndexNonExistingOrigTable() throws Exception {

    Table indexTable = buildIndexTable(DB_NAME, INDEX_TABLE_NAME);
    Index index = buildIndexWithDefaultValues();
    index.setOrigTableName("nonexistingtable");
    client.createIndex(index, indexTable);
  }

  @Test(expected = MetaException.class)
  public void testCreateIndexNullOrigTable() throws Exception {

    Table indexTable = buildIndexTable(DB_NAME, INDEX_TABLE_NAME);
    Index index = buildIndexWithDefaultValues();
    index.setOrigTableName(null);
    client.createIndex(index, indexTable);
  }

  @Test
  public void testCreateIndexNullIndex() throws Exception {
    // TODO: NPE should not be thrown.
    Table indexTable = buildIndexTable(DB_NAME, INDEX_TABLE_NAME);
    try {
      client.createIndex(null, indexTable);
      Assert.fail("TTransportException or NullPointerException should have happened");
    } catch (TTransportException | NullPointerException e) {
      // Expected exception
    }
  }

  @Test
  public void testCreateIndexNullIndexTable() throws Exception {

    createTable(DB_NAME, TABLE_NAME);
    Index index = new IndexBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .setIndexName(INDEX_NAME)
        .setIndexTableName(null)
        .addCol("id", "int", "test col id")
        .addCol("value", "string", "test col value")
        .setDeferredRebuild(false)
        .build();
    client.createIndex(index, null);
  }

  @Test(expected = InvalidObjectException.class)
  public void testCreateIndexWithEmptyOrigTable() throws Exception {

    Table indexTable = buildIndexTable(DB_NAME, INDEX_TABLE_NAME);
    Index index = buildIndexWithDefaultValues();
    index.setOrigTableName("");
    client.createIndex(index, indexTable);
  }

  @Test(expected = InvalidObjectException.class)
  public void testCreateIndexWithEmptyDBName() throws Exception {

    createTable(DB_NAME, TABLE_NAME);
    Index index = buildIndexWithDefaultValues();
    index.setDbName("");
    client.createIndex(index, buildIndexTable(DB_NAME, INDEX_TABLE_NAME));
  }

  @Test(expected = MetaException.class)
  public void testCreateIndexNullIndexName() throws Exception {

    createTable(DB_NAME, TABLE_NAME);
    Index index = buildIndexWithDefaultValues();
    index.setIndexTableName(null);
    client.createIndex(index, buildIndexTable(DB_NAME, INDEX_TABLE_NAME));
  }

  @Test
  public void testCreateIndexEmptyIndexName() throws Exception {

    Table origTable = createTable(DB_NAME, TABLE_NAME);
    Table indexTable = buildIndexTable(DB_NAME, INDEX_TABLE_NAME);
    createIndex(DB_NAME, origTable, "", indexTable);
    checkIfIndexExists(DB_NAME, TABLE_NAME, "");
    client.dropIndex(DB_NAME, TABLE_NAME, "", true);
  }

  @Test
  public void testCreateIndexWithDifferentIndexTableName() throws Exception {

    createTable(DB_NAME, TABLE_NAME);
    Index index = buildIndexWithDefaultValues();
    index.setIndexTableName("differentindextablename");
    Table indexTable = buildIndexTable(DB_NAME, INDEX_TABLE_NAME);
    client.createIndex(index, indexTable);
    checkIfIndexListEmpty(DB_NAME, TABLE_NAME);
    List<String> tableNames = client.listTableNamesByFilter(DB_NAME, "", MAX);
    Assert.assertNotNull(tableNames);
    Assert.assertEquals(1, tableNames.size());
    Assert.assertEquals(TABLE_NAME, tableNames.get(0));
    Assert.assertTrue(metaStore.isPathExists(new Path(indexTable.getSd().getLocation())));
  }

  @Test
  public void testCreateIndexWithDifferentDBInIndexTableAndIndex() throws Exception {

    createTable(DB_NAME, TABLE_NAME);
    String dbName = "second_index_db";
    createDB(dbName);
    Index index = buildIndexWithDefaultValues();
    Table indexTable = buildIndexTable(dbName, INDEX_TABLE_NAME);
    Assert.assertFalse(metaStore.isPathExists(new Path(indexTable.getSd().getLocation())));
    client.createIndex(index, indexTable);
    checkIfIndexListEmpty(DB_NAME, TABLE_NAME);
    checkIfIndexListEmpty(dbName, TABLE_NAME);
    List<String> tableNames = client.listTableNamesByFilter(DB_NAME, "", MAX);
    Assert.assertFalse(tableNames.contains(INDEX_TABLE_NAME));
    tableNames = client.listTableNamesByFilter(dbName, "", MAX);
    Assert.assertFalse(tableNames.contains(INDEX_TABLE_NAME));
    Assert.assertTrue(metaStore.isPathExists(new Path(indexTable.getSd().getLocation())));
    client.dropDatabase(dbName);
  }

  @Test
  public void testCreateIndexDifferentColsInIndexAndIndexTable() throws Exception {

    createTable(DB_NAME, TABLE_NAME);
    Table indexTable = buildIndexTable(DB_NAME, INDEX_TABLE_NAME);
    Index index = new IndexBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .setIndexName(INDEX_NAME)
        .setIndexTableName(INDEX_TABLE_NAME)
        .addCol("test_name", "string", "test col name")
        .setDeferredRebuild(false)
        .build();
    client.createIndex(index, indexTable);

    Index resultIndex = client.getIndex(DB_NAME, TABLE_NAME, INDEX_NAME);
    Assert.assertNotNull(resultIndex);
    Assert.assertEquals(INDEX_NAME, resultIndex.getIndexName());
    Assert.assertEquals(TABLE_NAME, resultIndex.getOrigTableName());
    Assert.assertEquals(INDEX_TABLE_NAME, resultIndex.getIndexTableName());
    List<FieldSchema> colsInIndex = resultIndex.getSd().getCols();
    Assert.assertEquals(1, colsInIndex.size());
    Assert.assertEquals("test_name", colsInIndex.get(0).getName());

    Table indexTableResult = client.getTable(DB_NAME, INDEX_TABLE_NAME);
    Assert.assertNotNull(indexTableResult);
    List<FieldSchema> colsInIndexTable = indexTableResult.getSd().getCols();
    Assert.assertNotNull(colsInIndexTable);
    Assert.assertEquals(2, colsInIndexTable.size());
    Assert.assertEquals("id", colsInIndexTable.get(0).getName());
    Assert.assertEquals("value", colsInIndexTable.get(1).getName());
    client.dropIndex(DB_NAME, TABLE_NAME, INDEX_NAME, true);
  }

  @Test
  public void testCreateIndexWithNullSd() throws Exception {

    createTable(DB_NAME, TABLE_NAME);
    Index index = buildIndexWithDefaultValues();
    index.setSd(null);
    client.createIndex(index, buildIndexTable(DB_NAME, INDEX_TABLE_NAME));
    checkIfIndexListEmpty(DB_NAME, TABLE_NAME);
  }

  // Drop index tests
  @Test(expected = NoSuchObjectException.class)
  public void testDropIndexInvalidDB() throws Exception {

    createIndex(INDEX_NAME, TABLE_NAME);
    client.dropIndex("nonexistingdb", TABLE_NAME, INDEX_NAME, true);
    verifyIndex(DB_NAME, TABLE_NAME, INDEX_NAME, INDEX_TABLE_NAME);
    client.dropIndex(DB_NAME, TABLE_NAME, INDEX_NAME, true);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDropIndexInvalidTable() throws Exception {

    createIndex(INDEX_NAME, TABLE_NAME);
    client.dropIndex(DB_NAME, "wrongtablename", INDEX_NAME, true);
    verifyIndex(DB_NAME, TABLE_NAME, INDEX_NAME, INDEX_TABLE_NAME);
    client.dropIndex(DB_NAME, TABLE_NAME, INDEX_NAME, true);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDropIndexInvalidIndex() throws Exception {

    createIndex(INDEX_NAME, TABLE_NAME);
    client.dropIndex(DB_NAME, TABLE_NAME, "invalidindexname", true);
    verifyIndex(DB_NAME, TABLE_NAME, INDEX_NAME, INDEX_TABLE_NAME);
    client.dropIndex(DB_NAME, TABLE_NAME, INDEX_NAME, true);
  }

  @Test(expected = MetaException.class)
  public void testDropIndexNullDBName() throws Exception {

    client.dropIndex(null, TABLE_NAME, INDEX_NAME, true);
  }

  @Test(expected = MetaException.class)
  public void testDropIndexNullTableName() throws Exception {

    client.dropIndex(DB_NAME, null, INDEX_NAME, true);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDropIndexNullIndexName() throws Exception {

    client.dropIndex(DB_NAME, TABLE_NAME, null, true);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDropIndexEmptyDBName() throws Exception {

    client.dropIndex("", TABLE_NAME, INDEX_NAME, true);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDropIndexEmptyTableName() throws Exception {

    client.dropIndex(DB_NAME, "", INDEX_NAME, true);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDropIndexEmptyIndexName() throws Exception {

    client.dropIndex(DB_NAME, TABLE_NAME, "", true);
  }

  // Alter index tests

  @Test
  public void testAlterIndex() throws Exception {

    // Only index parameters are allowed to be altered
    String indexTableName = TABLE_NAME + "__" + INDEX_NAME + "__";
    int oldCreateTime = (int) (System.currentTimeMillis() / 1000);
    String oldHandlerClassName = "TestHandlerClass1";
    String oldLocation = "/index/test/path1";
    String oldInputFormat = "org.apache.hadoop.hive.ql.io.HiveInputFormat";
    String oldOutputFormat = "org.apache.hadoop.hive.ql.io.HiveOutputFormat";
    String oldIndexParamKey = "indexParamKey1";
    String oldIndexParamValue = "indexParamValue1";
    String oldIndexSdParamKey = "sdparamkey1";
    String oldIndexSdParamValue = "sdparamvalue1";
    Map<String, String> indexParams = new HashMap<String, String>();
    indexParams.put(oldIndexParamKey, oldIndexParamValue);

    createTable(DB_NAME, TABLE_NAME);

    Index index = new IndexBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .setIndexName(INDEX_NAME)
        .setIndexTableName(indexTableName)
        .setCreateTime(oldCreateTime)
        .setLastAccessTime(oldCreateTime)
        .addCol("id", "int", "test col id")
        .addCol("value", "string", "test col value")
        .setDeferredRebuild(false)
        .setIndexParams(indexParams)
        .setHandlerClass(oldHandlerClassName)
        .setLocation(oldLocation)
        .setInputFormat(oldInputFormat)
        .setOutputFormat(oldOutputFormat)
        .addStorageDescriptorParam(oldIndexSdParamKey, oldIndexSdParamValue)
        .build();
    client.createIndex(index, buildIndexTable(DB_NAME, indexTableName));
    Index oldIndex = client.getIndex(DB_NAME, TABLE_NAME, INDEX_NAME);

    int newCreateTime = (int) (System.currentTimeMillis() / 1000);
    String newHandlerClassName = "TestHandlerClass2";
    String newLocation = "/index/test/path2";
    String newInputFormat = "NewInputFormat";
    String newOutputFormat = "NewOutputFormat";
    String newIndexParamValue = "indexParamValue2";
    String newIndexSdParamKey = "sdparamkey2";
    String newIndexSdParamValue = "sdparamvalue2";
    Map<String, String> newIndexParams = new HashMap<String, String>();
    newIndexParams.put(oldIndexParamKey, newIndexParamValue);

    Index newIndex = new IndexBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .setIndexName(INDEX_NAME)
        .setIndexTableName(indexTableName)
        .setCreateTime(newCreateTime)
        .setLastAccessTime(newCreateTime)
        .addCol("id", "int", "test col id")
        .setDeferredRebuild(true)
        .setIndexParams(newIndexParams)
        .setHandlerClass(newHandlerClassName)
        .setLocation(newLocation)
        .setInputFormat(newInputFormat)
        .setOutputFormat(newOutputFormat)
        .addStorageDescriptorParam(newIndexSdParamKey, newIndexSdParamValue)
        .build();
    client.alter_index(DB_NAME, TABLE_NAME, INDEX_NAME, newIndex);

    Index createdNewIndex = client.getIndex(DB_NAME, TABLE_NAME, INDEX_NAME);
    // Check if the index parameters are changed
    Map<String, String> params = createdNewIndex.getParameters();
    Assert.assertEquals(newIndexParamValue, params.get(oldIndexParamKey));
    // Reset the index parameters and compare the old and new indexes to
    // check that the other attributes remained the same.
    createdNewIndex.setParameters(oldIndex.getParameters());
    Assert.assertEquals(oldIndex, createdNewIndex);
    client.dropIndex(DB_NAME, TABLE_NAME, INDEX_NAME, true);
  }

  @Test(expected = MetaException.class)
  public void testAlterIndexNonExistingIndex() throws Exception {

    createTable(DB_NAME, TABLE_NAME);
    Index index = buildIndexWithDefaultValues();
    client.alter_index(DB_NAME, TABLE_NAME, INDEX_NAME, index);
  }

  @Test(expected = MetaException.class)
  public void testAlterIndexNonExistingDb() throws Exception {

    Index index = buildIndexWithDefaultValues();
    client.alter_index("nonexistingdb", TABLE_NAME, INDEX_NAME, index);
  }

  @Test(expected = MetaException.class)
  public void testAlterIndexNonExistingTable() throws Exception {

    Index index = buildIndexWithDefaultValues();
    client.alter_index(DB_NAME, TABLE_NAME, INDEX_NAME, index);
  }

  @Test(expected = MetaException.class)
  public void testAlterIndexNullIndexName() throws Exception {

    Index index = buildIndexWithDefaultValues();
    client.alter_index(DB_NAME, TABLE_NAME, null, index);
  }

  @Test(expected = MetaException.class)
  public void testAlterIndexNullDbName() throws Exception {

    Index index = buildIndexWithDefaultValues();
    client.alter_index(null, TABLE_NAME, INDEX_NAME, index);
  }

  @Test(expected = MetaException.class)
  public void testAlterIndexNullTableName() throws Exception {

    Index index = buildIndexWithDefaultValues();
    client.alter_index(DB_NAME, null, INDEX_NAME, index);
  }

  @Test
  public void testAlterIndexNullIndex() throws Exception {

    try {
      client.alter_index(DB_NAME, TABLE_NAME, INDEX_NAME, null);
      Assert.fail("Exception should have happened");
    } catch (TTransportException | NullPointerException e) {
      // TODO: NPE should not be thrown.
    }
  }

  @Test(expected = MetaException.class)
  public void testAlterIndexEmptyIndexName() throws Exception {

    Index index = buildIndexWithDefaultValues();
    client.alter_index(DB_NAME, TABLE_NAME, "", index);
  }

  @Test(expected = MetaException.class)
  public void testAlterIndexEmptyDbName() throws Exception {

    Index index = buildIndexWithDefaultValues();
    client.alter_index("", TABLE_NAME, INDEX_NAME, index);
  }

  @Test(expected = MetaException.class)
  public void testAlterIndexEmptyTableName() throws Exception {

    Index index = buildIndexWithDefaultValues();
    client.alter_index(DB_NAME, "", INDEX_NAME, index);
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterIndexNullSd() throws Exception {

    createIndex(INDEX_NAME, TABLE_NAME);
    Index index = buildIndexWithDefaultValues();
    index.setSd(null);
    client.alter_index(DB_NAME, TABLE_NAME, INDEX_NAME, index);
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterIndexDifferentIndexTable() throws Exception {

    createIndex(INDEX_NAME, TABLE_NAME);
    Index index = buildIndexWithDefaultValues();
    index.setIndexTableName("newindextable");
    client.alter_index(DB_NAME, TABLE_NAME, INDEX_NAME, index);
    client.dropIndex(DB_NAME, TABLE_NAME, INDEX_NAME, true);
  }

  public void testAlterIndexNullIndexNameInNewIndex() throws Exception {

    createIndex(INDEX_NAME, TABLE_NAME);
    Index index = new IndexBuilder()
        .setDbName(null)
        .setTableName(null)
        .setIndexName(null)
        .setIndexTableName(null)
        .addCol("id", "int", "test col id")
        .addCol("value", "string", "test col value")
        .setDeferredRebuild(false)
        .build();
    client.alter_index(DB_NAME, TABLE_NAME, INDEX_NAME, index);

    Index resultIndex = client.getIndex(DB_NAME, TABLE_NAME, INDEX_NAME);
    Assert.assertNotNull(resultIndex);
    Assert.assertEquals(DB_NAME, resultIndex.getDbName());
    Assert.assertEquals(TABLE_NAME, resultIndex.getOrigTableName());
    Assert.assertEquals(INDEX_NAME, resultIndex.getIndexName());
    Assert.assertEquals(INDEX_TABLE_NAME, resultIndex.getIndexTableName());
    client.dropIndex(DB_NAME, TABLE_NAME, INDEX_NAME, true);
  }

  @Test(expected=MetaException.class)
  public void testAlterIndexNullCols() throws Exception {

    createIndex(INDEX_NAME, TABLE_NAME);
    Index index = new IndexBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .setIndexName(INDEX_NAME)
        .setIndexTableName(INDEX_TABLE_NAME)
        .setCols(null)
        .build();
    client.alter_index(DB_NAME, TABLE_NAME, INDEX_NAME, index);
    client.dropIndex(DB_NAME, TABLE_NAME, INDEX_NAME, true);
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

    if (tableType != null) {
      tableBuilder.setType(tableType.name());
    }

    return tableBuilder.build();
  }

  private void createIndex(String indexName, String origTableName) throws Exception {
    Table origTable = createTable(DB_NAME, origTableName);
    String indexTableName = origTableName + "__" + indexName + "__";
    createIndex(DB_NAME, origTable, indexName, buildTable(DB_NAME, indexTableName,
            TableType.INDEX_TABLE));
  }

  private void createIndex(String dbName, Table origTable, String indexName, Table indexTable)
      throws Exception {
    int createTime = (int) (System.currentTimeMillis() / 1000);
    Map<String, String> params = new HashMap<String, String>();
    params.put("indexParamKey", "indexParamValue");

    Index index =
        buildIndex(dbName, origTable.getTableName(), indexName, indexTable.getTableName());
    index.setCreateTime(createTime);
    index.setLastAccessTime(createTime);
    index.setParameters(params);
    client.createIndex(index, indexTable);
  }

  private Index buildIndexWithDefaultValues() throws MetaException {
    return buildIndex(DB_NAME, TABLE_NAME, INDEX_NAME, INDEX_TABLE_NAME);
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
        .setDeferredRebuild(false)
        .build();
    return index;
  }

  private void createDB(String dbName) throws TException {
    Database db = new DatabaseBuilder().
        setName(dbName).
        build();
    client.createDatabase(db);
  }

  private void checkIfIndexListEmpty(String dbName, String origTableName)
      throws MetaException, TException {
    List<String> indexes = client.listIndexNames(dbName, origTableName, MAX);
    Assert.assertNotNull(indexes);
    Assert.assertTrue(indexes.isEmpty());
  }

  private void checkIfIndexExists(String dbName, String origTableName, String indexName)
      throws MetaException, TException {
    List<String> indexes = client.listIndexNames(dbName, origTableName, MAX);
    Assert.assertNotNull(indexes);
    Assert.assertEquals(1, indexes.size());
    Assert.assertEquals(indexName, indexes.get(0));
  }

  private void verifyIndex(String dbName, String origTableName, String indexName,
      String indexTableName)
      throws MetaException, UnknownTableException, NoSuchObjectException, TException {
    Index index = client.getIndex(dbName, origTableName, indexName);
    Assert.assertNotNull(index);
    Assert.assertEquals(dbName, index.getDbName());
    Assert.assertEquals(origTableName, index.getOrigTableName());
    Assert.assertEquals(indexName, index.getIndexName());
    Assert.assertEquals(indexTableName, index.getIndexTableName());
  }
}
