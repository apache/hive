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

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.thrift.TException;

import com.google.common.collect.Lists;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * API tests for HMS client's getTableMeta method.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestGetTableMeta {

  // Needed until there is no junit release with @BeforeParam, @AfterParam (junit 4.13)
  // https://github.com/junit-team/junit4/commit/1bf8438b65858565dbb64736bfe13aae9cfc1b5a
  // Then we should remove our own copy
  private static Set<AbstractMetaStoreService> metaStoreServices = null;
  private AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;

  private static final String DB_NAME = "testpartdb";
  private static final String TABLE_NAME = "testparttable";

  private List<TableMeta> expectedMetas = null;


  @Parameterized.Parameters(name = "{0}")
  public static List<Object[]> getMetaStoreToTest() throws Exception {
    List<Object[]> result = MetaStoreFactoryForTests.getMetaStores();
    metaStoreServices = result.stream()
            .map(test -> (AbstractMetaStoreService)test[1])
            .collect(toSet());
    return result;
  }

  public TestGetTableMeta(String name, AbstractMetaStoreService metaStore) throws Exception {
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

    // Clean up
    client.dropDatabase(DB_NAME + "_one", true, true, true);
    client.dropDatabase(DB_NAME + "_two", true, true, true);

    metaStore.cleanWarehouseDirs();

    //Create test dbs and tables
    expectedMetas = new ArrayList<>();
    String dbName = DB_NAME + "_one";
    createDB(dbName);
    expectedMetas.add(createTestTable(dbName, TABLE_NAME + "_one", TableType.EXTERNAL_TABLE));
    expectedMetas.add(createTestTable(dbName, TABLE_NAME + "", TableType.MANAGED_TABLE, "cmT"));
    expectedMetas.add(createTestTable(dbName, "v" + TABLE_NAME, TableType.VIRTUAL_VIEW));

    dbName = DB_NAME + "_two";
    createDB(dbName);
    expectedMetas.add(createTestTable(dbName, TABLE_NAME + "_one", TableType.MANAGED_TABLE));
    expectedMetas.add(createTestTable(dbName, "v" + TABLE_NAME, TableType.MATERIALIZED_VIEW, ""));
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


  private Database createDB(String dbName) throws TException {
    Database db = new DatabaseBuilder().
            setName(dbName).
            build();
    client.createDatabase(db);
    return db;
  }


  private Table createTable(String dbName, String tableName, TableType type)
          throws Exception {
    TableBuilder builder = new TableBuilder()
            .setDbName(dbName)
            .setTableName(tableName)
            .addCol("id", "int")
            .addCol("name", "string")
            .setType(type.name());


    Table table = builder.build();


    if (type == TableType.MATERIALIZED_VIEW) {
      CreationMetadata cm = new CreationMetadata(
          dbName, tableName, ImmutableSet.of());
      table.setCreationMetadata(cm);
    }

    if (type == TableType.EXTERNAL_TABLE) {
      table.getParameters().put("EXTERNAL", "true");
    }

    return table;
  }

  private TableMeta createTestTable(String dbName, String tableName, TableType type, String comment)
    throws Exception {
    Table table  = createTable(dbName, tableName, type);
    table.getParameters().put("comment", comment);
    client.createTable(table);
    TableMeta tableMeta = new TableMeta(dbName, tableName, type.name());
    tableMeta.setComments(comment);
    return tableMeta;
  }

  private TableMeta createTestTable(String dbName, String tableName, TableType type)
          throws Exception {
    Table table  = createTable(dbName, tableName, type);
    client.createTable(table);
    return new TableMeta(dbName, tableName, type.name());
  }

  private void assertTableMetas(int[] expected, List<TableMeta> actualTableMetas) {
    assertEquals("Expected " + expected.length + " but have " + actualTableMetas.size() +
            " tableMeta(s)", expected.length, actualTableMetas.size());

    Set<TableMeta> metas = actualTableMetas.stream().collect(toSet());
    for (int i : expected){
      assertTrue("Missing " + expectedMetas.get(i), metas.remove(expectedMetas.get(i)));
    }

    assertTrue("Unexpected tableMeta(s): " + metas, metas.isEmpty());
  }

  /**
   * Testing getTableMeta(String,String,List(String)) ->
   *         get_table_meta(String,String,List(String)).
   * @throws Exception
   */
  @Test
  public void testGetTableMeta() throws Exception {
    List<TableMeta> tableMetas = client.getTableMeta("asdf", "qwerty", Lists.newArrayList("zxcv"));
    assertTableMetas(new int[]{}, tableMetas);

    tableMetas = client.getTableMeta("testpartdb_two", "vtestparttable", Lists.newArrayList());
    assertTableMetas(new int[]{4}, tableMetas);

    tableMetas = client.getTableMeta("*", "*", Lists.newArrayList());
    assertTableMetas(new int[]{0, 1, 2, 3, 4}, tableMetas);

    tableMetas = client.getTableMeta("***", "**", Lists.newArrayList());
    assertTableMetas(new int[]{0, 1, 2, 3, 4}, tableMetas);

    tableMetas = client.getTableMeta("*one", "*", Lists.newArrayList());
    assertTableMetas(new int[]{0, 1, 2}, tableMetas);

    tableMetas = client.getTableMeta("*one*", "*", Lists.newArrayList());
    assertTableMetas(new int[]{0, 1, 2}, tableMetas);

    tableMetas = client.getTableMeta("testpartdb_two", "*", Lists.newArrayList());
    assertTableMetas(new int[]{3, 4}, tableMetas);

    tableMetas = client.getTableMeta("testpartdb_two*", "*", Lists.newArrayList());
    assertTableMetas(new int[]{3, 4}, tableMetas);

    tableMetas = client.getTableMeta("testpartdb*", "*", Lists.newArrayList(
            TableType.EXTERNAL_TABLE.name()));
    assertTableMetas(new int[]{0}, tableMetas);

    tableMetas = client.getTableMeta("testpartdb*", "*", Lists.newArrayList(
            TableType.EXTERNAL_TABLE.name(), TableType.MATERIALIZED_VIEW.name()));
    assertTableMetas(new int[]{0, 4}, tableMetas);

    tableMetas = client.getTableMeta("*one", "*", Lists.newArrayList("*TABLE"));
    assertTableMetas(new int[]{}, tableMetas);

    tableMetas = client.getTableMeta("*one", "*", Lists.newArrayList("*"));
    assertTableMetas(new int[]{}, tableMetas);
  }

  @Test
  public void testGetTableMetaCaseSensitive() throws Exception {
    List<TableMeta> tableMetas = client.getTableMeta("*tWo", "tEsT*", Lists.newArrayList());
    assertTableMetas(new int[]{3}, tableMetas);

    tableMetas = client.getTableMeta("*", "*", Lists.newArrayList("mAnAGeD_tABlE"));
    assertTableMetas(new int[]{}, tableMetas);
  }

  @Test
  public void testGetTableMetaNullOrEmptyDb() throws Exception {
    List<TableMeta> tableMetas = client.getTableMeta(null, "*", Lists.newArrayList());
    assertTableMetas(new int[]{0, 1, 2, 3, 4}, tableMetas);

    tableMetas = client.getTableMeta("", "*", Lists.newArrayList());
    assertTableMetas(new int[]{}, tableMetas);
  }

  @Test
  public void testGetTableMetaNullOrEmptyTbl() throws Exception {
    List<TableMeta> tableMetas = client.getTableMeta("*", null, Lists.newArrayList());
    assertTableMetas(new int[]{0, 1, 2, 3, 4}, tableMetas);

    tableMetas = client.getTableMeta("*", "", Lists.newArrayList());
    assertTableMetas(new int[]{}, tableMetas);
  }

  @Test
  public void testGetTableMetaNullOrEmptyTypes() throws Exception {
    List<TableMeta> tableMetas = client.getTableMeta("*", "*", Lists.newArrayList());
    assertTableMetas(new int[]{0, 1, 2, 3, 4}, tableMetas);

    tableMetas = client.getTableMeta("*", "*", Lists.newArrayList(""));
    assertTableMetas(new int[]{}, tableMetas);

    tableMetas = client.getTableMeta("*", "*", null);
    assertTableMetas(new int[]{0, 1, 2, 3, 4}, tableMetas);
  }

  @Test
  public void testGetTableMetaNullNoDbNoTbl() throws Exception {
    client.dropDatabase(DB_NAME + "_one", true, true, true);
    client.dropDatabase(DB_NAME + "_two", true, true, true);
    List<TableMeta> tableMetas = client.getTableMeta("*", "*", Lists.newArrayList());
    assertTableMetas(new int[]{}, tableMetas);
  }

}
