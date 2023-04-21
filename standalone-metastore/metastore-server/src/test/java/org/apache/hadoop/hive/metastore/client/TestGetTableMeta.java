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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;

import org.apache.thrift.TException;

import com.google.common.collect.Lists;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * API tests for HMS client's getTableMeta method.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestGetTableMeta extends MetaStoreClientTest {
  private AbstractMetaStoreService metaStore;
  private Configuration conf;
  private HiveMetaStoreClient client;
  private static final String DB_NAME = "testpartdb";
  private static final String TABLE_NAME = "testparttable";
  private static final String TABLE_OWNER = "testuser";
  private static final PrincipalType TABLE_OWNER_TYPE = PrincipalType.USER;
  private List<TableMeta> expectedMetas = null;

  public TestGetTableMeta(String name, AbstractMetaStoreService metaStore) {
    this.metaStore = metaStore;
  }

  @Before
  public void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    // Directly set client capability since HiveConf is unaccessable through standalone-metastore
    MetastoreConf.setBoolVar(conf, ConfVars.HIVE_IN_TEST, true);
    MetaStoreTestUtils.setConfForStandloneMode(conf);

    // Get new client
    client = metaStore.getClient();
    String[] clientCaps = new String[]{"HIVEMANAGEDINSERTWRITE","HIVEMANAGESTATS","HIVECACHEINVALIDATE",
        "CONNECTORWRITE"};
    client.setProcessorCapabilities(clientCaps);

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

  private void createDB(String dbName) throws TException {
    new DatabaseBuilder().
            setName(dbName).
            create(client, metaStore.getConf());
  }

  private Table createTable(String dbName, String tableName, TableType type, String owner, PrincipalType ownerType)
          throws Exception {
    TableBuilder builder = new TableBuilder()
            .setCatName("hive")
            .setDbName(dbName) .setTableName(tableName)
            .setOwner(owner)
            .setOwnerType(ownerType)
            .addCol("id", "int")
            .addCol("name", "string")
            .setType(type.name());

    Table table = builder.build(metaStore.getConf());

    if (type == TableType.MATERIALIZED_VIEW) {
      CreationMetadata cm = new CreationMetadata(
          MetaStoreUtils.getDefaultCatalog(metaStore.getConf()), dbName, tableName, ImmutableSet.of());
      cm.setSourceTables(Collections.emptyList());
      table.setCreationMetadata(cm);
    }

    if (type == TableType.EXTERNAL_TABLE) {
      table.getParameters().put("EXTERNAL", "true");
    } else if (type == TableType.MANAGED_TABLE) {
       // we do not want MANAGED tables to be converted to EXTERNAL
      table.getParameters().put("transactional", "true");
      table.getParameters().put("transactional_properties", "insert_only");
    }
    return table;
  }

  private class OwnerInfo {
    String owner;
    PrincipalType ownerType;
  }

  private TableMeta createTestTable(String dbName, String tableName, TableType type, String comment,
      OwnerInfo ownerInfo) throws Exception {
    String owner = ownerInfo == null ? TABLE_OWNER : ownerInfo.owner;
    PrincipalType ownerType = ownerInfo == null ? TABLE_OWNER_TYPE : ownerInfo.ownerType;
    Table table = createTable(dbName, tableName, type, owner, ownerType);
    TableMeta expectedTableMeta = new TableMeta(dbName, tableName, type.toString());
    if (comment != null) {
      table.getParameters().put("comment", comment);
      expectedTableMeta.setComments(comment);
    }
    expectedTableMeta.setOwnerName(owner);
    expectedTableMeta.setOwnerType(ownerType);
    expectedTableMeta.setCatName("hive");
    client.createTable(table);
    return expectedTableMeta;

  }

  private TableMeta createTestTable(String dbName, String tableName, TableType type, String comment)
      throws Exception {
    return createTestTable(dbName, tableName, type, comment, null);
  }

  private TableMeta createTestTable(String dbName, String tableName, TableType type)
      throws Exception {
    return createTestTable(dbName, tableName, type, null, null);
  }

  private void assertTableMetas(int[] expected, List<TableMeta> actualTableMetas) {
    assertTableMetas(expectedMetas, actualTableMetas, expected);
  }

  private void assertTableMetas(List<TableMeta> actual, int... expected) {
    assertTableMetas(expectedMetas, actual, expected);
  }

  private void assertTableMetas(List<TableMeta> fullExpected, List<TableMeta> actual, int... expected) {
    assertEquals("Expected " + expected.length + " but have " + actual.size() +
        " tableMeta(s)", expected.length, actual.size());

    Set<TableMeta> metas = new HashSet<>(actual);
    for (int i : expected){
      assertTrue("Missing " + fullExpected.get(i) + "" + actual, metas.remove(fullExpected.get(i)));
    }

    assertTrue("Unexpected tableMeta(s): " + metas, metas.isEmpty());

  }

  /**
   * Testing getTableMeta(String,String,List(String)) ->
   *         get_table_meta(String,String,List(String)).
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

  @Test
  public void tablesInDifferentCatalog() throws TException {
    String catName = "get_table_meta_catalog";
    Catalog cat = new CatalogBuilder()
        .setName(catName)
        .setLocation(MetaStoreTestUtils.getTestWarehouseDir(catName))
        .build();
    client.createCatalog(cat);

    String dbName = "db9";
    // For this one don't specify a location to make sure it gets put in the catalog directory
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .setCatalogName(catName)
        .create(client, metaStore.getConf());

    String[] tableNames = {"table_in_other_catalog_1", "table_in_other_catalog_2", "random_name"};
    List<TableMeta> expected = new ArrayList<>(tableNames.length);
    for (int i = 0; i < tableNames.length; i++) {
      client.createTable(new TableBuilder()
          .inDb(db)
          .setTableName(tableNames[i])
          .setOwner(TABLE_OWNER)
          .setOwnerType(TABLE_OWNER_TYPE)
          .addCol("id", "int")
          .addCol("name", "string")
          .build(metaStore.getConf()));
      Table table = client.getTable(catName, dbName, tableNames[i]);
      TableMeta tableMeta = new TableMeta(dbName, tableNames[i], table.getTableType());
      tableMeta.setCatName(catName);
      tableMeta.setOwnerName(TABLE_OWNER);
      tableMeta.setOwnerType(TABLE_OWNER_TYPE);
      expected.add(tableMeta);
    }

    List<String> typesList = Lists.newArrayList(TableType.MANAGED_TABLE.name(),
        TableType.EXTERNAL_TABLE.name());
    List<String> types = Collections.unmodifiableList(typesList);
    List<TableMeta> actual = client.getTableMeta(catName, dbName, "*", types);
    assertTableMetas(expected, actual, 0, 1, 2);

    actual = client.getTableMeta(catName, "*", "table_*", types);
    assertTableMetas(expected, actual, 0, 1);

    actual = client.getTableMeta(dbName, "table_in_other_catalog_*", types);
    assertTableMetas(expected, actual);
  }

  @Test
  public void noSuchCatalog() throws TException {
    List<TableMeta> tableMetas = client.getTableMeta("nosuchcatalog", "*", "*", Lists.newArrayList());
    Assert.assertEquals(0, tableMetas.size());
  }

  @Test
  public void catalogPatternsDontWork() throws TException {
    List<TableMeta> tableMetas = client.getTableMeta("h*", "*", "*", Lists.newArrayList());
    Assert.assertEquals(0, tableMetas.size());
  }
}
