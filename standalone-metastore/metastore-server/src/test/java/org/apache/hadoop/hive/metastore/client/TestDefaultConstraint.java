/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.client;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.SQLDefaultConstraintBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;

@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestDefaultConstraint extends MetaStoreClientTest {
  private static final String OTHER_DATABASE = "test_uc_other_database";
  private static final String OTHER_CATALOG = "test_uc_other_catalog";
  private static final String DATABASE_IN_OTHER_CATALOG = "test_uc_database_in_other_catalog";
  private final AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;
  private Table[] testTables = new Table[3];
  private Database inOtherCatalog;

  public TestDefaultConstraint(String name, AbstractMetaStoreService metaStore) throws Exception {
    this.metaStore = metaStore;
  }

  @Before
  public void setUp() throws Exception {
    // Get new client
    client = metaStore.getClient();

    // Clean up the database
    client.dropDatabase(OTHER_DATABASE, true, true, true);
    // Drop every table in the default database
    for(String tableName : client.getAllTables(DEFAULT_DATABASE_NAME)) {
      client.dropTable(DEFAULT_DATABASE_NAME, tableName, true, true, true);
    }

    client.dropDatabase(OTHER_CATALOG, DATABASE_IN_OTHER_CATALOG, true, true, true);
    try {
      client.dropCatalog(OTHER_CATALOG);
    } catch (NoSuchObjectException e) {
      // NOP
    }

    // Clean up trash
    metaStore.cleanWarehouseDirs();

    new DatabaseBuilder().setName(OTHER_DATABASE).create(client, metaStore.getConf());

    Catalog cat = new CatalogBuilder()
        .setName(OTHER_CATALOG)
        .setLocation(MetaStoreTestUtils.getTestWarehouseDir(OTHER_CATALOG))
        .build();
    client.createCatalog(cat);

    // For this one don't specify a location to make sure it gets put in the catalog directory
    inOtherCatalog = new DatabaseBuilder()
        .setName(DATABASE_IN_OTHER_CATALOG)
        .setCatalogName(OTHER_CATALOG)
        .create(client, metaStore.getConf());

    testTables[0] =
        new TableBuilder()
            .setTableName("test_table_1")
            .addCol("col1", "int")
            .addCol("col2", "varchar(32)")
            .create(client, metaStore.getConf());

    testTables[1] =
        new TableBuilder()
            .setDbName(OTHER_DATABASE)
            .setTableName("test_table_2")
            .addCol("col1", "int")
            .addCol("col2", "varchar(32)")
            .create(client, metaStore.getConf());

    testTables[2] =
        new TableBuilder()
            .inDb(inOtherCatalog)
            .setTableName("test_table_3")
            .addCol("col1", "int")
            .addCol("col2", "varchar(32)")
            .create(client, metaStore.getConf());

    // Reload tables from the MetaStore
    for(int i=0; i < testTables.length; i++) {
      testTables[i] = client.getTable(testTables[i].getCatName(), testTables[i].getDbName(),
          testTables[i].getTableName());
    }
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

  @Test
  public void createGetDrop() throws TException {
    Table table = testTables[0];
    // Make sure get on a table with no key returns empty list
    DefaultConstraintsRequest rqst =
        new DefaultConstraintsRequest(table.getCatName(), table.getDbName(), table.getTableName());
    List<SQLDefaultConstraint> fetched = client.getDefaultConstraints(rqst);
    Assert.assertTrue(fetched.isEmpty());

    // Single column unnamed primary key in default catalog and database
    List<SQLDefaultConstraint> dv = new SQLDefaultConstraintBuilder()
        .onTable(table)
        .addColumn("col1")
        .setDefaultVal(0)
        .build(metaStore.getConf());
    client.addDefaultConstraint(dv);

    rqst = new DefaultConstraintsRequest(table.getCatName(), table.getDbName(), table.getTableName());
    fetched = client.getDefaultConstraints(rqst);
    dv.get(0).setDc_name(fetched.get(0).getDc_name());
    Assert.assertEquals(dv, fetched);


    // Drop a primary key
    client.dropConstraint(table.getCatName(), table.getDbName(),
        table.getTableName(), dv.get(0).getDc_name());
    rqst = new DefaultConstraintsRequest(table.getCatName(), table.getDbName(), table.getTableName());
    fetched = client.getDefaultConstraints(rqst);
    Assert.assertTrue(fetched.isEmpty());

    // Make sure I can add it back
    client.addDefaultConstraint(dv);
  }

  @Test
  public void inOtherCatalog() throws TException {
    String constraintName = "ocdv";
    // Table in non 'hive' catalog
    List<SQLDefaultConstraint> dv = new SQLDefaultConstraintBuilder()
        .onTable(testTables[2])
        .addColumn("col1")
        .setConstraintName(constraintName)
        .setDefaultVal("empty")
        .build(metaStore.getConf());
    client.addDefaultConstraint(dv);

    DefaultConstraintsRequest rqst = new DefaultConstraintsRequest(testTables[2].getCatName(),
        testTables[2].getDbName(), testTables[2].getTableName());
    List<SQLDefaultConstraint> fetched = client.getDefaultConstraints(rqst);
    Assert.assertEquals(dv, fetched);

    client.dropConstraint(testTables[2].getCatName(), testTables[2].getDbName(),
        testTables[2].getTableName(), constraintName);
    rqst = new DefaultConstraintsRequest(testTables[2].getCatName(), testTables[2].getDbName(),
        testTables[2].getTableName());
    fetched = client.getDefaultConstraints(rqst);
    Assert.assertTrue(fetched.isEmpty());
  }

  @Test
  public void createTableWithConstraintsPk() throws TException {
    String constraintName = "ctwcdv";
    Table table = new TableBuilder()
        .setTableName("table_with_constraints")
        .addCol("col1", "int")
        .addCol("col2", "varchar(32)")
        .build(metaStore.getConf());

    List<SQLDefaultConstraint> dv = new SQLDefaultConstraintBuilder()
        .onTable(table)
        .addColumn("col1")
        .setConstraintName(constraintName)
        .setDefaultVal(0)
        .build(metaStore.getConf());

    client.createTableWithConstraints(table, null, null, null, null, dv, null);
    DefaultConstraintsRequest rqst = new DefaultConstraintsRequest(table.getCatName(), table.getDbName(), table.getTableName());
    List<SQLDefaultConstraint> fetched = client.getDefaultConstraints(rqst);
    Assert.assertEquals(dv, fetched);

    client.dropConstraint(table.getCatName(), table.getDbName(), table.getTableName(), constraintName);
    rqst = new DefaultConstraintsRequest(table.getCatName(), table.getDbName(), table.getTableName());
    fetched = client.getDefaultConstraints(rqst);
    Assert.assertTrue(fetched.isEmpty());

  }

  @Test
  public void createTableWithConstraintsPkInOtherCatalog() throws TException {
    Table table = new TableBuilder()
        .setTableName("table_in_other_catalog_with_constraints")
        .inDb(inOtherCatalog)
        .addCol("col1", "int")
        .addCol("col2", "varchar(32)")
        .build(metaStore.getConf());

    List<SQLDefaultConstraint> dv = new SQLDefaultConstraintBuilder()
        .onTable(table)
        .addColumn("col1")
        .setDefaultVal(0)
        .build(metaStore.getConf());

    client.createTableWithConstraints(table, null, null, null, null, dv, null);
    DefaultConstraintsRequest rqst = new DefaultConstraintsRequest(table.getCatName(), table.getDbName(), table.getTableName());
    List<SQLDefaultConstraint> fetched = client.getDefaultConstraints(rqst);
    dv.get(0).setDc_name(fetched.get(0).getDc_name());
    Assert.assertEquals(dv, fetched);

    client.dropConstraint(table.getCatName(), table.getDbName(), table.getTableName(), dv.get(0).getDc_name());
    rqst = new DefaultConstraintsRequest(table.getCatName(), table.getDbName(), table.getTableName());
    fetched = client.getDefaultConstraints(rqst);
    Assert.assertTrue(fetched.isEmpty());
  }

  @Test
  public void doubleAddUniqueConstraint() throws TException {
    Table table = testTables[0];
    // Make sure get on a table with no key returns empty list
    DefaultConstraintsRequest rqst =
        new DefaultConstraintsRequest(table.getCatName(), table.getDbName(), table.getTableName());
    List<SQLDefaultConstraint> fetched = client.getDefaultConstraints(rqst);
    Assert.assertTrue(fetched.isEmpty());

    // Single column unnamed primary key in default catalog and database
    List<SQLDefaultConstraint> dv = new SQLDefaultConstraintBuilder()
        .onTable(table)
        .addColumn("col1")
        .setDefaultVal(0)
        .build(metaStore.getConf());
    client.addDefaultConstraint(dv);

    try {
      dv = new SQLDefaultConstraintBuilder()
          .onTable(table)
          .addColumn("col2")
          .setDefaultVal("this string intentionally left empty")
          .build(metaStore.getConf());
      client.addDefaultConstraint(dv);
      Assert.fail();
    } catch (InvalidObjectException |TApplicationException e) {
      // NOP
    }
  }

  @Test
  public void addNoSuchTable() throws TException {
    try {
      List<SQLDefaultConstraint> dv = new SQLDefaultConstraintBuilder()
          .setTableName("nosuch")
          .addColumn("col2")
          .setDefaultVal("this string intentionally left empty")
          .build(metaStore.getConf());
      client.addDefaultConstraint(dv);
      Assert.fail();
    } catch (InvalidObjectException |TApplicationException e) {
      // NOP
    }
  }

  @Test
  public void getNoSuchTable() throws TException {
    DefaultConstraintsRequest rqst =
        new DefaultConstraintsRequest(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, "nosuch");
    List<SQLDefaultConstraint> dv = client.getDefaultConstraints(rqst);
    Assert.assertTrue(dv.isEmpty());
  }

  @Test
  public void getNoSuchDb() throws TException {
    DefaultConstraintsRequest rqst =
        new DefaultConstraintsRequest(DEFAULT_CATALOG_NAME, "nosuch", testTables[0].getTableName());
    List<SQLDefaultConstraint> dv = client.getDefaultConstraints(rqst);
    Assert.assertTrue(dv.isEmpty());
  }

  @Test
  public void getNoSuchCatalog() throws TException {
    DefaultConstraintsRequest rqst = new DefaultConstraintsRequest("nosuch",
        testTables[0].getDbName(), testTables[0].getTableName());
    List<SQLDefaultConstraint> dv = client.getDefaultConstraints(rqst);
    Assert.assertTrue(dv.isEmpty());
  }
}
