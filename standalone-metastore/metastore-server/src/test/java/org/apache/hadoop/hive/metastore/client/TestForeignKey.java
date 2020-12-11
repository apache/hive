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
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.SQLForeignKeyBuilder;
import org.apache.hadoop.hive.metastore.client.builder.SQLPrimaryKeyBuilder;
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

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;

@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestForeignKey extends MetaStoreClientTest {
  private static final String OTHER_DATABASE = "test_fk_other_database";
  private static final String OTHER_CATALOG = "test_fk_other_catalog";
  private static final String DATABASE_IN_OTHER_CATALOG = "test_fk_database_in_other_catalog";
  private final AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;
  private Table[] testTables = new Table[4];
  private Database inOtherCatalog;

  public TestForeignKey(String name, AbstractMetaStoreService metaStore) throws Exception {
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

    testTables[3] =
        new TableBuilder()
            .inDb(inOtherCatalog)
            .setTableName("test_table_4")
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
    Table parentTable = testTables[1];
    Table table = testTables[0];
    // Make sure get on a table with no key returns empty list
    ForeignKeysRequest rqst =
        new ForeignKeysRequest(parentTable.getDbName(), parentTable.getTableName(),
            table.getDbName(), table.getTableName());
    rqst.setCatName(table.getCatName());
    List<SQLForeignKey> fetched = client.getForeignKeys(rqst);
    Assert.assertTrue(fetched.isEmpty());

    // Single column unnamed primary key in default catalog and database
    List<SQLPrimaryKey> pk = new SQLPrimaryKeyBuilder()
        .onTable(parentTable)
        .addColumn("col1")
        .build(metaStore.getConf());
    client.addPrimaryKey(pk);

    List<SQLForeignKey> fk = new SQLForeignKeyBuilder()
        .fromPrimaryKey(pk)
        .onTable(table)
        .addColumn("col1")
        .build(metaStore.getConf());
    client.addForeignKey(fk);


    rqst = new ForeignKeysRequest(parentTable.getDbName(), parentTable.getTableName(),
        table.getDbName(), table.getTableName());
    rqst.setCatName(table.getCatName());
    fetched = client.getForeignKeys(rqst);
    fk.get(0).setFk_name(fetched.get(0).getFk_name());
    Assert.assertEquals(fk, fetched);


    // Drop a foreign key
    client.dropConstraint(table.getCatName(), table.getDbName(),
        table.getTableName(), fk.get(0).getFk_name());
    rqst = new ForeignKeysRequest(parentTable.getDbName(), parentTable.getTableName(),
        table.getDbName(), table.getTableName());
    rqst.setCatName(table.getCatName());
    fetched = client.getForeignKeys(rqst);
    Assert.assertTrue(fetched.isEmpty());

    // Make sure I can add it back
    client.addForeignKey(fk);
  }

  @Test
  public void createGetDrop2Column() throws TException {
    Table parentTable = testTables[1];
    Table table = testTables[0];
    String constraintName = "2colfk";

    // Single column unnamed primary key in default catalog and database
    List<SQLPrimaryKey> pk = new SQLPrimaryKeyBuilder()
        .onTable(parentTable)
        .addColumn("col1")
        .addColumn("col2")
        .build(metaStore.getConf());
    client.addPrimaryKey(pk);

    List<SQLForeignKey> fk = new SQLForeignKeyBuilder()
        .fromPrimaryKey(pk)
        .onTable(table)
        .addColumn("col1")
        .addColumn("col2")
        .setConstraintName(constraintName)
        .build(metaStore.getConf());
    client.addForeignKey(fk);


    ForeignKeysRequest rqst = new ForeignKeysRequest(parentTable.getDbName(),
        parentTable.getTableName(), table.getDbName(), table.getTableName());
    rqst.setCatName(table.getCatName());
    List<SQLForeignKey> fetched = client.getForeignKeys(rqst);
    Assert.assertEquals(fk, fetched);


    // Drop a foreign key
    client.dropConstraint(table.getCatName(), table.getDbName(),
        table.getTableName(), constraintName);
    rqst = new ForeignKeysRequest(parentTable.getDbName(), parentTable.getTableName(),
        table.getDbName(), table.getTableName());
    rqst.setCatName(table.getCatName());
    fetched = client.getForeignKeys(rqst);
    Assert.assertTrue(fetched.isEmpty());

    // Make sure I can add it back
    client.addForeignKey(fk);
  }

  @Test
  public void inOtherCatalog() throws TException {
    Table parentTable = testTables[2];
    Table table = testTables[3];
    String constraintName = "othercatfk";

    // Single column unnamed primary key in default catalog and database
    List<SQLPrimaryKey> pk = new SQLPrimaryKeyBuilder()
        .onTable(parentTable)
        .addColumn("col1")
        .build(metaStore.getConf());
    client.addPrimaryKey(pk);

    List<SQLForeignKey> fk = new SQLForeignKeyBuilder()
        .fromPrimaryKey(pk)
        .onTable(table)
        .addColumn("col1")
        .setConstraintName(constraintName)
        .build(metaStore.getConf());
    client.addForeignKey(fk);


    ForeignKeysRequest rqst = new ForeignKeysRequest(parentTable.getDbName(),
        parentTable.getTableName(), table.getDbName(), table.getTableName());
    rqst.setCatName(table.getCatName());
    List<SQLForeignKey> fetched = client.getForeignKeys(rqst);
    fk.get(0).setFk_name(fetched.get(0).getFk_name());
    Assert.assertEquals(fk, fetched);


    // Drop a foreign key
    client.dropConstraint(table.getCatName(), table.getDbName(),
        table.getTableName(), constraintName);
    rqst = new ForeignKeysRequest(parentTable.getDbName(), parentTable.getTableName(),
        table.getDbName(), table.getTableName());
    rqst.setCatName(table.getCatName());
    fetched = client.getForeignKeys(rqst);
    Assert.assertTrue(fetched.isEmpty());

    // Make sure I can add it back
    client.addForeignKey(fk);
  }

  @Test
  public void createTableWithConstraints() throws TException {
    String constraintName = "ctwckk";
    Table parentTable = testTables[0];
    Table table = new TableBuilder()
        .setTableName("table_with_constraints")
        .setDbName(parentTable.getDbName())
        .addCol("col1", "int")
        .addCol("col2", "varchar(32)")
        .build(metaStore.getConf());

    List<SQLPrimaryKey> pk = new SQLPrimaryKeyBuilder()
        .onTable(parentTable)
        .addColumn("col1")
        .build(metaStore.getConf());
    client.addPrimaryKey(pk);

    List<SQLForeignKey> fk = new SQLForeignKeyBuilder()
        .fromPrimaryKey(pk)
        .onTable(table)
        .addColumn("col1")
        .setConstraintName(constraintName)
        .build(metaStore.getConf());

    client.createTableWithConstraints(table, null, fk, null, null, null, null);

    ForeignKeysRequest rqst = new ForeignKeysRequest(parentTable.getDbName(), parentTable
        .getTableName(),
        table.getDbName(), table.getTableName());
    rqst.setCatName(table.getCatName());
    List<SQLForeignKey> fetched = client.getForeignKeys(rqst);
    Assert.assertEquals(fk, fetched);

  }

  @Test
  public void createTableWithConstraintsInOtherCatalog() throws TException {
    String constraintName = "ctwcocfk";
    Table parentTable = testTables[2];
    Table table = new TableBuilder()
        .setTableName("table_with_constraints")
        .inDb(inOtherCatalog)
        .addCol("col1", "int")
        .addCol("col2", "varchar(32)")
        .build(metaStore.getConf());

    List<SQLPrimaryKey> pk = new SQLPrimaryKeyBuilder()
        .onTable(parentTable)
        .addColumn("col1")
        .build(metaStore.getConf());
    client.addPrimaryKey(pk);

    List<SQLForeignKey> fk = new SQLForeignKeyBuilder()
        .fromPrimaryKey(pk)
        .onTable(table)
        .addColumn("col1")
        .setConstraintName(constraintName)
        .build(metaStore.getConf());

    client.createTableWithConstraints(table, null, fk, null, null, null, null);

    ForeignKeysRequest rqst = new ForeignKeysRequest(parentTable.getDbName(), parentTable
        .getTableName(),
        table.getDbName(), table.getTableName());
    rqst.setCatName(table.getCatName());
    List<SQLForeignKey> fetched = client.getForeignKeys(rqst);
    Assert.assertEquals(fk, fetched);
  }

  @Test(expected = MetaException.class)
  public void noSuchPk() throws TException {
    List<SQLPrimaryKey> pk = new SQLPrimaryKeyBuilder()
        .onTable(testTables[1])
        .addColumn("col1")
        .build(metaStore.getConf());
    // Don't actually create the key
    List<SQLForeignKey> fk = new SQLForeignKeyBuilder()
        .onTable(testTables[0])
        .fromPrimaryKey(pk)
        .addColumn("col2")
        .build(metaStore.getConf());
    client.addForeignKey(fk);
    Assert.fail();
  }

  @Test
  public void addNoSuchTable() throws TException {
    Table parentTable = testTables[0];

    List<SQLPrimaryKey> pk = new SQLPrimaryKeyBuilder()
        .onTable(parentTable)
        .addColumn("col1")
        .build(metaStore.getConf());
    client.addPrimaryKey(pk);

    try {
      List<SQLForeignKey> fk = new SQLForeignKeyBuilder()
          .setTableName("nosuch")
          .fromPrimaryKey(pk)
          .addColumn("col2")
          .build(metaStore.getConf());
      client.addForeignKey(fk);
      Assert.fail();
    } catch (InvalidObjectException |TApplicationException e) {
      // NOP
    }
  }

  @Test
  public void addNoSuchDb() throws TException {
    Table parentTable = testTables[0];

    List<SQLPrimaryKey> pk = new SQLPrimaryKeyBuilder()
        .onTable(parentTable)
        .addColumn("col1")
        .build(metaStore.getConf());
    client.addPrimaryKey(pk);

    try {
      List<SQLForeignKey> fk = new SQLForeignKeyBuilder()
          .setTableName(testTables[0].getTableName())
          .setDbName("nosuch")
          .fromPrimaryKey(pk)
          .addColumn("col2")
          .build(metaStore.getConf());
      client.addForeignKey(fk);
      Assert.fail();
    } catch (InvalidObjectException |TApplicationException e) {
      // NOP
    }
  }

  @Test
  public void addNoSuchCatalog() throws TException {
    Table parentTable = testTables[0];

    List<SQLPrimaryKey> pk = new SQLPrimaryKeyBuilder()
        .onTable(parentTable)
        .addColumn("col1")
        .build(metaStore.getConf());
    client.addPrimaryKey(pk);

    try {
      List<SQLForeignKey> fk = new SQLForeignKeyBuilder()
          .setTableName(testTables[0].getTableName())
          .setDbName(testTables[0].getDbName())
          .setCatName("nosuch")
          .fromPrimaryKey(pk)
          .addColumn("col2")
          .build(metaStore.getConf());
      client.addForeignKey(fk);
      Assert.fail();
    } catch (InvalidObjectException |TApplicationException e) {
      // NOP
    }
  }

  @Test
  public void foreignKeyAcrossCatalogs() throws TException {
    Table parentTable = testTables[2];
    Table table = testTables[0];

    // Single column unnamed primary key in default catalog and database
    List<SQLPrimaryKey> pk = new SQLPrimaryKeyBuilder()
        .onTable(parentTable)
        .addColumn("col1")
        .build(metaStore.getConf());
    client.addPrimaryKey(pk);

    try {
      List<SQLForeignKey> fk = new SQLForeignKeyBuilder()
          .fromPrimaryKey(pk)
          .onTable(table)
          .addColumn("col1")
          .build(metaStore.getConf());
      client.addForeignKey(fk);
      Assert.fail();
    } catch (InvalidObjectException |TApplicationException e) {
      // NOP
    }
  }

}
