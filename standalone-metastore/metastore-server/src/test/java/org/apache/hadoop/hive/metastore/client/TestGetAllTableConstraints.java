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
import org.apache.hadoop.hive.metastore.api.AllTableConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SQLAllTableConstraints;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.SQLCheckConstraintBuilder;
import org.apache.hadoop.hive.metastore.client.builder.SQLDefaultConstraintBuilder;
import org.apache.hadoop.hive.metastore.client.builder.SQLForeignKeyBuilder;
import org.apache.hadoop.hive.metastore.client.builder.SQLNotNullConstraintBuilder;
import org.apache.hadoop.hive.metastore.client.builder.SQLPrimaryKeyBuilder;
import org.apache.hadoop.hive.metastore.client.builder.SQLUniqueConstraintBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;

@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestGetAllTableConstraints extends MetaStoreClientTest {
  private static final String OTHER_DATABASE = "test_constraints_other_database";
  private static final String OTHER_CATALOG = "test_constraints_other_catalog";
  private static final String DATABASE_IN_OTHER_CATALOG = "test_constraints_database_in_other_catalog";
  private final AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;
  private Table[] testTables = new Table[2];

  public TestGetAllTableConstraints(String name, AbstractMetaStoreService metaStore) throws Exception {
    this.metaStore = metaStore;
  }

  @Before
  public void setUp() throws Exception {
    // Get new client
    client = metaStore.getClient();

    // Clean up the database
    client.dropDatabase(OTHER_DATABASE, true, true, true);
    // Drop every table in the default database
    for (String tableName : client.getAllTables(DEFAULT_DATABASE_NAME)) {
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

    Catalog cat =
        new CatalogBuilder().setName(OTHER_CATALOG).setLocation(MetaStoreTestUtils.getTestWarehouseDir(OTHER_CATALOG))
            .build();
    client.createCatalog(cat);

    testTables[0] = new TableBuilder().setTableName("test_table_1").addCol("col1", "int").addCol("col2", "int")
        .addCol("col3", "boolean").addCol("col4", "int").addCol("col5", "varchar(32)")
        .create(client, metaStore.getConf());

    testTables[1] = new TableBuilder().setDbName(OTHER_DATABASE).setTableName("test_table_2").addCol("col1", "int")
        .addCol("col2", "varchar(32)").create(client, metaStore.getConf());

    // Reload tables from the MetaStore
    for (int i = 0; i < testTables.length; i++) {
      testTables[i] =
          client.getTable(testTables[i].getCatName(), testTables[i].getDbName(), testTables[i].getTableName());
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

  /**
   * Test where no constraint is present in the table
   * @throws TException
   */
  @Test
  public void noConstraints() throws TException {
    Table table = testTables[0];
    SQLAllTableConstraints constraints = new SQLAllTableConstraints();
    constraints.setPrimaryKeys(new ArrayList<>());
    constraints.setForeignKeys(new ArrayList<>());
    constraints.setNotNullConstraints(new ArrayList<>());
    constraints.setCheckConstraints(new ArrayList<>());
    constraints.setDefaultConstraints(new ArrayList<>());
    constraints.setUniqueConstraints(new ArrayList<>());
    AllTableConstraintsRequest request =
        new AllTableConstraintsRequest(table.getDbName(), table.getTableName(), table.getCatName());
    SQLAllTableConstraints fetched = client.getAllTableConstraints(request);
    Assert.assertEquals(constraints, fetched);
  }

  /**
   * Test where only some of the constraints are present along with multiple values for a single constraint
   * @throws TException
   */
  @Test
  public void fewPresentWithMultipleConstraints() throws TException {
    Table table = testTables[0];
    SQLAllTableConstraints expected = new SQLAllTableConstraints();
    // Set col1 as primary key Constraint in default catalog and database
    List<SQLPrimaryKey> pk = new SQLPrimaryKeyBuilder().onTable(table).addColumn("col1").setConstraintName("col1_pk")
        .build(metaStore.getConf());
    client.addPrimaryKey(pk);
    expected.setPrimaryKeys(pk);

    // Set col2 with Unique Constraint in default catalog and database
    List<SQLUniqueConstraint> uc =
        new SQLUniqueConstraintBuilder().onTable(table).addColumn("col2").setConstraintName("col2_unique")
            .build(metaStore.getConf());
    client.addUniqueConstraint(uc);
    expected.setUniqueConstraints(uc);

    // Set col3 with default Constraint in default catalog and database
    List<SQLDefaultConstraint> dv =
        new SQLDefaultConstraintBuilder().onTable(table).addColumn("col3").setConstraintName("col3_default")
            .setDefaultVal(false).build(metaStore.getConf());
    client.addDefaultConstraint(dv);
    expected.setDefaultConstraints(dv);

    // Set col2 with not null constraint in default catalog and database;
    SQLNotNullConstraint nnCol2 =
        new SQLNotNullConstraint(table.getCatName(), table.getDbName(), table.getTableName(), "col2", "col2_not_null",
            true, true, true);
    SQLNotNullConstraint nnCol3 =
        new SQLNotNullConstraint(table.getCatName(), table.getDbName(), table.getTableName(), "col3", "col3_not_null",
            true, true, true);
    List<SQLNotNullConstraint> nn = new ArrayList<>();
    nn.add(nnCol2);
    nn.add(nnCol3);
    client.addNotNullConstraint(nn);
    expected.setNotNullConstraints(nn);

    expected.setForeignKeys(new ArrayList<>());
    expected.setCheckConstraints(new ArrayList<>());
    // Fetch all constraints for the table in default catalog and database
    AllTableConstraintsRequest request =
        new AllTableConstraintsRequest(table.getDbName(), table.getTableName(), table.getCatName());
    SQLAllTableConstraints fetched = client.getAllTableConstraints(request);
    Assert.assertEquals(expected, fetched);

  }

  /**
   * Test to verify all constraint are present in table
   * @throws TException
   */
  @Test
  public void allConstraintsPresent() throws TException {
    Table table = testTables[0];
    Table parentTable = testTables[1];
    SQLAllTableConstraints expected = new SQLAllTableConstraints();
    // Set col1 as primary key Constraint in default catalog and database
    List<SQLPrimaryKey> pk = new SQLPrimaryKeyBuilder().onTable(table).addColumn("col1").setConstraintName("col1_pk")
        .build(metaStore.getConf());
    client.addPrimaryKey(pk);
    expected.setPrimaryKeys(pk);

    // Set col2 with Unique Constraint in default catalog and database
    String uniqueConstraintName = "col2_unique";
    List<SQLUniqueConstraint> uc =
        new SQLUniqueConstraintBuilder().onTable(table).addColumn("col2").setConstraintName("col2_unique")
            .build(metaStore.getConf());
    client.addUniqueConstraint(uc);
    expected.setUniqueConstraints(uc);

    // Set col3 with default Constraint in default catalog and database
    List<SQLDefaultConstraint> dv =
        new SQLDefaultConstraintBuilder().onTable(table).addColumn("col3").setConstraintName("col3_default")
            .setDefaultVal(false).build(metaStore.getConf());
    client.addDefaultConstraint(dv);
    expected.setDefaultConstraints(dv);

    // Set col3 with not null constraint in default catalog and database;
    List<SQLNotNullConstraint> nn =
        new SQLNotNullConstraintBuilder().onTable(table).addColumn("col3").setConstraintName("col3_not_null")
            .build(metaStore.getConf());
    client.addNotNullConstraint(nn);
    expected.setNotNullConstraints(nn);

    // Set col2 with not check constraint in default catalog and database;
    List<SQLCheckConstraint> cc =
        new SQLCheckConstraintBuilder().onTable(table).addColumn("col2").setConstraintName("col2_check")
            .setCheckExpression("= 5").build(metaStore.getConf());
    client.addCheckConstraint(cc);
    expected.setCheckConstraints(cc);

    // Set col1 of parent table to PK and Set Col4 of table to FK
    List<SQLPrimaryKey> parentPk =
        new SQLPrimaryKeyBuilder().onTable(parentTable).addColumn("col1").setConstraintName("parentpk")
            .build(metaStore.getConf());
    client.addPrimaryKey(parentPk);
    String fkConstraintName = "fk";
    List<SQLForeignKey> fk =
        new SQLForeignKeyBuilder().fromPrimaryKey(parentPk).onTable(table).setConstraintName(fkConstraintName)
            .addColumn("col4").build(metaStore.getConf());
    client.addForeignKey(fk);
    expected.setForeignKeys(fk);

    // Fetch all constraints for the table in default catalog and database
    AllTableConstraintsRequest request =
        new AllTableConstraintsRequest(table.getDbName(), table.getTableName(), table.getCatName());
    SQLAllTableConstraints actual = client.getAllTableConstraints(request);
    Assert.assertEquals(expected, actual);
  }
}