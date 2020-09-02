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
import org.apache.hadoop.hive.metastore.api.Database;
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
  private Table[] testTables = new Table[3];

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

    // For this one don't specify a location to make sure it gets put in the catalog directory
    Database inOtherCatalog = new DatabaseBuilder().setName(DATABASE_IN_OTHER_CATALOG).setCatalogName(OTHER_CATALOG)
        .create(client, metaStore.getConf());

    testTables[0] = new TableBuilder().setTableName("test_table_1").addCol("col1", "int").addCol("col2", "int")
        .addCol("col3", "boolean").addCol("col4", "int").addCol("col5", "varchar(32)")
        .create(client, metaStore.getConf());

    testTables[1] = new TableBuilder().setDbName(OTHER_DATABASE).setTableName("test_table_2").addCol("col1", "int")
        .addCol("col2", "varchar(32)").create(client, metaStore.getConf());

    testTables[2] = new TableBuilder().inDb(inOtherCatalog).setTableName("test_table_3").addCol("col1", "int")
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

    AllTableConstraintsRequest request = new AllTableConstraintsRequest(table.getDbName(), table.getTableName());
    request.setCatName(table.getCatName());
    SQLAllTableConstraints fetched = client.getAllTableConstraints(request);

    Assert.assertTrue(fetched.getCheckConstraints().isEmpty());
    Assert.assertTrue(fetched.getForeignKeys().isEmpty());
    Assert.assertTrue(fetched.getDefaultConstraints().isEmpty());
    Assert.assertTrue(fetched.getNotNullConstraints().isEmpty());
    Assert.assertTrue(fetched.getPrimaryKeys().isEmpty());
    Assert.assertTrue(fetched.getUniqueConstraints().isEmpty());
  }

  /**
   * Test where only some of the constraints are present along with multiple values for a single constraint
   * @throws TException
   */
  @Test
  public void fewPresentWithMultipleConstraints() throws TException {
    Table table = testTables[0];

    // Set col1 as primary key Constraint in default catalog and database
    String pkConstraintName = "col1_pk";
    List<SQLPrimaryKey> pk =
        new SQLPrimaryKeyBuilder().onTable(table).addColumn("col1").setConstraintName(pkConstraintName)
            .build(metaStore.getConf());
    client.addPrimaryKey(pk);

    // Set col2 with Unique Constraint in default catalog and database
    String uniqueConstraintName = "col2_unique";
    List<SQLUniqueConstraint> uc =
        new SQLUniqueConstraintBuilder().onTable(table).addColumn("col2").setConstraintName(uniqueConstraintName)
            .build(metaStore.getConf());
    client.addUniqueConstraint(uc);

    // Set col3 with default Constraint in default catalog and database
    String defaultConstraintName = "col3_default";
    List<SQLDefaultConstraint> dv =
        new SQLDefaultConstraintBuilder().onTable(table).addColumn("col3").setConstraintName(defaultConstraintName)
            .setDefaultVal(false).build(metaStore.getConf());
    client.addDefaultConstraint(dv);

    // Set col2 with not null constraint in default catalog and database;
    String nnCol2ConstraintName = "col2_not_null";
    List<SQLNotNullConstraint> nnCol2 =
        new SQLNotNullConstraintBuilder().onTable(table).addColumn("col2").setConstraintName(nnCol2ConstraintName)
            .build(metaStore.getConf());
    client.addNotNullConstraint(nnCol2);

    // Set col3 with not null constraint in default catalog and database;
    String nnCol3ConstraintName = "col3_not_null";
    List<SQLNotNullConstraint> nnCol3 =
        new SQLNotNullConstraintBuilder().onTable(table).addColumn("col3").setConstraintName(nnCol3ConstraintName)
            .build(metaStore.getConf());
    client.addNotNullConstraint(nnCol3);

    // Fetch all constraints for the table in default catalog and database
    AllTableConstraintsRequest request = new AllTableConstraintsRequest(table.getDbName(), table.getTableName());
    request.setCatName(table.getCatName());
    SQLAllTableConstraints fetched = client.getAllTableConstraints(request);

    // Assert primary key constraint
    Assert.assertEquals(1, fetched.getPrimaryKeysSize());
    verifyPrimaryKey(fetched.getPrimaryKeys().get(0), table, "col1", 1, pkConstraintName);

    // Assert unique constraint
    Assert.assertEquals(1, fetched.getUniqueConstraintsSize());
    verifyUniqueConstraints(fetched.getUniqueConstraints().get(0), table, "col2", 1, uniqueConstraintName);

    // Assert Default constraint
    Assert.assertEquals(1, fetched.getDefaultConstraintsSize());
    verifyDefaultConstraints(fetched.getDefaultConstraints().get(0), table, "col3", "false", defaultConstraintName);

    // Assert Not Null constraint
    Assert.assertEquals(2, fetched.getNotNullConstraintsSize());
    verifyNotNullConstraints(fetched.getNotNullConstraints().get(0), table, "col2", nnCol2ConstraintName);
    verifyNotNullConstraints(fetched.getNotNullConstraints().get(1), table, "col3", nnCol3ConstraintName);

    // Check constraints which is not present in table
    Assert.assertTrue(fetched.getCheckConstraints().isEmpty());
    Assert.assertTrue(fetched.getForeignKeys().isEmpty());

  }

  /**
   * Test to verify all constraint are present in table
   * @throws TException
   */
  @Test
  public void allConstraintsPresent() throws TException {
    Table table = testTables[0];
    Table parentTable = testTables[1];

    // Set col1 as primary key Constraint in default catalog and database
    String pkConstraintName = "col1_pk";
    List<SQLPrimaryKey> pk =
        new SQLPrimaryKeyBuilder().onTable(table).addColumn("col1").setConstraintName(pkConstraintName)
            .build(metaStore.getConf());
    client.addPrimaryKey(pk);

    // Set col2 with Unique Constraint in default catalog and database
    String uniqueConstraintName = "col2_unique";
    List<SQLUniqueConstraint> uc =
        new SQLUniqueConstraintBuilder().onTable(table).addColumn("col2").setConstraintName(uniqueConstraintName)
            .build(metaStore.getConf());
    client.addUniqueConstraint(uc);

    // Set col3 with default Constraint in default catalog and database
    String defaultConstraintName = "col3_default";
    List<SQLDefaultConstraint> dv =
        new SQLDefaultConstraintBuilder().onTable(table).addColumn("col3").setConstraintName(defaultConstraintName)
            .setDefaultVal(false).build(metaStore.getConf());
    client.addDefaultConstraint(dv);

    // Set col3 with not null constraint in default catalog and database;
    String nnCol3ConstraintName = "col3_not_null";
    List<SQLNotNullConstraint> nnCol2 =
        new SQLNotNullConstraintBuilder().onTable(table).addColumn("col3").setConstraintName(nnCol3ConstraintName)
            .build(metaStore.getConf());
    client.addNotNullConstraint(nnCol2);

    // Set col2 with not check constraint in default catalog and database;
    String ccCol2ConstraintName = "col2_check";
    List<SQLCheckConstraint> cc =
        new SQLCheckConstraintBuilder().onTable(table).addColumn("col2").setConstraintName(ccCol2ConstraintName)
            .setCheckExpression("= 5").build(metaStore.getConf());
    client.addCheckConstraint(cc);

    // Set col1 of parent table to PK and Set Col4 of table to FK
    String parentPkConstraintName = "parentpk";
    List<SQLPrimaryKey> parentPk =
        new SQLPrimaryKeyBuilder().onTable(parentTable).addColumn("col1").setConstraintName(parentPkConstraintName)
            .build(metaStore.getConf());
    client.addPrimaryKey(parentPk);
    String fkConstraintName = "fk";
    List<SQLForeignKey> fk =
        new SQLForeignKeyBuilder().fromPrimaryKey(parentPk).onTable(table).setConstraintName(fkConstraintName)
            .addColumn("col4").build(metaStore.getConf());
    client.addForeignKey(fk);

    // Fetch all constraints for the table in default catalog and database
    AllTableConstraintsRequest request = new AllTableConstraintsRequest(table.getDbName(), table.getTableName());
    request.setCatName(table.getCatName());
    SQLAllTableConstraints fetched = client.getAllTableConstraints(request);

    // Assert primary key constraint
    Assert.assertEquals(1, fetched.getPrimaryKeysSize());
    verifyPrimaryKey(fetched.getPrimaryKeys().get(0), table, "col1", 1, pkConstraintName);

    // Assert unique constraint
    Assert.assertEquals(1, fetched.getUniqueConstraintsSize());
    verifyUniqueConstraints(fetched.getUniqueConstraints().get(0), table, "col2", 1, uniqueConstraintName);

    // Assert Default constraint
    Assert.assertEquals(1, fetched.getDefaultConstraintsSize());
    verifyDefaultConstraints(fetched.getDefaultConstraints().get(0), table, "col3", "false", defaultConstraintName);

    // Assert Not Null constraint
    Assert.assertEquals(1, fetched.getNotNullConstraintsSize());
    verifyNotNullConstraints(fetched.getNotNullConstraints().get(0), table, "col3", nnCol3ConstraintName);

    // Assert check constraint
    Assert.assertEquals(1, fetched.getNotNullConstraintsSize());
    verifyCheckConstraints(fetched.getCheckConstraints().get(0), table, "col2", "= 5", ccCol2ConstraintName);

    // Assert foreign key
    Assert.assertEquals(1, fetched.getForeignKeysSize());
    verifyForeignKey(fetched.getForeignKeys().get(0), parentTable, "col1", parentPkConstraintName, table, "col4", 1,
        fkConstraintName);

  }

  /**
   * Asset/verify expected Primary key with fetched primary key
   * @param primaryKey fetched primaryKey
   * @param table expected table
   * @param column expected primaryKey Column name
   * @param seq expected primaryKey seq
   * @param pkConstraintName expected constraint name
   */
  public void verifyPrimaryKey(SQLPrimaryKey primaryKey, Table table, String column, int seq, String pkConstraintName) {
    Assert.assertEquals(table.getDbName(), primaryKey.getTable_db());
    Assert.assertEquals(table.getTableName(), primaryKey.getTable_name());
    Assert.assertEquals(column, primaryKey.getColumn_name());
    Assert.assertEquals(seq, primaryKey.getKey_seq());
    Assert.assertEquals(pkConstraintName, primaryKey.getPk_name());
    Assert.assertTrue(primaryKey.isEnable_cstr());
    Assert.assertFalse(primaryKey.isValidate_cstr());
    Assert.assertFalse(primaryKey.isRely_cstr());
    Assert.assertEquals(table.getCatName(), primaryKey.getCatName());
  }

  /**
   * Asset/verify expected unique constraint with fetched unique constraint
   * @param uniqueConstraint fetched unique constraint
   * @param table expected table name
   * @param column expected column name
   * @param seq expected seq
   * @param uniqueConstraintName expected unique constraint name
   */
  public void verifyUniqueConstraints(SQLUniqueConstraint uniqueConstraint, Table table, String column, int seq,
      String uniqueConstraintName) {
    Assert.assertEquals(table.getDbName(), uniqueConstraint.getTable_db());
    Assert.assertEquals(table.getTableName(), uniqueConstraint.getTable_name());
    Assert.assertEquals(column, uniqueConstraint.getColumn_name());
    Assert.assertEquals(seq, uniqueConstraint.getKey_seq());
    Assert.assertEquals(uniqueConstraintName, uniqueConstraint.getUk_name());
    Assert.assertTrue(uniqueConstraint.isEnable_cstr());
    Assert.assertFalse(uniqueConstraint.isValidate_cstr());
    Assert.assertFalse(uniqueConstraint.isRely_cstr());
    Assert.assertEquals(table.getCatName(), uniqueConstraint.getCatName());
  }

  /**
   * Assert/verify expected default constraint with fetched default constraint
   * @param defaultConstraint fetched default constraint
   * @param table expected table name
   * @param column expected column name
   * @param defaultValue expected column default value
   * @param defaultConstraintName expected default constraint name
   */
  public void verifyDefaultConstraints(SQLDefaultConstraint defaultConstraint, Table table, String column,
      String defaultValue, String defaultConstraintName) {
    Assert.assertEquals(table.getDbName(), defaultConstraint.getTable_db());
    Assert.assertEquals(table.getTableName(), defaultConstraint.getTable_name());
    Assert.assertEquals(column, defaultConstraint.getColumn_name());
    Assert.assertEquals(defaultValue, defaultConstraint.getDefault_value());
    Assert.assertEquals(defaultConstraintName, defaultConstraint.getDc_name());
    Assert.assertTrue(defaultConstraint.isEnable_cstr());
    Assert.assertFalse(defaultConstraint.isValidate_cstr());
    Assert.assertFalse(defaultConstraint.isRely_cstr());
    Assert.assertEquals(table.getCatName(), defaultConstraint.getCatName());
  }

  /**
   * Assert/verify expected not null constraint with fetched not null constraint
   * @param notNullConstraint fetched not null constraint
   * @param table expected table name
   * @param column expected column name
   * @param notNullConstraintName expected not null constraint name
   */
  public void verifyNotNullConstraints(SQLNotNullConstraint notNullConstraint, Table table, String column,
      String notNullConstraintName) {
    Assert.assertEquals(table.getDbName(), notNullConstraint.getTable_db());
    Assert.assertEquals(table.getTableName(), notNullConstraint.getTable_name());
    Assert.assertEquals(column, notNullConstraint.getColumn_name());
    Assert.assertEquals(notNullConstraintName, notNullConstraint.getNn_name());
    Assert.assertTrue(notNullConstraint.isEnable_cstr());
    Assert.assertFalse(notNullConstraint.isValidate_cstr());
    Assert.assertFalse(notNullConstraint.isRely_cstr());
    Assert.assertEquals(table.getCatName(), notNullConstraint.getCatName());
  }

  /**
   * Assert/verify expected check constraint with fetched check constraint
   * @param checkConstraint fetched check constraint
   * @param table expected table name
   * @param column expected column name
   * @param checkValue expected check value
   * @param checkConstraintName expected check constraint name
   */
  public void verifyCheckConstraints(SQLCheckConstraint checkConstraint, Table table, String column, String checkValue,
      String checkConstraintName) {
    Assert.assertEquals(table.getDbName(), checkConstraint.getTable_db());
    Assert.assertEquals(table.getTableName(), checkConstraint.getTable_name());
    Assert.assertEquals(column, checkConstraint.getColumn_name());
    Assert.assertEquals(checkValue, checkConstraint.getCheck_expression());
    Assert.assertEquals(checkConstraintName, checkConstraint.getDc_name());
    Assert.assertTrue(checkConstraint.isEnable_cstr());
    Assert.assertFalse(checkConstraint.isValidate_cstr());
    Assert.assertFalse(checkConstraint.isRely_cstr());
    Assert.assertEquals(table.getCatName(), checkConstraint.getCatName());
  }

  /**
   * Assert/verify expected foreign key with fetched foreign key
   * @param foreignKey fetched foreign key
   * @param parentTable expected parent table name
   * @param parentPkColumn expected primary key column name in parent table
   * @param parentPkConstraintName expected primary key constraint name in parent table
   * @param fkTable expected foreign key table name
   * @param fkColumn expected foreign key column name
   * @param fkSeq expected foreign key seq
   * @param fkConstraintName expected foreign key constraint name
   */
  public void verifyForeignKey(SQLForeignKey foreignKey, Table parentTable, String parentPkColumn,
      String parentPkConstraintName, Table fkTable, String fkColumn, int fkSeq, String fkConstraintName) {
    Assert.assertEquals(fkTable.getDbName(), foreignKey.getFktable_db());
    Assert.assertEquals(fkTable.getTableName(), foreignKey.getFktable_name());
    Assert.assertEquals(fkColumn, foreignKey.getFkcolumn_name());
    Assert.assertEquals(parentTable.getDbName(), foreignKey.getPktable_db());
    Assert.assertEquals(parentTable.getTableName(), foreignKey.getPktable_name());
    Assert.assertEquals(parentPkColumn, foreignKey.getPkcolumn_name());
    Assert.assertEquals(fkSeq, foreignKey.getKey_seq());
    Assert.assertEquals(parentPkConstraintName, foreignKey.getPk_name());
    Assert.assertEquals(fkConstraintName, foreignKey.getFk_name());
    Assert.assertTrue(foreignKey.isEnable_cstr());
    Assert.assertFalse(foreignKey.isValidate_cstr());
    Assert.assertFalse(foreignKey.isRely_cstr());
    Assert.assertEquals(fkTable.getCatName(), foreignKey.getCatName());
  }
}