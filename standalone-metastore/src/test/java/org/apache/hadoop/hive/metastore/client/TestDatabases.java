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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.FunctionBuilder;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Test class for IMetaStoreClient API. Testing the Database related functions.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestDatabases {
  private static final Logger LOG = LoggerFactory.getLogger(TestDatabases.class);
  // Needed until there is no junit release with @BeforeParam, @AfterParam (junit 4.13)
  // https://github.com/junit-team/junit4/commit/1bf8438b65858565dbb64736bfe13aae9cfc1b5a
  // Then we should remove our own copy
  private static Set<AbstractMetaStoreService> metaStoreServices = null;
  private static final String DEFAULT_DATABASE = "default";
  private final AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;
  private Database[] testDatabases = new Database[4];

  @Parameterized.Parameters(name = "{0}")
  public static List<Object[]> getMetaStoreToTest() throws Exception {
    List<Object[]> result = MetaStoreFactoryForTests.getMetaStores();
    metaStoreServices = result.stream()
                            .map(test -> (AbstractMetaStoreService)test[1])
                            .collect(Collectors.toSet());
    return result;
  }

  public TestDatabases(String name, AbstractMetaStoreService metaStore) throws Exception {
    this.metaStore = metaStore;
    this.metaStore.start();
  }

  // Needed until there is no junit release with @BeforeParam, @AfterParam (junit 4.13)
  // https://github.com/junit-team/junit4/commit/1bf8438b65858565dbb64736bfe13aae9cfc1b5a
  // Then we should move this to @AfterParam
  @AfterClass
  public static void stopMetaStores() throws Exception {
    for(AbstractMetaStoreService metaStoreService : metaStoreServices) {
      try {
        metaStoreService.stop();
      } catch(Exception e) {
        // Catch the exceptions, so every other metastore could be stopped as well
        // Log it, so at least there is a slight possibility we find out about this :)
        LOG.error("Error stopping MetaStoreService", e);
      }
    }
  }

  @Before
  public void setUp() throws Exception {
    // Get new client
    client = metaStore.getClient();

    // Clean up the databases
    for(String databaseName : client.getAllDatabases()) {
      if (!databaseName.equals(DEFAULT_DATABASE)) {
        client.dropDatabase(databaseName, true, true, true);
      }
    }

    testDatabases[0] =
        new DatabaseBuilder().setName("test_database_1").build();
    testDatabases[1] =
        new DatabaseBuilder().setName("test_database_to_find_1").build();
    testDatabases[2] =
        new DatabaseBuilder().setName("test_database_to_find_2").build();
    testDatabases[3] =
        new DatabaseBuilder().setName("test_database_hidden_1").build();

    // Create the databases, and reload them from the MetaStore
    for(int i=0; i < testDatabases.length; i++) {
      client.createDatabase(testDatabases[i]);
      testDatabases[i] = client.getDatabase(testDatabases[i].getName());
    }
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

  /**
   * This test creates and queries a database and then drops it. Good for testing the happy path.
   * @throws Exception
   */
  @Test
  public void testCreateGetDeleteDatabase() throws Exception {
    Database database = getDatabaseWithAllParametersSet();
    client.createDatabase(database);
    Database createdDatabase = client.getDatabase(database.getName());

    // The createTime will be set on the server side, so the comparison should skip it
    Assert.assertEquals("Comparing databases", database, createdDatabase);
    Assert.assertTrue("The directory should be created", metaStore.isPathExists(
        new Path(database.getLocationUri())));
    client.dropDatabase(database.getName());
    Assert.assertFalse("The directory should be removed",
        metaStore.isPathExists(new Path(database.getLocationUri())));
    try {
      client.getDatabase(database.getName());
      Assert.fail("Expected a NoSuchObjectException to be thrown");
    } catch (NoSuchObjectException exception) {
      // Expected exception
    }
  }

  @Test
  public void testCreateDatabaseDefaultValues() throws Exception {
    Database database = new Database();
    database.setName("dummy");

    client.createDatabase(database);
    Database createdDatabase = client.getDatabase(database.getName());

    Assert.assertNull("Comparing description", createdDatabase.getDescription());
    Assert.assertEquals("Comparing location", metaStore.getWarehouseRoot() + "/" +
                                                  createdDatabase.getName() + ".db", createdDatabase.getLocationUri());
    Assert.assertEquals("Comparing parameters", new HashMap<String, String>(),
        createdDatabase.getParameters());
    Assert.assertNull("Comparing privileges", createdDatabase.getPrivileges());
    Assert.assertNull("Comparing owner name", createdDatabase.getOwnerName());
    Assert.assertEquals("Comparing owner type", PrincipalType.USER, createdDatabase.getOwnerType());
  }

  @Test(expected = MetaException.class)
  public void testCreateDatabaseNullName() throws Exception {
    Database database = testDatabases[0];

    // Missing class setting field
    database.setName(null);

    client.createDatabase(database);
    // Throwing InvalidObjectException would be more appropriate, but we do not change the API
  }

  @Test(expected = InvalidObjectException.class)
  public void testCreateDatabaseInvalidName() throws Exception {
    Database database = testDatabases[0];

    // Invalid character in new database name
    database.setName("test_database_1;");
    client.createDatabase(database);
  }

  @Test(expected = InvalidObjectException.class)
  public void testCreateDatabaseEmptyName() throws Exception {
    Database database = testDatabases[0];

    // Empty new database name
    database.setName("");
    client.createDatabase(database);
    // Throwing InvalidObjectException would be more appropriate, but we do not change the API
  }

  @Test(expected = AlreadyExistsException.class)
  public void testCreateDatabaseAlreadyExists() throws Exception {
    Database database = testDatabases[0];

    // Already existing database
    client.createDatabase(database);
  }

  @Test
  public void testDefaultDatabaseData() throws Exception {
    Database database = client.getDatabase(DEFAULT_DATABASE);
    Assert.assertEquals("Default database name", "default", database.getName());
    Assert.assertEquals("Default database description", "Default Hive database",
        database.getDescription());
    Assert.assertEquals("Default database location", metaStore.getWarehouseRoot(),
        new Path(database.getLocationUri()));
    Assert.assertEquals("Default database parameters", new HashMap<String, String>(),
        database.getParameters());
    Assert.assertEquals("Default database owner", "public", database.getOwnerName());
    Assert.assertEquals("Default database owner type", PrincipalType.ROLE, database.getOwnerType());
    Assert.assertNull("Default database privileges", database.getPrivileges());
  }

  @Test
  public void testGetDatabaseCaseInsensitive() throws Exception {
    Database database = testDatabases[0];

    // Test in upper case
    Database resultUpper = client.getDatabase(database.getName().toUpperCase());
    Assert.assertEquals("Comparing databases", database, resultUpper);

    // Test in mixed case
    Database resultMix = client.getDatabase("teST_dAtABase_1");
    Assert.assertEquals("Comparing databases", database, resultMix);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetDatabaseNoSuchDatabase() throws Exception {
    client.getDatabase("no_such_database");
  }

  @Test
  public void testGetDatabaseNullName() throws Exception {
    // Missing database name in the query
    try {
      client.getDatabase(null);
      // TODO: Should have a check on the server side.
      Assert.fail("Expected a NullPointerException or TTransportException to be thrown");
    } catch (NullPointerException exception) {
      // Expected exception - Embedded MetaStore
    } catch (TTransportException exception) {
      // Expected exception - Remote MetaStore
    }
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDropDatabaseNoSuchDatabase() throws Exception {
    client.dropDatabase("no_such_database");
  }

  @Test
  public void testDropDatabaseNullName() throws Exception {
    // Missing database in the query
    try {
      client.dropDatabase(null);
      // TODO: Should be checked on server side
      Assert.fail("Expected an NullPointerException or TTransportException to be thrown");
    } catch (NullPointerException exception) {
      // Expected exception - Embedded MetaStore
    } catch (TTransportException exception) {
      // Expected exception - Remote MetaStore
    }
  }

  @Test
  public void testDropDatabaseDefaultDatabase() throws Exception {
    // Check if it is possible to drop default database
    try {
      client.dropDatabase(DEFAULT_DATABASE);
      // TODO: Should be checked on server side
      Assert.fail("Expected an MetaException or TTransportException to be thrown");
    } catch (MetaException exception) {
      // Expected exception - Embedded MetaStore
    } catch (TTransportException exception) {
      // Expected exception - Remote MetaStore
    }
  }

  @Test
  public void testDropDatabaseCaseInsensitive() throws Exception {
    Database database = testDatabases[0];

    // Test in upper case
    client.dropDatabase(database.getName().toUpperCase());
    List<String> allDatabases = client.getAllDatabases();
    Assert.assertEquals("All databases size", 4, allDatabases.size());

    // Test in mixed case
    client.createDatabase(database);
    client.dropDatabase("TesT_DatABaSe_1");
    allDatabases = client.getAllDatabases();
    Assert.assertEquals("All databases size", 4, allDatabases.size());
  }

  @Test
  public void testDropDatabaseDeleteData() throws Exception {
    Database database = testDatabases[0];
    Path dataFile = new Path(database.getLocationUri().toString() + "/dataFile");
    metaStore.createFile(dataFile, "100");

    // Do not delete the data
    client.dropDatabase(database.getName(), false, false);
    // Check that the data still exist
    Assert.assertTrue("The data file should still exist", metaStore.isPathExists(dataFile));

    // Recreate the database
    client.createDatabase(database);
    Assert.assertTrue("The data file should still exist", metaStore.isPathExists(dataFile));

    // Delete the data
    client.dropDatabase(database.getName(), true, false);
    // Check that the data is removed
    Assert.assertFalse("The data file should not exist", metaStore.isPathExists(dataFile));
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDropDatabaseIgnoreUnknownFalse() throws Exception {
    // No such database
    client.dropDatabase("no_such_database", false, false);
  }

  @Test
  public void testDropDatabaseIgnoreUnknownTrue() throws Exception {
    // No such database
    client.dropDatabase("no_such_database", false, true);
  }

  @Test(expected = InvalidOperationException.class)
  public void testDropDatabaseWithTable() throws Exception {
    Database database = testDatabases[0];
    Table testTable =
        new TableBuilder()
            .setDbName(database.getName())
            .setTableName("test_table")
            .addCol("test_col", "int")
            .build();
    client.createTable(testTable);

    client.dropDatabase(database.getName(), true, true, false);
  }

  @Test
  public void testDropDatabaseWithTableCascade() throws Exception {
    Database database = testDatabases[0];
    Table testTable =
        new TableBuilder()
            .setDbName(database.getName())
            .setTableName("test_table")
            .addCol("test_col", "int")
            .build();
    client.createTable(testTable);

    client.dropDatabase(database.getName(), true, true, true);
    Assert.assertFalse("The directory should be removed",
        metaStore.isPathExists(new Path(database.getLocationUri())));
  }

  @Test(expected = InvalidOperationException.class)
  public void testDropDatabaseWithFunction() throws Exception {
    Database database = testDatabases[0];

    Function testFunction =
        new FunctionBuilder()
            .setDbName(database.getName())
            .setName("test_function")
            .setClass("org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper")
            .build();

    client.createFunction(testFunction);

    client.dropDatabase(database.getName(), true, true, false);
  }

  @Test
  public void testDropDatabaseWithFunctionCascade() throws Exception {
    Database database = testDatabases[0];

    Function testFunction =
        new FunctionBuilder()
            .setDbName(database.getName())
            .setName("test_function")
            .setClass("org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper")
            .build();

    client.createFunction(testFunction);

    client.dropDatabase(database.getName(), true, true, true);
    Assert.assertFalse("The directory should be removed",
        metaStore.isPathExists(new Path(database.getLocationUri())));
  }

  /**
   * Creates an index in the given database for testing purposes.
   * @param databaseName The database name in which the index should be creatd
   * @throws TException If there is an error during the index creation
   */
  private void createIndex(String databaseName) throws TException {
    Table testTable =
        new TableBuilder()
            .setDbName(databaseName)
            .setTableName("test_table")
            .addCol("test_col", "int")
            .build();

    Index testIndex =
        new IndexBuilder()
            .setIndexName("test_index")
            .setIndexTableName("test_index_table")
            .setDbAndTableName(testTable)
            .addCol("test_col", "int")
            .build();
    Table testIndexTable =
        new TableBuilder()
            .setDbName(databaseName)
            .setType(TableType.INDEX_TABLE.name())
            .setTableName("test_index_table")
            .addCol("test_col", "int")
            .build();

    // Drop database with index
    client.createTable(testTable);
    client.createIndex(testIndex, testIndexTable);
  }

  @Test
  public void testDropDatabaseWithIndex() throws Exception {
    Database database = testDatabases[0];
    createIndex(database.getName());

    // TODO: Known error, should be fixed
    // client.dropDatabase(database.getName(), true, true, true);
    // Need to drop index to clean up the mess
    try {
      // Without cascade
      client.dropDatabase(database.getName(), true, true, false);
      Assert.fail("Expected an InvalidOperationException to be thrown");
    } catch (InvalidOperationException exception) {
      // Expected exception
    }
    client.dropIndex(database.getName(), "test_table", "test_index", true);
    // TODO: End index hack
  }

  @Test
  public void testDropDatabaseWithIndexCascade() throws Exception {
    Database database = testDatabases[0];
    createIndex(database.getName());

    // With cascade
    // TODO: Known error, should be fixed
    // client.dropDatabase(database.getName(), true, true, true);
    // Need to drop index to clean up the mess
    client.dropIndex(database.getName(), "test_table", "test_index", true);
    client.dropDatabase(database.getName(), true, true, true);
    Assert.assertFalse("The directory should be removed",
        metaStore.isPathExists(new Path(database.getLocationUri())));
  }

  @Test
  public void testGetAllDatabases() throws Exception {
    List<String> allDatabases = client.getAllDatabases();
    Assert.assertEquals("All databases size", 5, allDatabases.size());
    for(Database database : testDatabases) {
      Assert.assertTrue("Checking database names", allDatabases.contains(database.getName()));
    }
    Assert.assertTrue("Checnking that default database is returned",
        allDatabases.contains(DEFAULT_DATABASE));

    // Drop one database, see what remains
    client.dropDatabase(testDatabases[1].getName());
    allDatabases = client.getAllDatabases();
    Assert.assertEquals("All databases size", 4, allDatabases.size());
    for(Database database : testDatabases) {
      if (!database.getName().equals(testDatabases[1].getName())) {
        Assert.assertTrue("Checking database names", allDatabases.contains(database.getName()));
      }
    }
    Assert.assertTrue("Checnking that default database is returned",
        allDatabases.contains(DEFAULT_DATABASE));
    Assert.assertFalse("Checking that the deleted database is not returned",
        allDatabases.contains(testDatabases[1].getName()));
  }

  @Test
  public void testGetDatabases() throws Exception {
    // Find databases which name contains _to_find_
    List<String> databases = client.getDatabases("*_to_find_*");
    Assert.assertEquals("Found databases size", 2, databases.size());
    Assert.assertTrue("Should contain", databases.contains("test_database_to_find_1"));
    Assert.assertTrue("Should contain", databases.contains("test_database_to_find_2"));

    // Find databases which name contains _to_find_ or _hidden_
    databases = client.getDatabases("*_to_find_*|*_hidden_*");
    Assert.assertEquals("Found databases size", 3, databases.size());
    Assert.assertTrue("Should contain", databases.contains("test_database_to_find_1"));
    Assert.assertTrue("Should contain", databases.contains("test_database_to_find_2"));
    Assert.assertTrue("Should contain", databases.contains("test_database_hidden_1"));

    // Look for databases but do not find any
    databases = client.getDatabases("*_not_such_database_*");
    Assert.assertEquals("No such databases size", 0, databases.size());

    // Look for databases without pattern
    databases = client.getDatabases(null);
    Assert.assertEquals("Search databases without pattern size", 5, databases.size());
  }

  @Test
  public void testGetDatabasesCaseInsensitive() throws Exception {
    // Check case insensitive search
    List<String> databases = client.getDatabases("*_tO_FiND*");
    Assert.assertEquals("Found databases size", 2, databases.size());
    Assert.assertTrue("Should contain", databases.contains("test_database_to_find_1"));
    Assert.assertTrue("Should contain", databases.contains("test_database_to_find_2"));
  }

  @Test
  public void testAlterDatabase() throws Exception {
    Database originalDatabase = testDatabases[0];
    Database newDatabase =
        new DatabaseBuilder()
            // The database name is not changed during alter
            .setName(originalDatabase.getName())
            .setOwnerType(PrincipalType.GROUP)
            .setOwnerName("owner2")
            .setLocation(metaStore.getWarehouseRoot() + "/database_location_2")
            .setDescription("dummy description 2")
            .addParam("param_key_1", "param_value_1_2")
            .addParam("param_key_2_3", "param_value_2_3")
            .build();

    client.alterDatabase(originalDatabase.getName(), newDatabase);
    Database alteredDatabase = client.getDatabase(newDatabase.getName());
    Assert.assertEquals("Comparing Databases", newDatabase, alteredDatabase);
  }

  @Test
  public void testAlterDatabaseNotNullableFields() throws Exception {
    Database database = getDatabaseWithAllParametersSet();
    client.createDatabase(database);
    Database originalDatabase = client.getDatabase(database.getName());
    Database newDatabase = new Database();
    newDatabase.setName("new_name");

    client.alterDatabase(originalDatabase.getName(), newDatabase);
    // The name should not be changed, so reload the db with the original name
    Database alteredDatabase = client.getDatabase(originalDatabase.getName());
    Assert.assertEquals("Database name should not change", originalDatabase.getName(),
        alteredDatabase.getName());
    Assert.assertEquals("Database description should not change", originalDatabase.getDescription(),
        alteredDatabase.getDescription());
    Assert.assertEquals("Database location should not change", originalDatabase.getLocationUri(),
        alteredDatabase.getLocationUri());
    Assert.assertEquals("Database parameters should be empty", new HashMap<String, String>(),
        alteredDatabase.getParameters());
    Assert.assertNull("Database owner should be empty", alteredDatabase.getOwnerName());
    Assert.assertEquals("Database owner type should not change", originalDatabase.getOwnerType(),
        alteredDatabase.getOwnerType());
    Assert.assertNull("Database privileges should be empty", alteredDatabase.getPrivileges());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testAlterDatabaseNoSuchDatabase() throws Exception {
    Database newDatabase = new DatabaseBuilder().setName("test_database_altered").build();

    client.alterDatabase("no_such_database", newDatabase);
  }

  @Test
  public void testAlterDatabaseCaseInsensitive() throws Exception {
    Database originalDatabase = testDatabases[0];
    Database newDatabase = originalDatabase.deepCopy();
    newDatabase.setDescription("Altered database");

    // Test in upper case
    client.alterDatabase(originalDatabase.getName().toUpperCase(), newDatabase);
    Database alteredDatabase = client.getDatabase(newDatabase.getName());
    Assert.assertEquals("Comparing databases", newDatabase, alteredDatabase);

    // Test in mixed case
    originalDatabase = testDatabases[2];
    newDatabase = originalDatabase.deepCopy();
    newDatabase.setDescription("Altered database 2");
    client.alterDatabase("TeST_daTAbaSe_TO_FiNd_2", newDatabase);
    alteredDatabase = client.getDatabase(newDatabase.getName());
    Assert.assertEquals("Comparing databases", newDatabase, alteredDatabase);
  }

  private Database getDatabaseWithAllParametersSet() throws Exception {
    return new DatabaseBuilder()
               .setName("dummy")
               .setOwnerType(PrincipalType.ROLE)
               .setOwnerName("owner")
               .setLocation(metaStore.getWarehouseRoot() + "/database_location")
               .setDescription("dummy description")
               .addParam("param_key_1", "param_value_1")
               .addParam("param_key_2", "param_value_2")
               .build();
  }
}
