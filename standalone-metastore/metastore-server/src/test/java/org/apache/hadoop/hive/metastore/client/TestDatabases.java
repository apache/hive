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
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.FunctionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;

/**
 * Test class for IMetaStoreClient API. Testing the Database related functions.
 */

@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestDatabases extends MetaStoreClientTest {
  private static final String DEFAULT_DATABASE = "default";
  private final AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;
  private Database[] testDatabases = new Database[4];

  public TestDatabases(String name, AbstractMetaStoreService metaStore) {
    this.metaStore = metaStore;
  }

  @Before
  public void setUp() throws Exception {
    HiveMetaStoreClient.setProcessorIdentifier("UnitTest@TestDatabases");
    HiveMetaStoreClient.setProcessorCapabilities(new String[] {
        "HIVEFULLACIDREAD",
        "EXTWRITE",
        "EXTREAD",
        "HIVEBUCKET2"
    } );

    // Get new client
    client = metaStore.getClient();

    // Clean up the databases
    for(String databaseName : client.getAllDatabases()) {
      if (!databaseName.equals(DEFAULT_DATABASE)) {
        client.dropDatabase(databaseName, true, true, true);
      }
    }

    testDatabases[0] =
        new DatabaseBuilder().setName("test_database_1").create(client, metaStore.getConf());
    testDatabases[1] =
        new DatabaseBuilder().setName("test_database_to_find_1").create(client, metaStore.getConf());
    testDatabases[2] =
        new DatabaseBuilder().setName("test_database_to_find_2").create(client, metaStore.getConf());
    testDatabases[3] =
        new DatabaseBuilder().setName("test_database_hidden_1").create(client, metaStore.getConf());

    // Create the databases, and reload them from the MetaStore
    for (int i=0; i < testDatabases.length; i++) {
      testDatabases[i] = client.getDatabase(testDatabases[i].getName());
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
   * This test creates and queries a database and then drops it. Good for testing the happy path.
   */
  @Test
  public void testCreateGetDeleteDatabase() throws Exception {
    Database database = getDatabaseWithAllParametersSet();
    client.createDatabase(database);
    Database createdDatabase = client.getDatabase(database.getName());

    // The createTime will be set on the server side, so the comparison should skip it
    database.setCreateTime(createdDatabase.getCreateTime());
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
    Database database = new DatabaseBuilder()
        .setName("dummy")
        .create(client, metaStore.getConf());

    Database createdDatabase = client.getDatabase(database.getName());

    Assert.assertNull("Comparing description", createdDatabase.getDescription());
    Assert.assertEquals("Comparing location", metaStore.getExternalWarehouseRoot() + "/" +
                                                  createdDatabase.getName() + ".db", createdDatabase.getLocationUri());
    Assert.assertEquals("Comparing parameters", new HashMap<String, String>(),
        createdDatabase.getParameters());
    Assert.assertNull("Comparing privileges", createdDatabase.getPrivileges());
    Assert.assertEquals("Comparing owner name", SecurityUtils.getUser(),
        createdDatabase.getOwnerName());
    Assert.assertEquals("Comparing owner type", PrincipalType.USER, createdDatabase.getOwnerType());
  }

  @Test
  public void testCreateDatabaseOwnerName() throws Exception{
    DatabaseBuilder databaseBuilder = new DatabaseBuilder()
        .setCatalogName("hive")
        .setName("dummy")
        .setOwnerName(null);

    Database db = databaseBuilder.create(client, metaStore.getConf());
    Assert.assertNotNull("Owner name should be filled", db.getOwnerName());
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
    database.setName("test_database§1;");
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
    Assert.assertEquals("Default database location", metaStore.getExternalWarehouseRoot(),
        new Path(database.getLocationUri()));
    Assert.assertEquals("Default database parameters", new HashMap<String, String>(),
        database.getParameters());
    Assert.assertEquals("Default database owner", "public", database.getOwnerName());
    Assert.assertEquals("Default database owner type", PrincipalType.ROLE, database.getOwnerType());
    Assert.assertNull("Default database privileges", database.getPrivileges());
    Assert.assertTrue("database create time should be set", database.isSetCreateTime());
    Assert.assertTrue("Database create time should be non-zero", database.getCreateTime() > 0);
  }

  @Test
  public void testDatabaseCreateTime() throws Exception {
    // create db without specifying createtime
    Database testDb =
        new DatabaseBuilder().setName("test_create_time").create(client, metaStore.getConf());
    Database database = client.getDatabase("test_create_time");
    Assert.assertTrue("Database create time should have been set",
        database.getCreateTime() > 0);
  }

  @Test
  public void testDbCreateTimeOverride() throws Exception {
    // create db by providing a create time. Should be overridden, create time should
    // always be set by metastore
    Database testDb =
        new DatabaseBuilder().setName("test_create_time")
            .setCreateTime(1)
            .create(client, metaStore.getConf());
    Database database = client.getDatabase("test_create_time");
    Assert.assertTrue("Database create time should have been set",
        database.getCreateTime() > 0);
    Assert.assertTrue("Database create time should have been reset by metastore",
        database.getCreateTime() != 1);
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

  @Test(expected = MetaException.class)
  public void testGetDatabaseNullName() throws Exception {
    // Missing database name in the query
    client.getDatabase(null);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDropDatabaseNoSuchDatabase() throws Exception {
    client.dropDatabase("no_such_database");
  }

  @Test(expected = MetaException.class)
  public void testDropDatabaseNullName() throws Exception {
    // Missing database in the query
    client.dropDatabase((String) null);
  }

  @Test(expected = MetaException.class)
  public void testDropDatabaseDefaultDatabase() throws Exception {
    // Check if it is possible to drop default database
    client.dropDatabase(DEFAULT_DATABASE);
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
    Path dataFile = new Path(database.getLocationUri() + "/dataFile");
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
            .create(client, metaStore.getConf());

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
            .create(client, metaStore.getConf());

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
            .create(client, metaStore.getConf());


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
            .create(client, metaStore.getConf());


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
            .setLocation(metaStore.getExternalWarehouseRoot() + "/database_location_2")
            .setDescription("dummy description 2")
            .addParam("param_key_1", "param_value_1_2")
            .addParam("param_key_2_3", "param_value_2_3")
            .setCreateTime(originalDatabase.getCreateTime())
            .build(metaStore.getConf());

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
    newDatabase.setCatalogName(DEFAULT_CATALOG_NAME);

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
    Database newDatabase = new DatabaseBuilder()
        .setName("test_database_altered")
        .build(metaStore.getConf());

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

  @Test
  public void databasesInCatalogs() throws TException, URISyntaxException {
    String catName = "mycatalog";
    Catalog cat = new CatalogBuilder()
        .setName(catName)
        .setLocation(MetaStoreTestUtils.getTestWarehouseDir(catName))
        .build();
    client.createCatalog(cat);

    String[] dbNames = {"db1", "db9"};
    Database[] dbs = new Database[2];
    // For this one don't specify a location to make sure it gets put in the catalog directory
    dbs[0] = new DatabaseBuilder()
        .setName(dbNames[0])
        .setCatalogName(catName)
        .create(client, metaStore.getConf());

    // For the second one, explicitly set a location to make sure it ends up in the specified place.
    String db1Location = MetaStoreTestUtils.getTestWarehouseDir(dbNames[1]);
    dbs[1] = new DatabaseBuilder()
        .setName(dbNames[1])
        .setCatalogName(catName)
        .setLocation(db1Location)
        .create(client, metaStore.getConf());

    Database fetched = client.getDatabase(catName, dbNames[0]);
    String expectedLocation = new File(cat.getLocationUri(), dbNames[0] + ".db").toURI().toString();
    Assert.assertEquals(expectedLocation, fetched.getLocationUri() + "/");
    String db0Location = new URI(fetched.getLocationUri()).getPath();
    File dir = new File(db0Location);
    Assert.assertTrue(dir.exists() && dir.isDirectory());

    fetched = client.getDatabase(catName, dbNames[1]);
    Assert.assertEquals(new File(db1Location).toURI().toString(), fetched.getLocationUri() + "/");
    dir = new File(new URI(fetched.getLocationUri()).getPath());
    Assert.assertTrue(dir.exists() && dir.isDirectory());

    Set<String> fetchedDbs = new HashSet<>(client.getAllDatabases(catName));
    Assert.assertEquals(3, fetchedDbs.size());
    for (String dbName : dbNames) Assert.assertTrue(fetchedDbs.contains(dbName));

    fetchedDbs = new HashSet<>(client.getAllDatabases());
    Assert.assertEquals(5, fetchedDbs.size());
    Assert.assertTrue(fetchedDbs.contains(Warehouse.DEFAULT_DATABASE_NAME));

    // Intentionally using the deprecated method to make sure it returns correct results.
    fetchedDbs = new HashSet<>(client.getAllDatabases());
    Assert.assertEquals(5, fetchedDbs.size());
    Assert.assertTrue(fetchedDbs.contains(Warehouse.DEFAULT_DATABASE_NAME));

    fetchedDbs = new HashSet<>(client.getDatabases(catName, "d*"));
    Assert.assertEquals(3, fetchedDbs.size());
    for (String dbName : dbNames) Assert.assertTrue(fetchedDbs.contains(dbName));

    fetchedDbs = new HashSet<>(client.getDatabases("d*"));
    Assert.assertEquals(1, fetchedDbs.size());
    Assert.assertTrue(fetchedDbs.contains(Warehouse.DEFAULT_DATABASE_NAME));

    // Intentionally using the deprecated method to make sure it returns correct results.
    fetchedDbs = new HashSet<>(client.getDatabases("d*"));
    Assert.assertEquals(1, fetchedDbs.size());
    Assert.assertTrue(fetchedDbs.contains(Warehouse.DEFAULT_DATABASE_NAME));

    fetchedDbs = new HashSet<>(client.getDatabases(catName, "*1"));
    Assert.assertEquals(1, fetchedDbs.size());
    Assert.assertTrue(fetchedDbs.contains(dbNames[0]));

    fetchedDbs = new HashSet<>(client.getDatabases("*9"));
    Assert.assertEquals(0, fetchedDbs.size());

    // Intentionally using the deprecated method to make sure it returns correct results.
    fetchedDbs = new HashSet<>(client.getDatabases("*9"));
    Assert.assertEquals(0, fetchedDbs.size());

    fetchedDbs = new HashSet<>(client.getDatabases(catName, "*x"));
    Assert.assertEquals(0, fetchedDbs.size());

    // Check that dropping database from wrong catalog fails
    try {
      client.dropDatabase(dbNames[0], true, false, false);
      Assert.fail();
    } catch (NoSuchObjectException e) {
      // NOP
    }

    // Check that dropping database from wrong catalog fails
    try {
      // Intentionally using deprecated method
      client.dropDatabase(dbNames[0], true, false, false);
      Assert.fail();
    } catch (NoSuchObjectException e) {
      // NOP
    }

    // Drop them from the proper catalog
    client.dropDatabase(catName, dbNames[0], true, false, false);
    dir = new File(db0Location);
    Assert.assertFalse(dir.exists());

    client.dropDatabase(catName, dbNames[1], true, false, false);
    dir = new File(db1Location);
    Assert.assertFalse(dir.exists());

    fetchedDbs = new HashSet<>(client.getAllDatabases(catName));
    Assert.assertEquals(1, fetchedDbs.size());
  }

  @Test(expected = InvalidObjectException.class)
  public void createDatabaseInNonExistentCatalog() throws TException {
    Database db = new DatabaseBuilder()
        .setName("doomed")
        .setCatalogName("nosuch")
        .create(client, metaStore.getConf());
  }

  @Test(expected = NoSuchObjectException.class)
  public void fetchDatabaseInNonExistentCatalog() throws TException {
    client.getDatabase("nosuch", Warehouse.DEFAULT_DATABASE_NAME);
  }

  @Test(expected = NoSuchObjectException.class)
  public void dropDatabaseInNonExistentCatalog() throws TException {
    client.dropDatabase("nosuch", Warehouse.DEFAULT_DATABASE_NAME, true, false, false);
  }

  private Database getDatabaseWithAllParametersSet() throws Exception {
    return new DatabaseBuilder()
               .setName("dummy")
               .setOwnerType(PrincipalType.ROLE)
               .setOwnerName("owner")
               .setLocation(metaStore.getExternalWarehouseRoot() + "/database_location")
               .setDescription("dummy description")
               .addParam("param_key_1", "param_value_1")
               .addParam("param_key_2", "param_value_2")
               .build(metaStore.getConf());
  }
}
