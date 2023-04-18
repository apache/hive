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
package org.apache.hadoop.hive.metastore.tools.schematool;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.text.StrTokenizer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.FunctionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;

public class TestSchemaToolCatalogOps {
  private static MetastoreSchemaTool schemaTool;
  private static HiveConf conf;
  private IMetaStoreClient client;
  private static String testMetastoreDB;
  private static PrintStream errStream;
  private static PrintStream outStream;
  private static String argsBase;
  private static File catalogLocation;

  @BeforeClass
  public static void initDb() throws HiveMetaException, IOException {
    conf = new HiveConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.AUTO_CREATE_ALL, false);
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.HMS_HANDLER_ATTEMPTS, 1);
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.THRIFT_CONNECTION_RETRIES, 1);
    testMetastoreDB = System.getProperty("java.io.tmpdir") +
        File.separator + "testschematoolcatopsdb";
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECT_URL_KEY,
        "jdbc:derby:" + testMetastoreDB + ";create=true");
    schemaTool = new MetastoreSchemaTool();
    schemaTool.init(System.getProperty("test.tmp.dir", "target/tmp"),
        new String[]{"-dbType", "derby", "--info"}, null, conf);

    String userName = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.CONNECTION_USER_NAME);
    String passWord = MetastoreConf.getPassword(conf, MetastoreConf.ConfVars.PWD);
    schemaTool.setUserName(userName);
    schemaTool.setPassWord(passWord);
    errStream = System.err;
    outStream = System.out;

    argsBase = "-dbType derby -userName " + userName + " -passWord " + passWord + " ";
    catalogLocation = Files.createTempDir();
    execute(new SchemaToolTaskInit(), "-initSchema"); // Pre-install the database so all the tables are there.
  }

  @AfterClass
  public static void removeDb() throws Exception {
    File metaStoreDir = new File(testMetastoreDB);
    if (metaStoreDir.exists()) {
      FileUtils.forceDeleteOnExit(metaStoreDir);
    }
    if (catalogLocation.exists()) {
      FileUtils.forceDeleteOnExit(catalogLocation);
    }
    System.setOut(outStream);
    System.setErr(errStream);
  }

  @Before
  public void createClient() throws MetaException {
    client = new HiveMetaStoreClient(conf);
  }

  @Test
  public void createCatalog() throws HiveMetaException, TException {
    String catName = "my_test_catalog";
    String location = "file:///tmp/my_test_catalog";
    String description = "very descriptive";
    String argsCreate = String.format("-createCatalog %s -catalogLocation \"%s\" -catalogDescription \"%s\"",
        catName, location, description);
    execute(new SchemaToolTaskCreateCatalog(), argsCreate);

    Catalog cat = client.getCatalog(catName);
    Assert.assertEquals(location, cat.getLocationUri());
    Assert.assertEquals(description, cat.getDescription());
  }

  @Test(expected = HiveMetaException.class)
  public void createExistingCatalog() throws HiveMetaException {
    String catName = "hive";
    String location = "somewhere";
    String argsCreate = String.format("-createCatalog %s -catalogLocation \"%s\"",
        catName, location);
    execute(new SchemaToolTaskCreateCatalog(), argsCreate);
  }

  @Test
  public void createExistingCatalogWithIfNotExists() throws HiveMetaException {
    String catName = "my_existing_test_catalog";
    String location = "file:///tmp/my_test_catalog";
    String description = "very descriptive";
    String argsCreate1 = String.format("-createCatalog %s -catalogLocation \"%s\" -catalogDescription \"%s\"",
        catName, location, description);
    execute(new SchemaToolTaskCreateCatalog(), argsCreate1);

    String argsCreate2 =
        String.format("-createCatalog %s -catalogLocation \"%s\" -catalogDescription \"%s\" -ifNotExists",
        catName, location, description);
    execute(new SchemaToolTaskCreateCatalog(), argsCreate2);
  }

  @Test
  public void alterCatalog() throws HiveMetaException, TException {
    String catName = "an_alterable_catalog";
    String location = "file:///tmp/an_alterable_catalog";
    String description = "description";
    String argsCreate = String.format("-createCatalog %s -catalogLocation \"%s\" -catalogDescription \"%s\"",
        catName, location, description);
    execute(new SchemaToolTaskCreateCatalog(), argsCreate);

    location = "file:///tmp/somewhere_else";
    String argsAlter1 = String.format("-alterCatalog %s -catalogLocation \"%s\"",
        catName, location);
    execute(new SchemaToolTaskAlterCatalog(), argsAlter1);
    Catalog cat = client.getCatalog(catName);
    Assert.assertEquals(location, cat.getLocationUri());
    Assert.assertEquals(description, cat.getDescription());

    description = "a better description";
    String argsAlter2 = String.format("-alterCatalog %s -catalogDescription \"%s\"",
        catName, description);
    execute(new SchemaToolTaskAlterCatalog(), argsAlter2);
    cat = client.getCatalog(catName);
    Assert.assertEquals(location, cat.getLocationUri());
    Assert.assertEquals(description, cat.getDescription());

    location = "file:///tmp/a_third_location";
    description = "best description yet";
    String argsAlter3 = String.format("-alterCatalog %s -catalogLocation \"%s\" -catalogDescription \"%s\"",
        catName, location, description);
    execute(new SchemaToolTaskAlterCatalog(), argsAlter3);
    cat = client.getCatalog(catName);
    Assert.assertEquals(location, cat.getLocationUri());
    Assert.assertEquals(description, cat.getDescription());
  }

  @Test(expected = HiveMetaException.class)
  public void alterBogusCatalog() throws HiveMetaException {
    String catName = "nosuch";
    String location = "file:///tmp/somewhere";
    String description = "whatever";
    String argsAlter = String.format("-alterCatalog %s -catalogLocation \"%s\" -catalogDescription \"%s\"",
        catName, location, description);
    execute(new SchemaToolTaskAlterCatalog(), argsAlter);
  }

  @Test(expected = HiveMetaException.class)
  public void alterCatalogNoChange() throws HiveMetaException {
    String catName = "alter_cat_no_change";
    String location = "file:///tmp/alter_cat_no_change";
    String description = "description";
    String argsCreate = String.format("-createCatalog %s -catalogLocation \"%s\" -catalogDescription \"%s\"",
        catName, location, description);
    execute(new SchemaToolTaskCreateCatalog(), argsCreate);

    String argsAlter = String.format("-alterCatalog %s", catName);
    execute(new SchemaToolTaskAlterCatalog(), argsAlter);
  }

  @Test
  public void moveDatabase() throws HiveMetaException, TException {
    String toCatName = "moveDbCat";
    String dbName = "moveDbDb";
    String tableName = "moveDbTable";
    String funcName = "movedbfunc";
    String partVal = "moveDbKey";

    withCatalog(toCatName, catName -> {
      Database db = new DatabaseBuilder()
          .setCatalogName(DEFAULT_CATALOG_NAME)
          .setName(dbName)
          .create(client, conf);

      new FunctionBuilder()
          .inDb(db)
          .setName(funcName)
          .setClass("org.apache.hive.myudf")
          .create(client, conf);

      Table table = new TableBuilder()
          .inDb(db)
          .setTableName(tableName)
          .addCol("a", "int")
          .addPartCol("p", "string")
          .create(client, conf);

      new PartitionBuilder()
          .inTable(table)
          .addValue(partVal)
          .addToTable(client, conf);

      String argsMoveDB = String.format("-moveDatabase %s -fromCatalog %s -toCatalog %s", dbName,
          DEFAULT_CATALOG_NAME, catName);
      execute(new SchemaToolTaskMoveDatabase(), argsMoveDB);

      Database fetchedDb = client.getDatabase(catName, dbName);
      Assert.assertNotNull(fetchedDb);
      Assert.assertEquals(catName.toLowerCase(), fetchedDb.getCatalogName());

      Function fetchedFunction = client.getFunction(catName, dbName, funcName);
      Assert.assertNotNull(fetchedFunction);
      Assert.assertEquals(catName.toLowerCase(), fetchedFunction.getCatName());
      Assert.assertEquals(dbName.toLowerCase(), fetchedFunction.getDbName());

      Table fetchedTable = client.getTable(catName, dbName, tableName);
      Assert.assertNotNull(fetchedTable);
      Assert.assertEquals(catName.toLowerCase(), fetchedTable.getCatName());
      Assert.assertEquals(dbName.toLowerCase(), fetchedTable.getDbName());

      Partition fetchedPart =
          client.getPartition(catName, dbName, tableName, Collections.singletonList(partVal));
      Assert.assertNotNull(fetchedPart);
      Assert.assertEquals(catName.toLowerCase(), fetchedPart.getCatName());
      Assert.assertEquals(dbName.toLowerCase(), fetchedPart.getDbName());
      Assert.assertEquals(tableName.toLowerCase(), fetchedPart.getTableName());

      // drop the function
      client.dropFunction(catName, dbName, funcName);
    });
  }

  @Test
  public void moveDatabaseWithExistingDbOfSameNameAlreadyInTargetCatalog()
      throws TException, HiveMetaException {
    String catName = "clobberCatalog";
    withCatalog(catName, name -> {
      try {
        String argsMoveDB = String.format("-moveDatabase %s -fromCatalog %s -toCatalog %s",
            DEFAULT_DATABASE_NAME, catName, DEFAULT_CATALOG_NAME);
        execute(new SchemaToolTaskMoveDatabase(), argsMoveDB);
        Assert.fail("Attempt to move default database should have failed.");
      } catch (HiveMetaException e) {
        // good
      }

      // Make sure nothing really moved
      Set<String> dbNames = new HashSet<>(client.getAllDatabases(DEFAULT_CATALOG_NAME));
      Assert.assertTrue(dbNames.contains(DEFAULT_DATABASE_NAME));
    });
  }

  @Test(expected = HiveMetaException.class)
  public void moveNonExistentDatabase() throws TException, HiveMetaException {
    String catName = "moveNonExistentDb";
    withCatalog(catName, name -> {
      String argsMoveDB = String.format("-moveDatabase nosuch -fromCatalog %s -toCatalog %s",
          name, DEFAULT_CATALOG_NAME);
      execute(new SchemaToolTaskMoveDatabase(), argsMoveDB);
    });
  }

  @Test
  public void moveDbToNonExistentCatalog() throws TException, HiveMetaException {
    String dbName = "doomedToHomelessness";
    new DatabaseBuilder()
        .setName(dbName)
        .create(client, conf);
    try {
      String argsMoveDB = String.format("-moveDatabase %s -fromCatalog %s -toCatalog nosuch",
          dbName, DEFAULT_CATALOG_NAME);
      execute(new SchemaToolTaskMoveDatabase(), argsMoveDB);
      Assert.fail("Attempt to move database to non-existent catalog should have failed.");
    } catch (HiveMetaException e) {
      // good
    }

    // Make sure nothing really moved
    Set<String> dbNames = new HashSet<>(client.getAllDatabases(DEFAULT_CATALOG_NAME));
    Assert.assertTrue(dbNames.contains(dbName.toLowerCase()));
  }

  @Test
  public void moveTable() throws TException, HiveMetaException {
    String toCatName = "moveTableCat";
    String toDbName = "moveTableDb";
    String tableName = "moveTableTable";
    String partVal = "moveTableKey";

    withCatalog(toCatName, catName -> {
      new DatabaseBuilder()
          .setCatalogName(toCatName)
          .setName(toDbName)
          .create(client, conf);

      Table table = new TableBuilder()
          .setTableName(tableName)
          .addCol("a", "int")
          .addPartCol("p", "string")
          .create(client, conf);

      new PartitionBuilder()
          .inTable(table)
          .addValue(partVal)
          .addToTable(client, conf);

      String argsMoveTable = String.format("-moveTable %s -fromCatalog %s -toCatalog %s -fromDatabase %s -toDatabase %s",
          tableName, DEFAULT_CATALOG_NAME, toCatName, DEFAULT_DATABASE_NAME, toDbName);
      execute(new SchemaToolTaskMoveTable(), argsMoveTable);

      Table fetchedTable = client.getTable(toCatName, toDbName, tableName);
      Assert.assertNotNull(fetchedTable);
      Assert.assertEquals(toCatName.toLowerCase(), fetchedTable.getCatName());
      Assert.assertEquals(toDbName.toLowerCase(), fetchedTable.getDbName());

      Partition fetchedPart =
          client.getPartition(toCatName, toDbName, tableName, Collections.singletonList(partVal));
      Assert.assertNotNull(fetchedPart);
      Assert.assertEquals(toCatName.toLowerCase(), fetchedPart.getCatName());
      Assert.assertEquals(toDbName.toLowerCase(), fetchedPart.getDbName());
      Assert.assertEquals(tableName.toLowerCase(), fetchedPart.getTableName());
    });
  }

  @Test
  public void moveTableWithinCatalog() throws TException, HiveMetaException {
    String toDbName = "moveTableWithinCatalogDb";
    String tableName = "moveTableWithinCatalogTable";
    String partVal = "moveTableWithinCatalogKey";

    new DatabaseBuilder()
        .setName(toDbName)
        .create(client, conf);

    Table table = new TableBuilder()
        .setTableName(tableName)
        .addCol("a", "int")
        .addPartCol("p", "string")
        .create(client, conf);

    new PartitionBuilder()
        .inTable(table)
        .addValue(partVal)
        .addToTable(client, conf);

    String argsMoveTable = String.format("-moveTable %s -fromCatalog %s -toCatalog %s -fromDatabase %s -toDatabase %s",
        tableName, DEFAULT_CATALOG_NAME, DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, toDbName);
    execute(new SchemaToolTaskMoveTable(), argsMoveTable);

    Table fetchedTable = client.getTable(DEFAULT_CATALOG_NAME, toDbName, tableName);
    Assert.assertNotNull(fetchedTable);
    Assert.assertEquals(DEFAULT_CATALOG_NAME, fetchedTable.getCatName());
    Assert.assertEquals(toDbName.toLowerCase(), fetchedTable.getDbName());

    Partition fetchedPart =
        client.getPartition(DEFAULT_CATALOG_NAME, toDbName, tableName, Collections.singletonList(partVal));
    Assert.assertNotNull(fetchedPart);
    Assert.assertEquals(DEFAULT_CATALOG_NAME, fetchedPart.getCatName());
    Assert.assertEquals(toDbName.toLowerCase(), fetchedPart.getDbName());
    Assert.assertEquals(tableName.toLowerCase(), fetchedPart.getTableName());
  }

  @Test
  public void moveTableWithExistingTableOfSameNameAlreadyInTargetDatabase()
      throws TException, HiveMetaException {
    String toDbName = "clobberTableDb";
    String tableName = "clobberTableTable";

    Database toDb = new DatabaseBuilder()
        .setName(toDbName)
        .create(client, conf);

    new TableBuilder()
        .setTableName(tableName)
        .addCol("a", "int")
        .create(client, conf);

    new TableBuilder()
        .inDb(toDb)
        .setTableName(tableName)
        .addCol("b", "varchar(32)")
        .create(client, conf);

    try {
      String argsMoveTable =
          String.format("-moveTable %s -fromCatalog %s -toCatalog %s -fromDatabase %s -toDatabase %s",
          tableName, DEFAULT_CATALOG_NAME, DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, toDbName);
      execute(new SchemaToolTaskMoveTable(), argsMoveTable);
      Assert.fail("Attempt to move table should have failed.");
    } catch (HiveMetaException e) {
      // good
    }

    // Make sure nothing really moved
    Set<String> tableNames = new HashSet<>(client.getAllTables(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME));
    Assert.assertTrue(tableNames.contains(tableName.toLowerCase()));

    // Make sure the table in the target database didn't get clobbered
    Table fetchedTable =  client.getTable(DEFAULT_CATALOG_NAME, toDbName, tableName);
    Assert.assertEquals("b", fetchedTable.getSd().getCols().get(0).getName());
  }

  @Test(expected = HiveMetaException.class)
  public void moveNonExistentTable() throws TException, HiveMetaException {
    String toDbName = "moveNonExistentTable";
    new DatabaseBuilder()
        .setName(toDbName)
        .create(client, conf);
    String argsMoveTable =
        String.format("-moveTable nosuch -fromCatalog %s -toCatalog %s -fromDatabase %s -toDatabase %s",
        DEFAULT_CATALOG_NAME, DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, toDbName);
    execute(new SchemaToolTaskMoveTable(), argsMoveTable);
  }

  @Test
  public void moveTableToNonExistentDb() throws TException, HiveMetaException {
    String tableName = "doomedToWander";
    new TableBuilder()
        .setTableName(tableName)
        .addCol("a", "int")
        .create(client, conf);

    try {
      String argsMoveTable =
          String.format("-moveTable %s -fromCatalog %s -toCatalog %s -fromDatabase %s -toDatabase nosuch",
          tableName, DEFAULT_CATALOG_NAME, DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME);
      execute(new SchemaToolTaskMoveTable(), argsMoveTable);
      Assert.fail("Attempt to move table to non-existent table should have failed.");
    } catch (HiveMetaException e) {
      // good
    }

    // Make sure nothing really moved
    Set<String> tableNames = new HashSet<>(client.getAllTables(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME));
    Assert.assertTrue(tableNames.contains(tableName.toLowerCase()));
  }

  private static void execute(SchemaToolTask task, String taskArgs) throws HiveMetaException {
    try {
      StrTokenizer tokenizer = new StrTokenizer(argsBase + taskArgs, ' ', '\"');
      SchemaToolCommandLine cl = new SchemaToolCommandLine(tokenizer.getTokenArray(), null);
      task.setCommandLineArguments(cl);
    } catch (Exception e) {
      throw new IllegalStateException("Could not parse comman line \n" + argsBase + taskArgs, e);
    }

    task.setHiveSchemaTool(schemaTool);
    task.execute();
  }

  @FunctionalInterface
  private interface CheckFunc<T> {
    void apply(T t) throws HiveMetaException, TException;
  }

  // Function will build catalog itself and clean the catalog after the operation.
  private void withCatalog(String catName, CheckFunc<String> f)
      throws HiveMetaException, TException {
    try {
      new CatalogBuilder()
          .setName(catName)
          .setLocation(catalogLocation.toString())
          .create(client);
      f.apply(catName);
    } finally {
      List<String> dbNames = client.getAllDatabases(catName);
      for (String dbName : dbNames) {
        List<String> tblNames = client.getAllTables(catName, dbName);
        for (String tblName : tblNames) {
          client.dropTable(catName, dbName, tblName);
        }
        client.dropDatabase(catName, dbName);
      }
      client.dropCatalog(catName);
    }
  }
}
