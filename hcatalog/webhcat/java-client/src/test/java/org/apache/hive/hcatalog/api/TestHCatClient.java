/**
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.api;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hive.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema.Type;
import org.apache.hive.hcatalog.NoExitSecurityManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertArrayEquals;

public class TestHCatClient {
  private static final Logger LOG = LoggerFactory.getLogger(TestHCatClient.class);
  private static final String msPort = "20101";
  private static HiveConf hcatConf;
  private static SecurityManager securityManager;

  private static class RunMS implements Runnable {

    @Override
    public void run() {
      try {
        HiveMetaStore.main(new String[]{"-v", "-p", msPort});
      } catch (Throwable t) {
        LOG.error("Exiting. Got exception from metastore: ", t);
      }
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    LOG.info("Shutting down metastore.");
    System.setSecurityManager(securityManager);
  }

  @BeforeClass
  public static void startMetaStoreServer() throws Exception {

    Thread t = new Thread(new RunMS());
    t.start();
    Thread.sleep(40000);

    securityManager = System.getSecurityManager();
    System.setSecurityManager(new NoExitSecurityManager());
    hcatConf = new HiveConf(TestHCatClient.class);
    hcatConf.set("hive.metastore.local", "false");
    hcatConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:"
      + msPort);
    hcatConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    hcatConf.set(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
      HCatSemanticAnalyzer.class.getName());
    hcatConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hcatConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hcatConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname,
      "false");
    System.setProperty(HiveConf.ConfVars.PREEXECHOOKS.varname, " ");
    System.setProperty(HiveConf.ConfVars.POSTEXECHOOKS.varname, " ");
  }

  @Test
  public void testBasicDDLCommands() throws Exception {
    String db = "testdb";
    String tableOne = "testTable1";
    String tableTwo = "testTable2";
    HCatClient client = HCatClient.create(new Configuration(hcatConf));
    client.dropDatabase(db, true, HCatClient.DropDBMode.CASCADE);

    HCatCreateDBDesc dbDesc = HCatCreateDBDesc.create(db).ifNotExists(false)
      .build();
    client.createDatabase(dbDesc);
    List<String> dbNames = client.listDatabaseNamesByPattern("*");
    assertTrue(dbNames.contains("default"));
    assertTrue(dbNames.contains(db));

    HCatDatabase testDb = client.getDatabase(db);
    assertTrue(testDb.getComment() == null);
    assertTrue(testDb.getProperties().size() == 0);
    String warehouseDir = System
      .getProperty("test.warehouse.dir", "/user/hive/warehouse");
    String expectedDir = warehouseDir.replaceAll("\\\\", "/").replaceFirst("pfile:///", "pfile:/");
    assertEquals(expectedDir + "/" + db + ".db", testDb.getLocation());
    ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
    cols.add(new HCatFieldSchema("id", Type.INT, "id comment"));
    cols.add(new HCatFieldSchema("value", Type.STRING, "value comment"));
    HCatCreateTableDesc tableDesc = HCatCreateTableDesc
      .create(db, tableOne, cols).fileFormat("rcfile").build();
    client.createTable(tableDesc);
    HCatTable table1 = client.getTable(db, tableOne);
    assertTrue(table1.getInputFileFormat().equalsIgnoreCase(
      RCFileInputFormat.class.getName()));
    assertTrue(table1.getOutputFileFormat().equalsIgnoreCase(
      RCFileOutputFormat.class.getName()));
    assertTrue(table1.getSerdeLib().equalsIgnoreCase(
      ColumnarSerDe.class.getName()));
    assertTrue(table1.getCols().equals(cols));
    // Since "ifexists" was not set to true, trying to create the same table
    // again
    // will result in an exception.
    try {
      client.createTable(tableDesc);
      fail("Expected exception");
    } catch (HCatException e) {
      assertTrue(e.getMessage().contains(
        "AlreadyExistsException while creating table."));
    }

    client.dropTable(db, tableOne, true);
    HCatCreateTableDesc tableDesc2 = HCatCreateTableDesc.create(db,
      tableTwo, cols).build();
    client.createTable(tableDesc2);
    HCatTable table2 = client.getTable(db, tableTwo);
    assertTrue(table2.getInputFileFormat().equalsIgnoreCase(
      TextInputFormat.class.getName()));
    assertTrue(table2.getOutputFileFormat().equalsIgnoreCase(
      IgnoreKeyTextOutputFormat.class.getName()));
    assertEquals((expectedDir + "/" + db + ".db/" + tableTwo).toLowerCase(), table2.getLocation().toLowerCase());
    client.close();
  }

  @Test
  public void testPartitionsHCatClientImpl() throws Exception {
    HCatClient client = HCatClient.create(new Configuration(hcatConf));
    String dbName = "ptnDB";
    String tableName = "pageView";
    client.dropDatabase(dbName, true, HCatClient.DropDBMode.CASCADE);

    HCatCreateDBDesc dbDesc = HCatCreateDBDesc.create(dbName)
      .ifNotExists(true).build();
    client.createDatabase(dbDesc);
    ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
    cols.add(new HCatFieldSchema("userid", Type.INT, "id columns"));
    cols.add(new HCatFieldSchema("viewtime", Type.BIGINT,
      "view time columns"));
    cols.add(new HCatFieldSchema("pageurl", Type.STRING, ""));
    cols.add(new HCatFieldSchema("ip", Type.STRING,
      "IP Address of the User"));

    ArrayList<HCatFieldSchema> ptnCols = new ArrayList<HCatFieldSchema>();
    ptnCols.add(new HCatFieldSchema("dt", Type.STRING, "date column"));
    ptnCols.add(new HCatFieldSchema("country", Type.STRING,
      "country column"));
    HCatCreateTableDesc tableDesc = HCatCreateTableDesc
      .create(dbName, tableName, cols).fileFormat("sequencefile")
      .partCols(ptnCols).build();
    client.createTable(tableDesc);

    Map<String, String> firstPtn = new HashMap<String, String>();
    firstPtn.put("dt", "04/30/2012");
    firstPtn.put("country", "usa");
    HCatAddPartitionDesc addPtn = HCatAddPartitionDesc.create(dbName,
      tableName, null, firstPtn).build();
    client.addPartition(addPtn);

    Map<String, String> secondPtn = new HashMap<String, String>();
    secondPtn.put("dt", "04/12/2012");
    secondPtn.put("country", "brazil");
    HCatAddPartitionDesc addPtn2 = HCatAddPartitionDesc.create(dbName,
      tableName, null, secondPtn).build();
    client.addPartition(addPtn2);

    Map<String, String> thirdPtn = new HashMap<String, String>();
    thirdPtn.put("dt", "04/13/2012");
    thirdPtn.put("country", "argentina");
    HCatAddPartitionDesc addPtn3 = HCatAddPartitionDesc.create(dbName,
      tableName, null, thirdPtn).build();
    client.addPartition(addPtn3);

    List<HCatPartition> ptnList = client.listPartitionsByFilter(dbName,
      tableName, null);
    assertTrue(ptnList.size() == 3);

    HCatPartition ptn = client.getPartition(dbName, tableName, firstPtn);
    assertTrue(ptn != null);

    client.dropPartitions(dbName, tableName, firstPtn, true);
    ptnList = client.listPartitionsByFilter(dbName,
      tableName, null);
    assertTrue(ptnList.size() == 2);

    List<HCatPartition> ptnListTwo = client.listPartitionsByFilter(dbName,
      tableName, "country = \"argentina\"");
    assertTrue(ptnListTwo.size() == 1);

    client.markPartitionForEvent(dbName, tableName, thirdPtn,
      PartitionEventType.LOAD_DONE);
    boolean isMarked = client.isPartitionMarkedForEvent(dbName, tableName,
      thirdPtn, PartitionEventType.LOAD_DONE);
    assertTrue(isMarked);
    client.close();
  }

  @Test
  public void testDatabaseLocation() throws Exception {
    HCatClient client = HCatClient.create(new Configuration(hcatConf));
    String dbName = "locationDB";
    client.dropDatabase(dbName, true, HCatClient.DropDBMode.CASCADE);

    HCatCreateDBDesc dbDesc = HCatCreateDBDesc.create(dbName)
      .ifNotExists(true).location("/tmp/" + dbName).build();
    client.createDatabase(dbDesc);
    HCatDatabase newDB = client.getDatabase(dbName);
    assertTrue(newDB.getLocation().equalsIgnoreCase("file:/tmp/" + dbName));
    client.close();
  }

  @Test
  public void testCreateTableLike() throws Exception {
    HCatClient client = HCatClient.create(new Configuration(hcatConf));
    String tableName = "tableone";
    String cloneTable = "tabletwo";
    client.dropTable(null, tableName, true);
    client.dropTable(null, cloneTable, true);

    ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
    cols.add(new HCatFieldSchema("id", Type.INT, "id columns"));
    cols.add(new HCatFieldSchema("value", Type.STRING, "id columns"));
    HCatCreateTableDesc tableDesc = HCatCreateTableDesc
      .create(null, tableName, cols).fileFormat("rcfile").build();
    client.createTable(tableDesc);
    // create a new table similar to previous one.
    client.createTableLike(null, tableName, cloneTable, true, false, null);
    List<String> tables = client.listTableNamesByPattern(null, "table*");
    assertTrue(tables.size() == 2);
    client.close();
  }

  @Test
  public void testRenameTable() throws Exception {
    HCatClient client = HCatClient.create(new Configuration(hcatConf));
    String tableName = "temptable";
    String newName = "mytable";
    client.dropTable(null, tableName, true);
    client.dropTable(null, newName, true);
    ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
    cols.add(new HCatFieldSchema("id", Type.INT, "id columns"));
    cols.add(new HCatFieldSchema("value", Type.STRING, "id columns"));
    HCatCreateTableDesc tableDesc = HCatCreateTableDesc
      .create(null, tableName, cols).fileFormat("rcfile").build();
    client.createTable(tableDesc);
    client.renameTable(null, tableName, newName);
    try {
      client.getTable(null, tableName);
    } catch (HCatException exp) {
      assertTrue("Unexpected exception message: " + exp.getMessage(),
          exp.getMessage().contains("NoSuchObjectException while fetching table"));
    }
    HCatTable newTable = client.getTable(null, newName);
    assertTrue(newTable != null);
    assertTrue(newTable.getTableName().equals(newName));
    client.close();
  }

  @Test
  public void testTransportFailure() throws Exception {
    HCatClient client = HCatClient.create(new Configuration(hcatConf));
    boolean isExceptionCaught = false;
    // Table creation with a long table name causes ConnectionFailureException
    final String tableName = "Temptable" + new BigInteger(200, new Random()).toString(2);

    ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
    cols.add(new HCatFieldSchema("id", Type.INT, "id columns"));
    cols.add(new HCatFieldSchema("value", Type.STRING, "id columns"));
    try {
      HCatCreateTableDesc tableDesc = HCatCreateTableDesc
        .create(null, tableName, cols).fileFormat("rcfile").build();
      client.createTable(tableDesc);
    } catch (Exception exp) {
      isExceptionCaught = true;
      assertEquals("Unexpected exception type.", HCatException.class, exp.getClass());
      // The connection was closed, so create a new one.
      client = HCatClient.create(new Configuration(hcatConf));
      String newName = "goodTable";
      client.dropTable(null, newName, true);
      HCatCreateTableDesc tableDesc2 = HCatCreateTableDesc
        .create(null, newName, cols).fileFormat("rcfile").build();
      client.createTable(tableDesc2);
      HCatTable newTable = client.getTable(null, newName);
      assertTrue(newTable != null);
      assertTrue(newTable.getTableName().equalsIgnoreCase(newName));

    } finally {
      client.close();
      assertTrue("The expected exception was never thrown.", isExceptionCaught);
    }
  }

  @Test
  public void testOtherFailure() throws Exception {
    HCatClient client = HCatClient.create(new Configuration(hcatConf));
    String tableName = "Temptable";
    boolean isExceptionCaught = false;
    client.dropTable(null, tableName, true);
    ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
    cols.add(new HCatFieldSchema("id", Type.INT, "id columns"));
    cols.add(new HCatFieldSchema("value", Type.STRING, "id columns"));
    try {
      HCatCreateTableDesc tableDesc = HCatCreateTableDesc
        .create(null, tableName, cols).fileFormat("rcfile").build();
      client.createTable(tableDesc);
      // The DB foo is non-existent.
      client.getTable("foo", tableName);
    } catch (Exception exp) {
      isExceptionCaught = true;
      assertTrue(exp instanceof HCatException);
      String newName = "goodTable";
      client.dropTable(null, newName, true);
      HCatCreateTableDesc tableDesc2 = HCatCreateTableDesc
        .create(null, newName, cols).fileFormat("rcfile").build();
      client.createTable(tableDesc2);
      HCatTable newTable = client.getTable(null, newName);
      assertTrue(newTable != null);
      assertTrue(newTable.getTableName().equalsIgnoreCase(newName));
    } finally {
      client.close();
      assertTrue("The expected exception was never thrown.", isExceptionCaught);
    }
  }

  @Test
  public void testDropTableException() throws Exception {
    HCatClient client = HCatClient.create(new Configuration(hcatConf));
    String tableName = "tableToBeDropped";
    boolean isExceptionCaught = false;
    client.dropTable(null, tableName, true);
    try {
      client.dropTable(null, tableName, false);
    } catch (Exception exp) {
      isExceptionCaught = true;
      assertTrue(exp instanceof HCatException);
      LOG.info("Drop Table Exception: " + exp.getCause());
    } finally {
      client.close();
      assertTrue("The expected exception was never thrown.", isExceptionCaught);
    }
  }

  @Test
  public void testUpdateTableSchema() throws Exception {
    try {
      HCatClient client = HCatClient.create(new Configuration(hcatConf));
      final String dbName = "testUpdateTableSchema_DBName";
      final String tableName = "testUpdateTableSchema_TableName";

      client.dropDatabase(dbName, true, HCatClient.DropDBMode.CASCADE);

      client.createDatabase(HCatCreateDBDesc.create(dbName).build());
      List<HCatFieldSchema> oldSchema = Arrays.asList(new HCatFieldSchema("foo", Type.INT, ""),
          new HCatFieldSchema("bar", Type.STRING, ""));
      client.createTable(HCatCreateTableDesc.create(dbName, tableName, oldSchema).build());

      List<HCatFieldSchema> newSchema = Arrays.asList(new HCatFieldSchema("completely", Type.DOUBLE, ""),
          new HCatFieldSchema("new", Type.FLOAT, ""),
          new HCatFieldSchema("fields", Type.STRING, ""));

      client.updateTableSchema(dbName, tableName, newSchema);

      assertArrayEquals(newSchema.toArray(), client.getTable(dbName, tableName).getCols().toArray());

      client.dropDatabase(dbName, false, HCatClient.DropDBMode.CASCADE);
    }
    catch (Exception exception) {
      LOG.error("Unexpected exception.", exception);
      assertTrue("Unexpected exception: " + exception.getMessage(), false);
    }
  }

  @Test
  public void testObjectNotFoundException() throws Exception {
    try {

      HCatClient client = HCatClient.create(new  Configuration(hcatConf));
      String dbName = "testObjectNotFoundException_DBName";
      String tableName = "testObjectNotFoundException_TableName";
      client.dropDatabase(dbName, true, HCatClient.DropDBMode.CASCADE);

      try {    // Test that fetching a non-existent db-name yields ObjectNotFound.
        client.getDatabase(dbName);
        assertTrue("Expected ObjectNotFoundException.", false);
      } catch(Exception exception) {
        LOG.info("Got exception: ", exception);
        assertTrue("Expected ObjectNotFoundException. Got:" + exception.getClass(),
            exception instanceof ObjectNotFoundException);
      }

      client.createDatabase(HCatCreateDBDesc.create(dbName).build());

      try {   // Test that fetching a non-existent table-name yields ObjectNotFound.
        client.getTable(dbName, tableName);
        assertTrue("Expected ObjectNotFoundException.", false);
      } catch(Exception exception) {
        LOG.info("Got exception: ", exception);
        assertTrue("Expected ObjectNotFoundException. Got:" + exception.getClass(),
            exception instanceof ObjectNotFoundException);
      }

      String partitionColumn = "part";

      List<HCatFieldSchema> columns = Arrays.asList(new HCatFieldSchema("col", Type.STRING, ""));
      ArrayList<HCatFieldSchema> partitionColumns = new ArrayList<HCatFieldSchema>(
          Arrays.asList(new HCatFieldSchema(partitionColumn, Type.STRING, "")));
      client.createTable(HCatCreateTableDesc.create(dbName, tableName, columns)
          .partCols(partitionColumns)
          .build());

      Map<String, String> partitionSpec = new HashMap<String, String>();
      partitionSpec.put(partitionColumn, "foobar");
      try {  // Test that fetching a non-existent partition yields ObjectNotFound.
        client.getPartition(dbName, tableName, partitionSpec);
        assertTrue("Expected ObjectNotFoundException.", false);
      } catch(Exception exception) {
        LOG.info("Got exception: ", exception);
        assertTrue("Expected ObjectNotFoundException. Got:" + exception.getClass(),
            exception instanceof ObjectNotFoundException);
      }

      client.addPartition(HCatAddPartitionDesc.create(dbName, tableName, "", partitionSpec).build());

      // Test that listPartitionsByFilter() returns an empty-set, if the filter selects no partitions.
      assertEquals("Expected empty set of partitions.",
          0, client.listPartitionsByFilter(dbName, tableName, partitionColumn + " < 'foobar'").size());

      try {  // Test that listPartitionsByFilter() throws HCatException if the partition-key is incorrect.
        partitionSpec.put("NonExistentKey", "foobar");
        client.getPartition(dbName, tableName, partitionSpec);
        assertTrue("Expected HCatException.", false);
      } catch(Exception exception) {
        LOG.info("Got exception: ", exception);
        assertTrue("Expected HCatException. Got:" + exception.getClass(),
            exception instanceof HCatException);
        assertFalse("Did not expect ObjectNotFoundException.", exception instanceof ObjectNotFoundException);
      }

    }
    catch (Throwable t) {
      LOG.error("Unexpected exception!", t);
      assertTrue("Unexpected exception! " + t.getMessage(), false);
    }
  }

  @Test
  public void testGetMessageBusTopicName() throws Exception {
    try {
      HCatClient client = HCatClient.create(new Configuration(hcatConf));
      String dbName = "testGetMessageBusTopicName_DBName";
      String tableName = "testGetMessageBusTopicName_TableName";
      client.dropDatabase(dbName, true, HCatClient.DropDBMode.CASCADE);
      client.createDatabase(HCatCreateDBDesc.create(dbName).build());
      String messageBusTopicName = "MY.topic.name";
      Map<String, String> tableProperties = new HashMap<String, String>(1);
      tableProperties.put(HCatConstants.HCAT_MSGBUS_TOPIC_NAME, messageBusTopicName);
      client.createTable(HCatCreateTableDesc.create(dbName, tableName, Arrays.asList(new HCatFieldSchema("foo", Type.STRING, ""))).tblProps(tableProperties).build());

      assertEquals("MessageBus topic-name doesn't match!", messageBusTopicName, client.getMessageBusTopicName(dbName, tableName));
      client.dropDatabase(dbName, true, HCatClient.DropDBMode.CASCADE);
      client.close();
    }
    catch (Exception exception) {
      LOG.error("Unexpected exception.", exception);
      assertTrue("Unexpected exception:" + exception.getMessage(), false);
    }
  }

  @Test
  public void testPartitionSchema() throws Exception {
    try {
      HCatClient client = HCatClient.create(new Configuration(hcatConf));
      final String dbName = "myDb";
      final String tableName = "myTable";

      client.dropDatabase(dbName, true, HCatClient.DropDBMode.CASCADE);

      client.createDatabase(HCatCreateDBDesc.create(dbName).build());
      List<HCatFieldSchema> columnSchema = Arrays.asList(new HCatFieldSchema("foo", Type.INT, ""),
          new HCatFieldSchema("bar", Type.STRING, ""));

      List<HCatFieldSchema> partitionSchema = Arrays.asList(new HCatFieldSchema("dt", Type.STRING, ""),
          new HCatFieldSchema("grid", Type.STRING, ""));

      client.createTable(HCatCreateTableDesc.create(dbName, tableName, columnSchema).partCols(partitionSchema).build());

      HCatTable table = client.getTable(dbName, tableName);
      List<HCatFieldSchema> partitionColumns = table.getPartCols();

      assertArrayEquals("Didn't get expected partition-schema back from the HCatTable.",
          partitionSchema.toArray(), partitionColumns.toArray());
      client.dropDatabase(dbName, false, HCatClient.DropDBMode.CASCADE);
    }
    catch (Exception unexpected) {
      LOG.error("Unexpected exception!", unexpected);
      assertTrue("Unexpected exception! " + unexpected.getMessage(), false);
    }
  }

  @Test
  public void testGetPartitionsWithPartialSpec() throws Exception {
    try {
      HCatClient client = HCatClient.create(new Configuration(hcatConf));
      final String dbName = "myDb";
      final String tableName = "myTable";

      client.dropDatabase(dbName, true, HCatClient.DropDBMode.CASCADE);

      client.createDatabase(HCatCreateDBDesc.create(dbName).build());
      List<HCatFieldSchema> columnSchema = Arrays.asList(new HCatFieldSchema("foo", Type.INT, ""),
          new HCatFieldSchema("bar", Type.STRING, ""));

      List<HCatFieldSchema> partitionSchema = Arrays.asList(new HCatFieldSchema("dt", Type.STRING, ""),
          new HCatFieldSchema("grid", Type.STRING, ""));

      client.createTable(HCatCreateTableDesc.create(dbName, tableName, columnSchema).partCols(new ArrayList<HCatFieldSchema>(partitionSchema)).build());

      Map<String, String> partitionSpec = new HashMap<String, String>();
      partitionSpec.put("grid", "AB");
      partitionSpec.put("dt", "2011_12_31");
      client.addPartition(HCatAddPartitionDesc.create(dbName, tableName, "", partitionSpec).build());
      partitionSpec.put("grid", "AB");
      partitionSpec.put("dt", "2012_01_01");
      client.addPartition(HCatAddPartitionDesc.create(dbName, tableName, "", partitionSpec).build());
      partitionSpec.put("dt", "2012_01_01");
      partitionSpec.put("grid", "OB");
      client.addPartition(HCatAddPartitionDesc.create(dbName, tableName, "", partitionSpec).build());
      partitionSpec.put("dt", "2012_01_01");
      partitionSpec.put("grid", "XB");
      client.addPartition(HCatAddPartitionDesc.create(dbName, tableName, "", partitionSpec).build());

      Map<String, String> partialPartitionSpec = new HashMap<String, String>();
      partialPartitionSpec.put("dt", "2012_01_01");

      List<HCatPartition> partitions = client.getPartitions(dbName, tableName, partialPartitionSpec);
      assertEquals("Unexpected number of partitions.", 3, partitions.size());
      assertArrayEquals("Mismatched partition.", new String[]{"2012_01_01", "AB"}, partitions.get(0).getValues().toArray());
      assertArrayEquals("Mismatched partition.", new String[]{"2012_01_01", "OB"}, partitions.get(1).getValues().toArray());
      assertArrayEquals("Mismatched partition.", new String[]{"2012_01_01", "XB"}, partitions.get(2).getValues().toArray());

      client.dropDatabase(dbName, false, HCatClient.DropDBMode.CASCADE);
    }
    catch (Exception unexpected) {
      LOG.error("Unexpected exception!", unexpected);
      assertTrue("Unexpected exception! " + unexpected.getMessage(), false);
    }
  }

  @Test
  public void testDropPartitionsWithPartialSpec() throws Exception {
    try {
      HCatClient client = HCatClient.create(new Configuration(hcatConf));
      final String dbName = "myDb";
      final String tableName = "myTable";

      client.dropDatabase(dbName, true, HCatClient.DropDBMode.CASCADE);

      client.createDatabase(HCatCreateDBDesc.create(dbName).build());
      List<HCatFieldSchema> columnSchema = Arrays.asList(new HCatFieldSchema("foo", Type.INT, ""),
          new HCatFieldSchema("bar", Type.STRING, ""));

      List<HCatFieldSchema> partitionSchema = Arrays.asList(new HCatFieldSchema("dt", Type.STRING, ""),
          new HCatFieldSchema("grid", Type.STRING, ""));

      client.createTable(HCatCreateTableDesc.create(dbName, tableName, columnSchema).partCols(new ArrayList<HCatFieldSchema>(partitionSchema)).build());

      Map<String, String> partitionSpec = new HashMap<String, String>();
      partitionSpec.put("grid", "AB");
      partitionSpec.put("dt", "2011_12_31");
      client.addPartition(HCatAddPartitionDesc.create(dbName, tableName, "", partitionSpec).build());
      partitionSpec.put("grid", "AB");
      partitionSpec.put("dt", "2012_01_01");
      client.addPartition(HCatAddPartitionDesc.create(dbName, tableName, "", partitionSpec).build());
      partitionSpec.put("dt", "2012_01_01");
      partitionSpec.put("grid", "OB");
      client.addPartition(HCatAddPartitionDesc.create(dbName, tableName, "", partitionSpec).build());
      partitionSpec.put("dt", "2012_01_01");
      partitionSpec.put("grid", "XB");
      client.addPartition(HCatAddPartitionDesc.create(dbName, tableName, "", partitionSpec).build());

      Map<String, String> partialPartitionSpec = new HashMap<String, String>();
      partialPartitionSpec.put("dt", "2012_01_01");

      client.dropPartitions(dbName, tableName, partialPartitionSpec, true);

      List<HCatPartition> partitions = client.getPartitions(dbName, tableName);
      assertEquals("Unexpected number of partitions.", 1, partitions.size());
      assertArrayEquals("Mismatched partition.", new String[]{"2011_12_31", "AB"}, partitions.get(0).getValues().toArray());

      client.dropDatabase(dbName, false, HCatClient.DropDBMode.CASCADE);
    }
    catch (Exception unexpected) {
      LOG.error("Unexpected exception!", unexpected);
      assertTrue("Unexpected exception! " + unexpected.getMessage(), false);
    }
  }

}
