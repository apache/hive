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

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.ql.WindowsPathUtil;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hive.hcatalog.api.repl.Command;
import org.apache.hive.hcatalog.api.repl.ReplicationTask;
import org.apache.hive.hcatalog.api.repl.ReplicationUtils;
import org.apache.hive.hcatalog.api.repl.StagingDirectoryProvider;
import org.apache.hive.hcatalog.api.repl.exim.EximReplicationTaskFactory;
import org.apache.hive.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema.Type;
import org.apache.hive.hcatalog.NoExitSecurityManager;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;
import org.apache.hive.hcatalog.listener.DbNotificationListener;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertArrayEquals;

import org.apache.hadoop.util.Shell;

import javax.annotation.Nullable;

public class TestHCatClient {
  private static final Logger LOG = LoggerFactory.getLogger(TestHCatClient.class);
  private static final String msPort = "20101";
  private static HiveConf hcatConf;
  private static boolean isReplicationTargetHCatRunning = false;
  private static final String replicationTargetHCatPort = "20102";
  private static HiveConf replicationTargetHCatConf;
  private static SecurityManager securityManager;
  private static boolean useExternalMS = false;

  public static class RunMS implements Runnable {

    private final String msPort;
    private List<String> args = new ArrayList<String>();

    public RunMS(String msPort) {
      this.msPort = msPort;
      this.args.add("-v");
      this.args.add("-p");
      this.args.add(this.msPort);
    }

    public RunMS arg(String arg) {
      this.args.add(arg);
      return this;
    }

    @Override
    public void run() {
      try {
        HiveMetaStore.main(args.toArray(new String[args.size()]));
      } catch (Throwable t) {
        LOG.error("Exiting. Got exception from metastore: ", t);
      }
    }
  } // class RunMS;

  @AfterClass
  public static void tearDown() throws Exception {
    if (!useExternalMS) {
      LOG.info("Shutting down metastore.");
      System.setSecurityManager(securityManager);
    }
  }

  @BeforeClass
  public static void startMetaStoreServer() throws Exception {

    hcatConf = new HiveConf(TestHCatClient.class);
    String metastoreUri = System.getProperty("test."+HiveConf.ConfVars.METASTOREURIS.varname);
    if (metastoreUri != null) {
      hcatConf.setVar(HiveConf.ConfVars.METASTOREURIS, metastoreUri);
      useExternalMS = true;
      return;
    }
    if (Shell.WINDOWS) {
      WindowsPathUtil.convertPathsFromWindowsToHdfs(hcatConf);
    }

    System.setProperty(HiveConf.ConfVars.METASTORE_EVENT_LISTENERS.varname,
        DbNotificationListener.class.getName()); // turn on db notification listener on metastore
    Thread t = new Thread(new RunMS(msPort));
    t.start();
    Thread.sleep(10000);

    securityManager = System.getSecurityManager();
    System.setSecurityManager(new NoExitSecurityManager());
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

  public static HiveConf getConf(){
    return hcatConf;
  }

  public static String fixPath(String path) {
    if(!Shell.WINDOWS) {
      return path;
    }
    String expectedDir = path.replaceAll("\\\\", "/");
    if (!expectedDir.startsWith("/")) {
      expectedDir = "/" + expectedDir;
    }
    return expectedDir;
  }

  public static String makePartLocation(HCatTable table, Map<String, String> partitionSpec) throws MetaException {
    return (new Path(table.getSd().getLocation(), Warehouse.makePartPath(partitionSpec))).toUri().toString();
  }

  @Test
  public void testBasicDDLCommands() throws Exception {
    String db = "testdb";
    String tableOne = "testTable1";
    String tableTwo = "testTable2";
    String tableThree = "testTable3";
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
    if (useExternalMS) {
      assertTrue(testDb.getLocation().matches(".*" + "/" + db + ".db"));
    } else {
      String expectedDir = warehouseDir.replaceFirst("pfile:///", "pfile:/");
      assertEquals(expectedDir + "/" + db + ".db", testDb.getLocation());
    }
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
      LazyBinaryColumnarSerDe.class.getName()));
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
      tableTwo, cols).fieldsTerminatedBy('\001').escapeChar('\002').linesTerminatedBy('\003').
      mapKeysTerminatedBy('\004').collectionItemsTerminatedBy('\005').nullDefinedAs('\006').build();
    client.createTable(tableDesc2);
    HCatTable table2 = client.getTable(db, tableTwo);
    assertTrue("Expected TextInputFormat, but got: " + table2.getInputFileFormat(),
               table2.getInputFileFormat().equalsIgnoreCase(TextInputFormat.class.getName()));
    assertTrue(table2.getOutputFileFormat().equalsIgnoreCase(
      HiveIgnoreKeyTextOutputFormat.class.getName()));
    assertTrue("SerdeParams not found", table2.getSerdeParams() != null);
    assertEquals("checking " + serdeConstants.FIELD_DELIM, Character.toString('\001'),
      table2.getSerdeParams().get(serdeConstants.FIELD_DELIM));
    assertEquals("checking " + serdeConstants.ESCAPE_CHAR, Character.toString('\002'),
      table2.getSerdeParams().get(serdeConstants.ESCAPE_CHAR));
    assertEquals("checking " + serdeConstants.LINE_DELIM, Character.toString('\003'),
      table2.getSerdeParams().get(serdeConstants.LINE_DELIM));
    assertEquals("checking " + serdeConstants.MAPKEY_DELIM, Character.toString('\004'),
      table2.getSerdeParams().get(serdeConstants.MAPKEY_DELIM));
    assertEquals("checking " + serdeConstants.COLLECTION_DELIM, Character.toString('\005'),
      table2.getSerdeParams().get(serdeConstants.COLLECTION_DELIM));
    assertEquals("checking " + serdeConstants.SERIALIZATION_NULL_FORMAT, Character.toString('\006'),
      table2.getSerdeParams().get(serdeConstants.SERIALIZATION_NULL_FORMAT));
    
    assertTrue(table2.getLocation().toLowerCase().matches(".*" + ("/" + db + ".db/" + tableTwo).toLowerCase()));

    HCatCreateTableDesc tableDesc3 = HCatCreateTableDesc.create(db,
      tableThree, cols).fileFormat("orcfile").build();
    client.createTable(tableDesc3);
    HCatTable table3 = client.getTable(db, tableThree);
    assertTrue(table3.getInputFileFormat().equalsIgnoreCase(
      OrcInputFormat.class.getName()));
    assertTrue(table3.getOutputFileFormat().equalsIgnoreCase(
      OrcOutputFormat.class.getName()));
    assertTrue(table3.getSerdeLib().equalsIgnoreCase(
      OrcSerde.class.getName()));
    assertTrue(table1.getCols().equals(cols));

    client.close();
  }

  /**
   * This test tests that a plain table instantiation matches what hive says an
   * empty table create should look like.
   * @throws Exception
   */
  @Test
  public void testEmptyTableInstantiation() throws Exception {
    HCatClient client = HCatClient.create(new Configuration(hcatConf));


    String dbName = "default";
    String tblName = "testEmptyCreate";
    ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
    cols.add(new HCatFieldSchema("id", Type.INT, "id comment"));
    cols.add(new HCatFieldSchema("value", Type.STRING, "value comment"));

    client.dropTable(dbName, tblName, true);
    // Create a minimalistic table
    client.createTable(HCatCreateTableDesc
        .create(new HCatTable(dbName, tblName).cols(cols), false)
        .build());

    HCatTable tCreated = client.getTable(dbName, tblName);

    org.apache.hadoop.hive.metastore.api.Table emptyTable = Table.getEmptyTable(dbName, tblName);

    Map<String, String> createdProps = tCreated.getTblProps();
    Map<String, String> emptyProps = emptyTable.getParameters();

    mapEqualsContainedIn(emptyProps, createdProps);

    // Test sd params - we check that all the parameters in an empty table
    // are retained as-is. We may add beyond it, but not change values for
    // any parameters that hive defines for an empty table.

    Map<String, String> createdSdParams = tCreated.getSerdeParams();
    Map<String, String> emptySdParams = emptyTable.getSd().getSerdeInfo().getParameters();

    mapEqualsContainedIn(emptySdParams, createdSdParams);
  }

  /**
   * Verifies that an inner map is present inside an outer map, with
   * all values being equal.
   */
  private void mapEqualsContainedIn(Map<String, String> inner, Map<String, String> outer) {
    assertNotNull(inner);
    assertNotNull(outer);
    for ( Map.Entry<String,String> e : inner.entrySet()){
      assertTrue(outer.containsKey(e.getKey()));
      assertEquals(outer.get(e.getKey()), e.getValue());
    }
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
    HCatTable table = new HCatTable(dbName, tableName).cols(cols)
                                                      .partCols(ptnCols)
                                                      .fileFormat("sequenceFile");
    HCatCreateTableDesc tableDesc = HCatCreateTableDesc.create(table, false).build();
    client.createTable(tableDesc);

    // Verify that the table is created successfully.
    table = client.getTable(dbName, tableName);

    Map<String, String> firstPtn = new HashMap<String, String>();
    firstPtn.put("dt", "04/30/2012");
    firstPtn.put("country", "usa");
    // Test new HCatAddPartitionsDesc API.
    HCatAddPartitionDesc addPtn = HCatAddPartitionDesc.create(new HCatPartition(table, firstPtn, null)).build();
    client.addPartition(addPtn);

    Map<String, String> secondPtn = new HashMap<String, String>();
    secondPtn.put("dt", "04/12/2012");
    secondPtn.put("country", "brazil");
    // Test deprecated HCatAddPartitionsDesc API.
    HCatAddPartitionDesc addPtn2 = HCatAddPartitionDesc.create(dbName,
      tableName, null, secondPtn).build();
    client.addPartition(addPtn2);

    Map<String, String> thirdPtn = new HashMap<String, String>();
    thirdPtn.put("dt", "04/13/2012");
    thirdPtn.put("country", "argentina");
    // Test deprecated HCatAddPartitionsDesc API.
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
    assertTrue(newDB.getLocation().matches(".*/tmp/" + dbName));
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
      HCatTable table = new HCatTable(dbName, tableName).cols(columns).partCols(partitionColumns);
      client.createTable(HCatCreateTableDesc.create(table, false).build());

      HCatTable createdTable = client.getTable(dbName,tableName);

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

      client.addPartition(HCatAddPartitionDesc.create(new HCatPartition(createdTable, partitionSpec,
          makePartLocation(createdTable,partitionSpec))).build());

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

      HCatTable table = new HCatTable(dbName, tableName).cols(columnSchema).partCols(partitionSchema);
      client.createTable(HCatCreateTableDesc.create(table, false).build());

      // Verify that the table was created successfully.
      table = client.getTable(dbName, tableName);
      assertNotNull("The created just now can't be null.", table);

      Map<String, String> partitionSpec = new HashMap<String, String>();
      partitionSpec.put("grid", "AB");
      partitionSpec.put("dt", "2011_12_31");
      client.addPartition(HCatAddPartitionDesc.create(new HCatPartition(table, partitionSpec,
          makePartLocation(table,partitionSpec))).build());
      partitionSpec.put("grid", "AB");
      partitionSpec.put("dt", "2012_01_01");
      client.addPartition(HCatAddPartitionDesc.create(new HCatPartition(table, partitionSpec,
          makePartLocation(table,partitionSpec))).build());
      partitionSpec.put("dt", "2012_01_01");
      partitionSpec.put("grid", "OB");
      client.addPartition(HCatAddPartitionDesc.create(new HCatPartition(table, partitionSpec,
          makePartLocation(table,partitionSpec))).build());
      partitionSpec.put("dt", "2012_01_01");
      partitionSpec.put("grid", "XB");
      client.addPartition(HCatAddPartitionDesc.create(new HCatPartition(table, partitionSpec,
          makePartLocation(table,partitionSpec))).build());

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

      HCatTable table = new HCatTable(dbName, tableName).cols(columnSchema).partCols(partitionSchema);
      client.createTable(HCatCreateTableDesc.create(table, false).build());

      // Verify that the table was created successfully.
      table = client.getTable(dbName, tableName);
      assertNotNull("Table couldn't be queried for. ", table);

      Map<String, String> partitionSpec = new HashMap<String, String>();
      partitionSpec.put("grid", "AB");
      partitionSpec.put("dt", "2011_12_31");
      client.addPartition(HCatAddPartitionDesc.create(new HCatPartition(table, partitionSpec,
          makePartLocation(table, partitionSpec))).build());
      partitionSpec.put("grid", "AB");
      partitionSpec.put("dt", "2012_01_01");
      client.addPartition(HCatAddPartitionDesc.create(new HCatPartition(table, partitionSpec,
          makePartLocation(table, partitionSpec))).build());
      partitionSpec.put("dt", "2012_01_01");
      partitionSpec.put("grid", "OB");
      client.addPartition(HCatAddPartitionDesc.create(new HCatPartition(table, partitionSpec,
          makePartLocation(table, partitionSpec))).build());
      partitionSpec.put("dt", "2012_01_01");
      partitionSpec.put("grid", "XB");
      client.addPartition(HCatAddPartitionDesc.create(new HCatPartition(table, partitionSpec,
          makePartLocation(table, partitionSpec))).build());

      Map<String, String> partialPartitionSpec = new HashMap<String, String>();
      partialPartitionSpec.put("dt", "2012_01_01");

      client.dropPartitions(dbName, tableName, partialPartitionSpec, true);

      List<HCatPartition> partitions = client.getPartitions(dbName, tableName);
      assertEquals("Unexpected number of partitions.", 1, partitions.size());
      assertArrayEquals("Mismatched partition.", new String[]{"2011_12_31", "AB"}, partitions.get(0).getValues().toArray());

      List<HCatFieldSchema> partColumns = partitions.get(0).getPartColumns();
      assertEquals(2, partColumns.size());
      assertEquals("dt", partColumns.get(0).getName());
      assertEquals("grid", partColumns.get(1).getName());

      client.dropDatabase(dbName, false, HCatClient.DropDBMode.CASCADE);
    }
    catch (Exception unexpected) {
      LOG.error("Unexpected exception!", unexpected);
      assertTrue("Unexpected exception! " + unexpected.getMessage(), false);
    }
  }

  private void startReplicationTargetMetaStoreIfRequired() throws Exception {
    if (!isReplicationTargetHCatRunning) {
      Thread t = new Thread(new RunMS(replicationTargetHCatPort)
                              .arg("--hiveconf")
                              .arg("javax.jdo.option.ConnectionURL") // Reset, to use a different Derby instance.
                              .arg(hcatConf.get("javax.jdo.option.ConnectionURL")
                                                 .replace("metastore", "target_metastore")));
      t.start();
      Thread.sleep(10000);
      replicationTargetHCatConf = new HiveConf(hcatConf);
      replicationTargetHCatConf.setVar(HiveConf.ConfVars.METASTOREURIS,
                                       "thrift://localhost:" + replicationTargetHCatPort);
      isReplicationTargetHCatRunning = true;
    }
  }

  /**
   * Test for event-based replication scenario
   *
   * Does not test if replication actually happened, merely tests if we're able to consume a repl task
   * iter appropriately, calling all the functions expected of the interface, without errors.
   */
  @Test
  public void testReplicationTaskIter() throws Exception {

    Configuration cfg = new Configuration(hcatConf);
    cfg.set(HiveConf.ConfVars.METASTORE_BATCH_RETRIEVE_MAX.varname,"10"); // set really low batch size to ensure batching
    cfg.set(HiveConf.ConfVars.HIVE_REPL_TASK_FACTORY.varname, EximReplicationTaskFactory.class.getName());
    HCatClient sourceMetastore = HCatClient.create(cfg);

    String dbName = "testReplicationTaskIter";
    long baseId = sourceMetastore.getCurrentNotificationEventId();

    {
      // Perform some operations

      // 1: Create a db after dropping if needed => 1 or 2 events
      sourceMetastore.dropDatabase(dbName, true, HCatClient.DropDBMode.CASCADE);
      sourceMetastore.createDatabase(HCatCreateDBDesc.create(dbName).ifNotExists(false).build());

      // 2: Create an unpartitioned table T1 => 1 event
      String tblName1 = "T1";
      List<HCatFieldSchema> cols1 = HCatSchemaUtils.getHCatSchema("a:int,b:string").getFields();
      HCatTable table1 = (new HCatTable(dbName, tblName1)).cols(cols1);
      sourceMetastore.createTable(HCatCreateTableDesc.create(table1).build());

      // 3: Create a partitioned table T2 => 1 event

      String tblName2 = "T2";
      List<HCatFieldSchema> cols2 = HCatSchemaUtils.getHCatSchema("a:int").getFields();
      List<HCatFieldSchema> pcols2 = HCatSchemaUtils.getHCatSchema("b:string").getFields();
      HCatTable table2 = (new HCatTable(dbName, tblName2)).cols(cols2).partCols(pcols2);
      sourceMetastore.createTable(HCatCreateTableDesc.create(table2).build());

      // 4: Add a partition P1 to T2 => 1 event

      HCatTable table2Created = sourceMetastore.getTable(dbName,tblName2);

      Map<String, String> ptnDesc1 = new HashMap<String,String>();
      ptnDesc1.put("b","test1");
      HCatPartition ptn1 = (new HCatPartition(table2Created, ptnDesc1,
          makePartLocation(table2Created,ptnDesc1)));
      sourceMetastore.addPartition(HCatAddPartitionDesc.create(ptn1).build());

      // 5 : Create and drop partition P2 to T2 10 times => 20 events

      for (int i = 0; i < 20; i++){
        Map<String, String> ptnDesc = new HashMap<String,String>();
        ptnDesc.put("b","testmul"+i);
        HCatPartition ptn = (new HCatPartition(table2Created, ptnDesc,
            makePartLocation(table2Created,ptnDesc)));
        sourceMetastore.addPartition(HCatAddPartitionDesc.create(ptn).build());
        sourceMetastore.dropPartitions(dbName,tblName2,ptnDesc,true);
      }

      // 6 : Drop table T1 => 1 event
      sourceMetastore.dropTable(dbName, tblName1, true);

      // 7 : Drop table T2 => 1 event
      sourceMetastore.dropTable(dbName, tblName2, true);

      // verify that the number of events since we began is at least 25 more
      long currId = sourceMetastore.getCurrentNotificationEventId();
      assertTrue("currId[" + currId + "] must be more than 25 greater than baseId[" + baseId + "]", currId > baseId + 25);

    }

    // do rest of tests on db we just picked up above.

    List<HCatNotificationEvent> notifs = sourceMetastore.getNextNotification(
        0, 0, new IMetaStoreClient.NotificationFilter() {
      @Override
      public boolean accept(NotificationEvent event) {
        return true;
      }
    });
    for(HCatNotificationEvent n : notifs){
      LOG.info("notif from dblistener:" + n.getEventId()
          + ":" + n.getEventTime() + ",t:" + n.getEventType() + ",o:" + n.getDbName() + "." + n.getTableName());
    }

    Iterator<ReplicationTask> taskIter = sourceMetastore.getReplicationTasks(0, -1, dbName, null);
    while(taskIter.hasNext()){
      ReplicationTask task = taskIter.next();
      HCatNotificationEvent n = task.getEvent();
      LOG.info("notif from tasks:" + n.getEventId()
          + ":" + n.getEventTime() + ",t:" + n.getEventType() + ",o:" + n.getDbName() + "." + n.getTableName()
          + ",s:" + n.getEventScope());
      LOG.info("task :" + task.getClass().getName());
      if (task.needsStagingDirs()){
        StagingDirectoryProvider provider = new StagingDirectoryProvider() {
          @Override
          public String getStagingDirectory(String key) {
            LOG.info("getStagingDirectory(" + key + ") called!");
            return "/tmp/" + key.replaceAll(" ","_");
          }
        };
        task
            .withSrcStagingDirProvider(provider)
            .withDstStagingDirProvider(provider);
      }
      if (task.isActionable()){
        LOG.info("task was actionable!");
        Function<Command, String> commandDebugPrinter = new Function<Command, String>() {
          @Override
          public String apply(@Nullable Command cmd) {
            StringBuilder sb = new StringBuilder();
            String serializedCmd = null;
            try {
              serializedCmd = ReplicationUtils.serializeCommand(cmd);
            } catch (IOException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }
            sb.append("SERIALIZED:"+serializedCmd+"\n");
            Command command = null;
            try {
              command = ReplicationUtils.deserializeCommand(serializedCmd);
            } catch (IOException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }
            sb.append("CMD:[" + command.getClass().getName() + "]\n");
            sb.append("EVENTID:[" +command.getEventId()+"]\n");
            for (String s : command.get()) {
              sb.append("CMD:" + s);
              sb.append("\n");
            }
            sb.append("Retriable:" + command.isRetriable() + "\n");
            sb.append("Undoable:" + command.isUndoable() + "\n");
            if (command.isUndoable()) {
              for (String s : command.getUndo()) {
                sb.append("UNDO:" + s);
                sb.append("\n");
              }
            }
            List<String> locns = command.cleanupLocationsPerRetry();
            sb.append("cleanupLocationsPerRetry entries :" + locns.size());
            for (String s : locns){
              sb.append("RETRY_CLEANUP:"+s);
              sb.append("\n");
            }
            locns = command.cleanupLocationsAfterEvent();
            sb.append("cleanupLocationsAfterEvent entries :" + locns.size());
            for (String s : locns){
              sb.append("AFTER_EVENT_CLEANUP:"+s);
              sb.append("\n");
            }
            return sb.toString();
          }
        };
        LOG.info("On src:");
        for (String s : Iterables.transform(task.getSrcWhCommands(), commandDebugPrinter)){
          LOG.info(s);
        }
        LOG.info("On dest:");
        for (String s : Iterables.transform(task.getDstWhCommands(), commandDebugPrinter)){
          LOG.info(s);
        }
      } else {
        LOG.info("task was not actionable.");
      }
    }
  }

  /**
   * Test for detecting schema-changes for an HCatalog table, across 2 different HCat instances.
   * A table is created with the same schema on 2 HCat instances. The table-schema is modified on the source HCat
   * instance (columns, I/O formats, SerDe definitions, etc.). The table metadata is compared between source
   * and target, the changes are detected and propagated to target.
   * @throws Exception
   */
  @Test
  public void testTableSchemaPropagation() throws Exception {
    try {
      startReplicationTargetMetaStoreIfRequired();
      HCatClient sourceMetaStore = HCatClient.create(new Configuration(hcatConf));
      final String dbName = "myDb";
      final String tableName = "myTable";

      sourceMetaStore.dropDatabase(dbName, true, HCatClient.DropDBMode.CASCADE);

      sourceMetaStore.createDatabase(HCatCreateDBDesc.create(dbName).build());
      List<HCatFieldSchema> columnSchema = Arrays.asList(new HCatFieldSchema("foo", Type.INT, ""),
          new HCatFieldSchema("bar", Type.STRING, ""));

      List<HCatFieldSchema> partitionSchema = Arrays.asList(new HCatFieldSchema("dt", Type.STRING, ""),
          new HCatFieldSchema("grid", Type.STRING, ""));

      HCatTable sourceTable = new HCatTable(dbName, tableName).cols(columnSchema).partCols(partitionSchema);
      sourceMetaStore.createTable(HCatCreateTableDesc.create(sourceTable).build());

      // Verify that the sourceTable was created successfully.
      sourceTable = sourceMetaStore.getTable(dbName, tableName);
      assertNotNull("Table couldn't be queried for. ", sourceTable);

      // Serialize Table definition. Deserialize using the target HCatClient instance.
      String tableStringRep = sourceMetaStore.serializeTable(sourceTable);
      HCatClient targetMetaStore = HCatClient.create(new Configuration(replicationTargetHCatConf));
      targetMetaStore.dropDatabase(dbName, true, HCatClient.DropDBMode.CASCADE);
      targetMetaStore.createDatabase(HCatCreateDBDesc.create(dbName).build());

      HCatTable targetTable = targetMetaStore.deserializeTable(tableStringRep);

      assertEquals("Table after deserialization should have been identical to sourceTable.",
          HCatTable.NO_DIFF, sourceTable.diff(targetTable));

      // Create table on Target.
      targetMetaStore.createTable(HCatCreateTableDesc.create(targetTable).build());
      // Verify that the created table is identical to sourceTable.
      targetTable = targetMetaStore.getTable(dbName, tableName);
      assertEquals("Table after deserialization should have been identical to sourceTable.",
          HCatTable.NO_DIFF, sourceTable.diff(targetTable));

      // Modify sourceTable.
      List<HCatFieldSchema> newColumnSchema = new ArrayList<HCatFieldSchema>(columnSchema);
      newColumnSchema.add(new HCatFieldSchema("goo_new", Type.DOUBLE, ""));
      Map<String, String> tableParams = new HashMap<String, String>(1);
      tableParams.put("orc.compress", "ZLIB");
      sourceTable.cols(newColumnSchema) // Add a column.
                 .fileFormat("orcfile")     // Change SerDe, File I/O formats.
                 .tblProps(tableParams)
                 .serdeParam(serdeConstants.FIELD_DELIM, Character.toString('\001'));
      sourceMetaStore.updateTableSchema(dbName, tableName, sourceTable);
      sourceTable = sourceMetaStore.getTable(dbName, tableName);

      // Diff against table on target.

      EnumSet<HCatTable.TableAttribute> diff = targetTable.diff(sourceTable);
      assertTrue("Couldn't find change in column-schema.",
          diff.contains(HCatTable.TableAttribute.COLUMNS));
      assertTrue("Couldn't find change in InputFormat.",
          diff.contains(HCatTable.TableAttribute.INPUT_FORMAT));
      assertTrue("Couldn't find change in OutputFormat.",
          diff.contains(HCatTable.TableAttribute.OUTPUT_FORMAT));
      assertTrue("Couldn't find change in SerDe.",
          diff.contains(HCatTable.TableAttribute.SERDE));
      assertTrue("Couldn't find change in SerDe parameters.",
          diff.contains(HCatTable.TableAttribute.SERDE_PROPERTIES));
      assertTrue("Couldn't find change in Table parameters.",
          diff.contains(HCatTable.TableAttribute.TABLE_PROPERTIES));

      // Replicate the changes to the replicated-table.
      targetMetaStore.updateTableSchema(dbName, tableName, targetTable.resolve(sourceTable, diff));
      targetTable = targetMetaStore.getTable(dbName, tableName);

      assertEquals("After propagating schema changes, source and target tables should have been equivalent.",
          HCatTable.NO_DIFF, targetTable.diff(sourceTable));

    }
    catch (Exception unexpected) {
      LOG.error("Unexpected exception!", unexpected);
      assertTrue("Unexpected exception! " + unexpected.getMessage(), false);
    }
  }

  /**
   * Test that partition-definitions can be replicated between HCat-instances,
   * independently of table-metadata replication.
   * 2 identical tables are created on 2 different HCat instances ("source" and "target").
   * On the source instance,
   * 1. One partition is added with the old format ("TEXTFILE").
   * 2. The table is updated with an additional column and the data-format changed to ORC.
   * 3. Another partition is added with the new format.
   * 4. The partitions' metadata is copied to the target HCat instance, without updating the target table definition.
   * 5. The partitions' metadata is tested to be an exact replica of that on the source.
   * @throws Exception
   */
  @Test
  public void testPartitionRegistrationWithCustomSchema() throws Exception {
    try {
      startReplicationTargetMetaStoreIfRequired();

      HCatClient sourceMetaStore = HCatClient.create(new Configuration(hcatConf));
      final String dbName = "myDb";
      final String tableName = "myTable";

      sourceMetaStore.dropDatabase(dbName, true, HCatClient.DropDBMode.CASCADE);

      sourceMetaStore.createDatabase(HCatCreateDBDesc.create(dbName).build());
      List<HCatFieldSchema> columnSchema = new ArrayList<HCatFieldSchema>(
          Arrays.asList(new HCatFieldSchema("foo", Type.INT, ""),
                        new HCatFieldSchema("bar", Type.STRING, "")));

      List<HCatFieldSchema> partitionSchema = Arrays.asList(new HCatFieldSchema("dt", Type.STRING, ""),
                                                            new HCatFieldSchema("grid", Type.STRING, ""));

      HCatTable sourceTable = new HCatTable(dbName, tableName).cols(columnSchema)
                                                              .partCols(partitionSchema)
                                                              .comment("Source table.");

      sourceMetaStore.createTable(HCatCreateTableDesc.create(sourceTable).build());

      // Verify that the sourceTable was created successfully.
      sourceTable = sourceMetaStore.getTable(dbName, tableName);
      assertNotNull("Table couldn't be queried for. ", sourceTable);

      // Partitions added now should inherit table-schema, properties, etc.
      Map<String, String> partitionSpec_1 = new HashMap<String, String>();
      partitionSpec_1.put("grid", "AB");
      partitionSpec_1.put("dt", "2011_12_31");
      HCatPartition sourcePartition_1 = new HCatPartition(sourceTable, partitionSpec_1,
          makePartLocation(sourceTable,partitionSpec_1));

      sourceMetaStore.addPartition(HCatAddPartitionDesc.create(sourcePartition_1).build());
      assertEquals("Unexpected number of partitions. ",
                   1, sourceMetaStore.getPartitions(dbName, tableName).size());
      // Verify that partition_1 was added correctly, and properties were inherited from the HCatTable.
      HCatPartition addedPartition_1 = sourceMetaStore.getPartition(dbName, tableName, partitionSpec_1);
      assertEquals("Column schema doesn't match.", sourceTable.getCols(), addedPartition_1.getColumns());
      assertEquals("InputFormat doesn't match.", sourceTable.getInputFileFormat(), addedPartition_1.getInputFormat());
      assertEquals("OutputFormat doesn't match.", sourceTable.getOutputFileFormat(), addedPartition_1.getOutputFormat());
      assertEquals("SerDe doesn't match.", sourceTable.getSerdeLib(), addedPartition_1.getSerDe());
      assertEquals("SerDe params don't match.", sourceTable.getSerdeParams(), addedPartition_1.getSerdeParams());

      // Replicate table definition.

      HCatClient targetMetaStore = HCatClient.create(new Configuration(replicationTargetHCatConf));
      targetMetaStore.dropDatabase(dbName, true, HCatClient.DropDBMode.CASCADE);

      targetMetaStore.createDatabase(HCatCreateDBDesc.create(dbName).build());
      // Make a copy of the source-table, as would be done across class-loaders.
      HCatTable targetTable = targetMetaStore.deserializeTable(sourceMetaStore.serializeTable(sourceTable));
      targetMetaStore.createTable(HCatCreateTableDesc.create(targetTable).build());
      targetTable = targetMetaStore.getTable(dbName, tableName);

      assertEquals("Created table doesn't match the source.", HCatTable.NO_DIFF, targetTable.diff(sourceTable));

      // Modify Table schema at the source.
      List<HCatFieldSchema> newColumnSchema = new ArrayList<HCatFieldSchema>(columnSchema);
      newColumnSchema.add(new HCatFieldSchema("goo_new", Type.DOUBLE, ""));
      Map<String, String> tableParams = new HashMap<String, String>(1);
      tableParams.put("orc.compress", "ZLIB");
      sourceTable.cols(newColumnSchema) // Add a column.
          .fileFormat("orcfile")     // Change SerDe, File I/O formats.
          .tblProps(tableParams)
          .serdeParam(serdeConstants.FIELD_DELIM, Character.toString('\001'));
      sourceMetaStore.updateTableSchema(dbName, tableName, sourceTable);
      sourceTable = sourceMetaStore.getTable(dbName, tableName);

      // Add another partition to the source.
      Map<String, String> partitionSpec_2 = new HashMap<String, String>();
      partitionSpec_2.put("grid", "AB");
      partitionSpec_2.put("dt", "2012_01_01");
      HCatPartition sourcePartition_2 = new HCatPartition(sourceTable, partitionSpec_2,
          makePartLocation(sourceTable,partitionSpec_2));
      sourceMetaStore.addPartition(HCatAddPartitionDesc.create(sourcePartition_2).build());

      // The source table now has 2 partitions, one in TEXTFILE, the other in ORC.
      // Test adding these partitions to the target-table *without* replicating the table-change.

      List<HCatPartition> sourcePartitions = sourceMetaStore.getPartitions(dbName, tableName);
      assertEquals("Unexpected number of source partitions.", 2, sourcePartitions.size());

      List<HCatAddPartitionDesc> addPartitionDescs = new ArrayList<HCatAddPartitionDesc>(sourcePartitions.size());
      for (HCatPartition partition : sourcePartitions) {
        addPartitionDescs.add(HCatAddPartitionDesc.create(partition).build());
      }

      targetMetaStore.addPartitions(addPartitionDescs);

      List<HCatPartition> targetPartitions = targetMetaStore.getPartitions(dbName, tableName);

      assertEquals("Expected the same number of partitions. ", sourcePartitions.size(), targetPartitions.size());

      for (int i=0; i<targetPartitions.size(); ++i) {
        HCatPartition sourcePartition = sourcePartitions.get(i),
                      targetPartition = targetPartitions.get(i);
        assertEquals("Column schema doesn't match.", sourcePartition.getColumns(), targetPartition.getColumns());
        assertEquals("InputFormat doesn't match.", sourcePartition.getInputFormat(), targetPartition.getInputFormat());
        assertEquals("OutputFormat doesn't match.", sourcePartition.getOutputFormat(), targetPartition.getOutputFormat());
        assertEquals("SerDe doesn't match.", sourcePartition.getSerDe(), targetPartition.getSerDe());
        assertEquals("SerDe params don't match.", sourcePartition.getSerdeParams(), targetPartition.getSerdeParams());
      }

    }
    catch (Exception unexpected) {
      LOG.error( "Unexpected exception! ",  unexpected);
      assertTrue("Unexpected exception! " + unexpected.getMessage(), false);
    }
  }

  /**
   * Test that partition-definitions can be replicated between HCat-instances,
   * independently of table-metadata replication, using PartitionSpec interfaces.
   * (This is essentially the same test as testPartitionRegistrationWithCustomSchema(),
   * transliterated to use the PartitionSpec APIs.)
   * 2 identical tables are created on 2 different HCat instances ("source" and "target").
   * On the source instance,
   * 1. One partition is added with the old format ("TEXTFILE").
   * 2. The table is updated with an additional column and the data-format changed to ORC.
   * 3. Another partition is added with the new format.
   * 4. The partitions' metadata is copied to the target HCat instance, without updating the target table definition.
   * 5. The partitions' metadata is tested to be an exact replica of that on the source.
   * @throws Exception
   */
  @Test
  public void testPartitionSpecRegistrationWithCustomSchema() throws Exception {
    try {
      startReplicationTargetMetaStoreIfRequired();

      HCatClient sourceMetaStore = HCatClient.create(new Configuration(hcatConf));
      final String dbName = "myDb";
      final String tableName = "myTable";

      sourceMetaStore.dropDatabase(dbName, true, HCatClient.DropDBMode.CASCADE);

      sourceMetaStore.createDatabase(HCatCreateDBDesc.create(dbName).build());
      List<HCatFieldSchema> columnSchema = new ArrayList<HCatFieldSchema>(
          Arrays.asList(new HCatFieldSchema("foo", Type.INT, ""),
              new HCatFieldSchema("bar", Type.STRING, "")));

      List<HCatFieldSchema> partitionSchema = Arrays.asList(new HCatFieldSchema("dt", Type.STRING, ""),
          new HCatFieldSchema("grid", Type.STRING, ""));

      HCatTable sourceTable = new HCatTable(dbName, tableName).cols(columnSchema)
          .partCols(partitionSchema)
          .comment("Source table.");

      sourceMetaStore.createTable(HCatCreateTableDesc.create(sourceTable).build());

      // Verify that the sourceTable was created successfully.
      sourceTable = sourceMetaStore.getTable(dbName, tableName);
      assertNotNull("Table couldn't be queried for. ", sourceTable);

      // Partitions added now should inherit table-schema, properties, etc.
      Map<String, String> partitionSpec_1 = new HashMap<String, String>();
      partitionSpec_1.put("grid", "AB");
      partitionSpec_1.put("dt", "2011_12_31");
      HCatPartition sourcePartition_1 = new HCatPartition(sourceTable, partitionSpec_1,
          makePartLocation(sourceTable,partitionSpec_1));

      sourceMetaStore.addPartition(HCatAddPartitionDesc.create(sourcePartition_1).build());
      assertEquals("Unexpected number of partitions. ",
          1, sourceMetaStore.getPartitions(dbName, tableName).size());
      // Verify that partition_1 was added correctly, and properties were inherited from the HCatTable.
      HCatPartition addedPartition_1 = sourceMetaStore.getPartition(dbName, tableName, partitionSpec_1);
      assertEquals("Column schema doesn't match.", sourceTable.getCols(), addedPartition_1.getColumns());
      assertEquals("InputFormat doesn't match.", sourceTable.getInputFileFormat(), addedPartition_1.getInputFormat());
      assertEquals("OutputFormat doesn't match.", sourceTable.getOutputFileFormat(), addedPartition_1.getOutputFormat());
      assertEquals("SerDe doesn't match.", sourceTable.getSerdeLib(), addedPartition_1.getSerDe());
      assertEquals("SerDe params don't match.", sourceTable.getSerdeParams(), addedPartition_1.getSerdeParams());

      // Replicate table definition.

      HCatClient targetMetaStore = HCatClient.create(new Configuration(replicationTargetHCatConf));
      targetMetaStore.dropDatabase(dbName, true, HCatClient.DropDBMode.CASCADE);

      targetMetaStore.createDatabase(HCatCreateDBDesc.create(dbName).build());
      // Make a copy of the source-table, as would be done across class-loaders.
      HCatTable targetTable = targetMetaStore.deserializeTable(sourceMetaStore.serializeTable(sourceTable));
      targetMetaStore.createTable(HCatCreateTableDesc.create(targetTable).build());
      targetTable = targetMetaStore.getTable(dbName, tableName);

      assertEquals("Created table doesn't match the source.", HCatTable.NO_DIFF, targetTable.diff(sourceTable));

      // Modify Table schema at the source.
      List<HCatFieldSchema> newColumnSchema = new ArrayList<HCatFieldSchema>(columnSchema);
      newColumnSchema.add(new HCatFieldSchema("goo_new", Type.DOUBLE, ""));
      Map<String, String> tableParams = new HashMap<String, String>(1);
      tableParams.put("orc.compress", "ZLIB");
      sourceTable.cols(newColumnSchema) // Add a column.
          .fileFormat("orcfile")     // Change SerDe, File I/O formats.
          .tblProps(tableParams)
          .serdeParam(serdeConstants.FIELD_DELIM, Character.toString('\001'));
      sourceMetaStore.updateTableSchema(dbName, tableName, sourceTable);
      sourceTable = sourceMetaStore.getTable(dbName, tableName);

      // Add another partition to the source.
      Map<String, String> partitionSpec_2 = new HashMap<String, String>();
      partitionSpec_2.put("grid", "AB");
      partitionSpec_2.put("dt", "2012_01_01");
      HCatPartition sourcePartition_2 = new HCatPartition(sourceTable, partitionSpec_2,
          makePartLocation(sourceTable,partitionSpec_2));
      sourceMetaStore.addPartition(HCatAddPartitionDesc.create(sourcePartition_2).build());

      // The source table now has 2 partitions, one in TEXTFILE, the other in ORC.
      // Test adding these partitions to the target-table *without* replicating the table-change.

      HCatPartitionSpec sourcePartitionSpec = sourceMetaStore.getPartitionSpecs(dbName, tableName, -1);
      assertEquals("Unexpected number of source partitions.", 2, sourcePartitionSpec.size());

      // Serialize the hcatPartitionSpec.
      List<String> partitionSpecString = sourceMetaStore.serializePartitionSpec(sourcePartitionSpec);

      // Deserialize the HCatPartitionSpec using the target HCatClient instance.
      HCatPartitionSpec targetPartitionSpec = targetMetaStore.deserializePartitionSpec(partitionSpecString);
      assertEquals("Could not add the expected number of partitions.",
          sourcePartitionSpec.size(), targetMetaStore.addPartitionSpec(targetPartitionSpec));

      // Retrieve partitions.
      targetPartitionSpec = targetMetaStore.getPartitionSpecs(dbName, tableName, -1);
      assertEquals("Could not retrieve the expected number of partitions.",
          sourcePartitionSpec.size(), targetPartitionSpec.size());

      // Assert that the source and target partitions are equivalent.
      HCatPartitionSpec.HCatPartitionIterator sourceIterator = sourcePartitionSpec.getPartitionIterator();
      HCatPartitionSpec.HCatPartitionIterator targetIterator = targetPartitionSpec.getPartitionIterator();

      while (targetIterator.hasNext()) {
        assertTrue("Fewer target partitions than source.", sourceIterator.hasNext());
        HCatPartition sourcePartition = sourceIterator.next();
        HCatPartition targetPartition = targetIterator.next();
        assertEquals("Column schema doesn't match.", sourcePartition.getColumns(), targetPartition.getColumns());
        assertEquals("InputFormat doesn't match.", sourcePartition.getInputFormat(), targetPartition.getInputFormat());
        assertEquals("OutputFormat doesn't match.", sourcePartition.getOutputFormat(), targetPartition.getOutputFormat());
        assertEquals("SerDe doesn't match.", sourcePartition.getSerDe(), targetPartition.getSerDe());
        assertEquals("SerDe params don't match.", sourcePartition.getSerdeParams(), targetPartition.getSerdeParams());

      }
    }
    catch (Exception unexpected) {
      LOG.error( "Unexpected exception! ",  unexpected);
      assertTrue("Unexpected exception! " + unexpected.getMessage(), false);
    }
  }

}
