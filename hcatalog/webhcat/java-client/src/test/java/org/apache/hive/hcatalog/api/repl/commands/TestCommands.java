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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.api.repl.commands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.hcatalog.HcatTestUtils;
import org.apache.hive.hcatalog.api.HCatAddPartitionDesc;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatCreateDBDesc;
import org.apache.hive.hcatalog.api.HCatCreateTableDesc;
import org.apache.hive.hcatalog.api.HCatDatabase;
import org.apache.hive.hcatalog.api.HCatPartition;
import org.apache.hive.hcatalog.api.HCatTable;
import org.apache.hive.hcatalog.api.ObjectNotFoundException;
import org.apache.hive.hcatalog.api.TestHCatClient;
import org.apache.hive.hcatalog.api.repl.Command;
import org.apache.hive.hcatalog.api.repl.CommandTestUtils;
import org.apache.hive.hcatalog.api.repl.ReplicationUtils;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestCommands {

  private static Logger LOG = LoggerFactory.getLogger(CommandTestUtils.class.getName());

  private static HiveConf hconf;
  private static IDriver driver;
  private static HCatClient client;
  private static String TEST_PATH;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    TestHCatClient.startMetaStoreServer();
    hconf = TestHCatClient.getConf();
    hconf.set(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname,"");
    hconf
    .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    TEST_PATH = System.getProperty("test.warehouse.dir","/tmp") + Path.SEPARATOR +
        TestCommands.class.getCanonicalName() + "-" + System.currentTimeMillis();
    Path testPath = new Path(TEST_PATH);
    FileSystem fs = FileSystem.get(testPath.toUri(),hconf);
    fs.mkdirs(testPath);

    driver = DriverFactory.newDriver(hconf);
    SessionState.start(new CliSessionState(hconf));
    client = HCatClient.create(hconf);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TestHCatClient.tearDown();
  }

  @Test
  public void testDropDatabaseCommand() throws HCatException {
    String dbName = "cmd_testdb";
    int evid = 999;
    Command testCmd = new DropDatabaseCommand(dbName, evid);

    assertEquals(evid,testCmd.getEventId());
    assertEquals(1, testCmd.get().size());
    assertEquals(true,testCmd.isRetriable());
    assertEquals(false,testCmd.isUndoable());

    CommandTestUtils.testCommandSerialization(testCmd);

    client.dropDatabase(dbName, true, HCatClient.DropDBMode.CASCADE);
    client.createDatabase(HCatCreateDBDesc.create(dbName).ifNotExists(false).build());
    HCatDatabase db = client.getDatabase(dbName);
    assertNotNull(db);

    LOG.info("About to run :"+testCmd.get().get(0));
    driver.run(testCmd.get().get(0));

    Exception onfe = null;
    try {
      HCatDatabase db_del = client.getDatabase(dbName);
    } catch (Exception e) {
      onfe = e;
    }

    assertNotNull(onfe);
    assertTrue(onfe instanceof ObjectNotFoundException);
  }

  @Test
  public void testDropTableCommand() throws HCatException {
    String dbName = "cmd_testdb";
    String tableName = "cmd_testtable";
    int evid = 789;
    List<HCatFieldSchema> cols = HCatSchemaUtils.getHCatSchema("a:int,b:string").getFields();

    Command testReplicatedDropCmd = new DropTableCommand(dbName,tableName,true,evid);

    assertEquals(evid,testReplicatedDropCmd.getEventId());
    assertEquals(1, testReplicatedDropCmd.get().size());
    assertEquals(true, testReplicatedDropCmd.isRetriable());
    assertEquals(false, testReplicatedDropCmd.isUndoable());

    CommandTestUtils.testCommandSerialization(testReplicatedDropCmd);

    Command testNormalDropCmd = new DropTableCommand(dbName,tableName,false,evid);

    assertEquals(evid,testNormalDropCmd.getEventId());
    assertEquals(1, testNormalDropCmd.get().size());
    assertEquals(true,testNormalDropCmd.isRetriable());
    assertEquals(false,testNormalDropCmd.isUndoable());

    CommandTestUtils.testCommandSerialization(testNormalDropCmd);

    client.dropDatabase(dbName, true, HCatClient.DropDBMode.CASCADE);
    client.createDatabase(HCatCreateDBDesc.create(dbName).ifNotExists(false).build());

    Map<String, String> tprops = new HashMap<String,String>();
    tprops.put(ReplicationUtils.REPL_STATE_ID,String.valueOf(evid + 5));
    HCatTable tableToCreate = (new HCatTable(dbName, tableName)).tblProps(tprops).cols(cols);

    client.createTable(HCatCreateTableDesc.create(tableToCreate).build());
    HCatTable t1 = client.getTable(dbName, tableName);
    assertNotNull(t1);

    // Test replicated drop, should not drop, because evid < repl.state.id
    LOG.info("About to run :"+testReplicatedDropCmd.get().get(0));
    driver.run(testReplicatedDropCmd.get().get(0));
    HCatTable t2 = client.getTable(dbName,tableName);
    assertNotNull(t2);

    // Test normal drop, should drop unconditionally.
    LOG.info("About to run :"+testNormalDropCmd.get().get(0));
    driver.run(testNormalDropCmd.get().get(0));

    Exception onfe = null;
    try {
      HCatTable t_del = client.getTable(dbName, tableName);
    } catch (Exception e) {
      onfe = e;
    }

    assertNotNull(onfe);
    assertTrue(onfe instanceof ObjectNotFoundException);

    Map<String, String> tprops2 = new HashMap<String,String>();
    tprops2.put(ReplicationUtils.REPL_STATE_ID,String.valueOf(evid - 5));
    HCatTable tableToCreate2 = (new HCatTable(dbName, tableName)).tblProps(tprops2).cols(cols);

    client.createTable(HCatCreateTableDesc.create(tableToCreate2).build());
    HCatTable t3 = client.getTable(dbName, tableName);
    assertNotNull(t3);

    // Test replicated drop, should drop this time, since repl.state.id < evid.
    LOG.info("About to run :"+testReplicatedDropCmd.get().get(0));
    driver.run(testReplicatedDropCmd.get().get(0));

    Exception onfe2 = null;
    try {
      HCatTable t_del = client.getTable(dbName, tableName);
    } catch (Exception e) {
      onfe2 = e;
    }

    assertNotNull(onfe2);
    assertTrue(onfe2 instanceof ObjectNotFoundException);

  }

  @Test
  public void testDropPartitionCommand() throws HCatException, MetaException {
    String dbName = "cmd_testdb";
    String tableName = "cmd_testtable";
    int evid = 789;

    List<HCatFieldSchema> pcols = HCatSchemaUtils.getHCatSchema("b:string").getFields();
    List<HCatFieldSchema> cols = HCatSchemaUtils.getHCatSchema("a:int").getFields();
    Map<String, String> ptnDesc = new HashMap<String,String>();
    ptnDesc.put("b","test");

    Command testReplicatedDropPtnCmd = new DropPartitionCommand(dbName, tableName, ptnDesc, true, evid);

    assertEquals(evid,testReplicatedDropPtnCmd.getEventId());
    assertEquals(1, testReplicatedDropPtnCmd.get().size());
    assertEquals(true, testReplicatedDropPtnCmd.isRetriable());
    assertEquals(false, testReplicatedDropPtnCmd.isUndoable());

    CommandTestUtils.testCommandSerialization(testReplicatedDropPtnCmd);

    Command testNormalDropPtnCmd = new DropPartitionCommand(dbName,tableName, ptnDesc, false, evid);

    assertEquals(evid,testNormalDropPtnCmd.getEventId());
    assertEquals(1, testNormalDropPtnCmd.get().size());
    assertEquals(true,testNormalDropPtnCmd.isRetriable());
    assertEquals(false,testNormalDropPtnCmd.isUndoable());

    CommandTestUtils.testCommandSerialization(testNormalDropPtnCmd);

    client.dropDatabase(dbName, true, HCatClient.DropDBMode.CASCADE);
    client.createDatabase(HCatCreateDBDesc.create(dbName).ifNotExists(false).build());

    Map<String, String> props = new HashMap<String,String>();
    props.put(ReplicationUtils.REPL_STATE_ID,String.valueOf(evid + 5));
    HCatTable table = (new HCatTable(dbName, tableName)).tblProps(props).cols(cols).partCols(pcols);

    client.createTable(HCatCreateTableDesc.create(table).build());
    HCatTable tableCreated = client.getTable(dbName, tableName);
    assertNotNull(tableCreated);

    HCatPartition ptnToAdd = (new HCatPartition(tableCreated, ptnDesc,
        TestHCatClient.makePartLocation(tableCreated,ptnDesc))).parameters(props);
    client.addPartition(HCatAddPartitionDesc.create(ptnToAdd).build());

    HCatPartition p1 = client.getPartition(dbName,tableName,ptnDesc);
    assertNotNull(p1);

    // Test replicated drop, should not drop, because evid < repl.state.id
    LOG.info("About to run :"+testReplicatedDropPtnCmd.get().get(0));
    driver.run(testReplicatedDropPtnCmd.get().get(0));
    HCatPartition p2 = client.getPartition(dbName,tableName,ptnDesc);
    assertNotNull(p2);

    // Test normal drop, should drop unconditionally.
    LOG.info("About to run :"+testNormalDropPtnCmd.get().get(0));
    driver.run(testNormalDropPtnCmd.get().get(0));

    Exception onfe = null;
    try {
      HCatPartition p_del = client.getPartition(dbName,tableName,ptnDesc);
    } catch (Exception e) {
      onfe = e;
    }

    assertNotNull(onfe);
    assertTrue(onfe instanceof ObjectNotFoundException);

    Map<String, String> props2 = new HashMap<String,String>();
    props2.put(ReplicationUtils.REPL_STATE_ID,String.valueOf(evid - 5));

    HCatPartition ptnToAdd2 = (new HCatPartition(tableCreated, ptnDesc,
        TestHCatClient.makePartLocation(tableCreated,ptnDesc))).parameters(props2);
    client.addPartition(HCatAddPartitionDesc.create(ptnToAdd2).build());

    HCatPartition p3 = client.getPartition(dbName,tableName,ptnDesc);
    assertNotNull(p3);

    // Test replicated drop, should drop this time, since repl.state.id < evid.
    LOG.info("About to run :"+testReplicatedDropPtnCmd.get().get(0));
    driver.run(testReplicatedDropPtnCmd.get().get(0));

    Exception onfe2 = null;
    try {
      HCatPartition p_del = client.getPartition(dbName,tableName,ptnDesc);
    } catch (Exception e) {
      onfe2 = e;
    }

    assertNotNull(onfe2);
    assertTrue(onfe2 instanceof ObjectNotFoundException);
  }

  @Test
  public void testDropTableCommand2() throws HCatException, MetaException {
    // Secondary DropTableCommand test for testing repl-drop-tables' effect on partitions inside a partitioned table
    // when there exist partitions inside the table which are older than the drop event.
    // Our goal is this : Create a table t, with repl.last.id=157, say.
    // Create 2 partitions inside it, with repl.last.id=150 and 160, say.
    // Now, process a drop table command with eventid=155.
    // It should result in the table and the partition with repl.last.id=160 continuing to exist,
    // but dropping the partition with repl.last.id=150.

    String dbName = "cmd_testdb";
    String tableName = "cmd_testtable";
    int evid = 157;

    List<HCatFieldSchema> pcols = HCatSchemaUtils.getHCatSchema("b:string").getFields();
    List<HCatFieldSchema> cols = HCatSchemaUtils.getHCatSchema("a:int").getFields();

    Command testReplicatedDropCmd = new DropTableCommand(dbName,tableName,true,evid);

    client.dropDatabase(dbName, true, HCatClient.DropDBMode.CASCADE);
    client.createDatabase(HCatCreateDBDesc.create(dbName).ifNotExists(false).build());

    Map<String, String> tprops = new HashMap<String,String>();
    tprops.put(ReplicationUtils.REPL_STATE_ID,String.valueOf(evid + 2));
    HCatTable table = (new HCatTable(dbName, tableName)).tblProps(tprops).cols(cols).partCols(pcols);

    client.createTable(HCatCreateTableDesc.create(table).build());
    HCatTable tableCreated = client.getTable(dbName, tableName);
    assertNotNull(tableCreated);

    Map<String, String> ptnDesc1 = new HashMap<String,String>();
    ptnDesc1.put("b","test-older");
    Map<String, String> props1 = new HashMap<String,String>();
    props1.put(ReplicationUtils.REPL_STATE_ID,String.valueOf(evid - 5));
    HCatPartition ptnToAdd1 = (new HCatPartition(tableCreated, ptnDesc1,
        TestHCatClient.makePartLocation(tableCreated,ptnDesc1))).parameters(props1);
    client.addPartition(HCatAddPartitionDesc.create(ptnToAdd1).build());

    Map<String, String> ptnDesc2 = new HashMap<String,String>();
    ptnDesc2.put("b","test-newer");
    Map<String, String> props2 = new HashMap<String,String>();
    props2.put(ReplicationUtils.REPL_STATE_ID, String.valueOf(evid + 5));
    HCatPartition ptnToAdd2 = (new HCatPartition(tableCreated, ptnDesc2,
        TestHCatClient.makePartLocation(tableCreated,ptnDesc2))).parameters(props2);
    client.addPartition(HCatAddPartitionDesc.create(ptnToAdd2).build());

    HCatPartition p1 = client.getPartition(dbName,tableName,ptnDesc1);
    assertNotNull(p1);
    HCatPartition p2 = client.getPartition(dbName,tableName,ptnDesc2);
    assertNotNull(p2);

    LOG.info("About to run :"+testReplicatedDropCmd.get().get(0));
    driver.run(testReplicatedDropCmd.get().get(0));

    HCatTable t_stillExists = client.getTable(dbName,tableName);
    assertNotNull(t_stillExists);

    HCatPartition p2_stillExists = client.getPartition(dbName,tableName,ptnDesc2);

    Exception onfe = null;
    try {
      HCatPartition p1_del = client.getPartition(dbName,tableName,ptnDesc1);
    } catch (Exception e) {
      onfe = e;
    }

    assertNotNull(onfe);
    assertTrue(onfe instanceof ObjectNotFoundException);
  }


  @Test
  public void testBasicReplEximCommands() throws IOException {
    // repl export, has repl.last.id and repl.scope=all in it
    // import repl dump, table has repl.last.id on it (will likely be 0)
    int evid = 111;
    String exportLocation = TEST_PATH + File.separator + "testBasicReplExim";
    Path tempPath = new Path(TEST_PATH ,"testBasicReplEximTmp");
    String tempLocation = tempPath.toUri().getPath();

    String dbName = "exim";
    String tableName = "basicSrc";
    String importedTableName = "basicDst";
    List<HCatFieldSchema> cols = HCatSchemaUtils.getHCatSchema("b:string").getFields();

    client.dropDatabase(dbName, true, HCatClient.DropDBMode.CASCADE);
    client.createDatabase(HCatCreateDBDesc.create(dbName).ifNotExists(false).build());

    HCatTable table = (new HCatTable(dbName, tableName)).cols(cols).fileFormat("textfile");

    client.createTable(HCatCreateTableDesc.create(table).build());
    HCatTable t = client.getTable(dbName, tableName);
    assertNotNull(t);

    String[] data = new String[]{ "eleven" , "twelve" };

    HcatTestUtils.createTestDataFile(tempLocation,data);

    CommandProcessorResponse ret = driver.run(
        "LOAD DATA LOCAL INPATH '"+tempLocation+"' OVERWRITE INTO TABLE "+ dbName+ "." + tableName
    );
    assertEquals(ret.getResponseCode() + ":" + ret.getErrorMessage(), null, ret.getException());

    CommandProcessorResponse selectRet = driver.run("SELECT * from " + dbName + "." + tableName);
    assertEquals(selectRet.getResponseCode() + ":" + selectRet.getErrorMessage(),
        null, selectRet.getException());

    List<String> values = new ArrayList<String>();
    driver.getResults(values);

    assertEquals(2, values.size());
    assertEquals(data[0],values.get(0));
    assertEquals(data[1],values.get(1));

    ExportCommand exportCmd = new ExportCommand(dbName,tableName,null,
        exportLocation, false, evid);

    LOG.info("About to run :" + exportCmd.get().get(0));
    CommandProcessorResponse ret2 = driver.run(exportCmd.get().get(0));
    assertEquals(ret2.getResponseCode() + ":" + ret2.getErrorMessage(), null, ret2.getException());

    List<String> exportPaths = exportCmd.cleanupLocationsAfterEvent();
    assertEquals(1,exportPaths.size());
    String metadata = getMetadataContents(exportPaths.get(0));
    LOG.info("Export returned the following _metadata contents:");
    LOG.info(metadata);
    assertTrue(metadata + "did not match \"repl.scope\"=\"all\"", metadata.matches(".*\"repl.scope\":\"all\".*"));
    assertTrue(metadata + "has \"repl.last.id\"",metadata.matches(".*\"repl.last.id\":.*"));

    ImportCommand importCmd = new ImportCommand(dbName, importedTableName, null, exportLocation, false, evid);

    LOG.info("About to run :" + importCmd.get().get(0));
    CommandProcessorResponse ret3 = driver.run(importCmd.get().get(0));
    assertEquals(ret3.getResponseCode() + ":" + ret3.getErrorMessage(), null, ret3.getException());

    CommandProcessorResponse selectRet2 = driver.run("SELECT * from " + dbName + "." + importedTableName);
    assertEquals(selectRet2.getResponseCode() + ":" + selectRet2.getErrorMessage(),
        null, selectRet2.getException());

    List<String> values2 = new ArrayList<String>();
    driver.getResults(values2);

    assertEquals(2, values2.size());
    assertEquals(data[0],values2.get(0));
    assertEquals(data[1],values2.get(1));

    HCatTable importedTable = client.getTable(dbName,importedTableName);
    assertNotNull(importedTable);

    assertTrue(importedTable.getTblProps().containsKey("repl.last.id"));
  }

  @Test
  public void testMetadataReplEximCommands() throws IOException {
    // repl metadata export, has repl.last.id and repl.scope=metadata
    // import repl metadata dump, table metadata changed, allows override, has repl.last.id
    int evid = 222;
    String exportLocation = TEST_PATH + File.separator + "testMetadataReplExim";
    Path tempPath = new Path(TEST_PATH ,"testMetadataReplEximTmp");
    String tempLocation = tempPath.toUri().getPath();

    String dbName = "exim";
    String tableName = "basicSrc";
    String importedTableName = "basicDst";
    List<HCatFieldSchema> cols = HCatSchemaUtils.getHCatSchema("b:string").getFields();

    client.dropDatabase(dbName, true, HCatClient.DropDBMode.CASCADE);
    client.createDatabase(HCatCreateDBDesc.create(dbName).ifNotExists(false).build());

    HCatTable table = (new HCatTable(dbName, tableName)).cols(cols).fileFormat("textfile");

    client.createTable(HCatCreateTableDesc.create(table).build());
    HCatTable t = client.getTable(dbName, tableName);
    assertNotNull(t);

    String[] data = new String[]{ "eleven" , "twelve" };

    HcatTestUtils.createTestDataFile(tempLocation,data);

    CommandProcessorResponse ret = driver.run(
        "LOAD DATA LOCAL INPATH '"+tempLocation+"' OVERWRITE INTO TABLE "+ dbName+ "." + tableName
    );
    assertEquals(ret.getResponseCode() + ":" + ret.getErrorMessage(), null, ret.getException());

    CommandProcessorResponse selectRet = driver.run("SELECT * from " + dbName + "." + tableName);
    assertEquals(selectRet.getResponseCode() + ":" + selectRet.getErrorMessage(),
        null, selectRet.getException());

    List<String> values = new ArrayList<String>();
    driver.getResults(values);

    assertEquals(2, values.size());
    assertEquals(data[0],values.get(0));
    assertEquals(data[1],values.get(1));

    ExportCommand exportMdCmd = new ExportCommand(dbName,tableName,null,
        exportLocation, true, evid);

    LOG.info("About to run :" + exportMdCmd.get().get(0));
    CommandProcessorResponse ret2 = driver.run(exportMdCmd.get().get(0));
    assertEquals(ret2.getResponseCode() + ":" + ret2.getErrorMessage(), null, ret2.getException());

    List<String> exportPaths = exportMdCmd.cleanupLocationsAfterEvent();
    assertEquals(1,exportPaths.size());
    String metadata = getMetadataContents(exportPaths.get(0));
    LOG.info("Export returned the following _metadata contents:");
    LOG.info(metadata);
    assertTrue(metadata + "did not match \"repl.scope\"=\"metadata\"",metadata.matches(".*\"repl.scope\":\"metadata\".*"));
    assertTrue(metadata + "has \"repl.last.id\"",metadata.matches(".*\"repl.last.id\":.*"));

    ImportCommand importMdCmd = new ImportCommand(dbName, importedTableName, null, exportLocation, true, evid);

    LOG.info("About to run :" + importMdCmd.get().get(0));
    CommandProcessorResponse ret3 = driver.run(importMdCmd.get().get(0));
    assertEquals(ret3.getResponseCode() + ":" + ret3.getErrorMessage(), null, ret3.getException());

    CommandProcessorResponse selectRet2 = driver.run("SELECT * from " + dbName + "." + importedTableName);
    assertEquals(selectRet2.getResponseCode() + ":" + selectRet2.getErrorMessage(),
        null, selectRet2.getException());

    List<String> values2 = new ArrayList<String>();
    driver.getResults(values2);

    assertEquals(0, values2.size());

    HCatTable importedTable = client.getTable(dbName,importedTableName);
    assertNotNull(importedTable);

    assertTrue(importedTable.getTblProps().containsKey("repl.last.id"));
  }


  @Test
  public void testNoopReplEximCommands() throws Exception {
    // repl noop export on non-existant table, has repl.noop, does not error
    // import repl noop dump, no error

    int evid = 333;
    String exportLocation = TEST_PATH + File.separator + "testNoopReplExim";
    String dbName = "doesNotExist" + System.currentTimeMillis();
    String tableName = "nope" + System.currentTimeMillis();

    ExportCommand noopExportCmd = new ExportCommand(dbName,tableName,null,
        exportLocation, false, evid);

    LOG.info("About to run :" + noopExportCmd.get().get(0));
    CommandProcessorResponse ret = driver.run(noopExportCmd.get().get(0));
    assertEquals(ret.getResponseCode() + ":" + ret.getErrorMessage(), null, ret.getException());

    List<String> exportPaths = noopExportCmd.cleanupLocationsAfterEvent();
    assertEquals(1,exportPaths.size());
    String metadata = getMetadataContents(exportPaths.get(0));
    LOG.info("Export returned the following _metadata contents:");
    LOG.info(metadata);
    assertTrue(metadata + "did not match \"repl.noop\"=\"true\"",metadata.matches(".*\"repl.noop\":\"true\".*"));

    ImportCommand noopImportCmd = new ImportCommand(dbName, tableName, null, exportLocation, false, evid);

    LOG.info("About to run :" + noopImportCmd.get().get(0));
    CommandProcessorResponse ret2 = driver.run(noopImportCmd.get().get(0));
    assertEquals(ret2.getResponseCode() + ":" + ret2.getErrorMessage(), null, ret2.getException());

    Exception onfe = null;
    try {
      HCatDatabase d_doesNotExist = client.getDatabase(dbName);
    } catch (Exception e) {
      onfe = e;
    }

    assertNotNull(onfe);
    assertTrue(onfe instanceof ObjectNotFoundException);
  }

  private static String getMetadataContents(String exportPath) throws IOException {
    Path mdFilePath = new Path(exportPath,"_metadata");

    FileSystem fs = FileSystem.get(mdFilePath.toUri(), hconf);
    assertTrue(mdFilePath.toUri().toString() + "does not exist",fs.exists(mdFilePath));

    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(mdFilePath)));
    StringBuilder sb = new StringBuilder();
    String line;
    while ((line = reader.readLine()) != null) {
      sb.append(line);
    }
    reader.close();
    return sb.toString();
  }

}
