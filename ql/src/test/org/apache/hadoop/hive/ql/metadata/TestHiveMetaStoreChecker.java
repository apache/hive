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
package org.apache.hadoop.hive.ql.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.CheckResult;
import org.apache.hadoop.hive.metastore.HiveMetaStoreChecker;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.MetastoreException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * TestHiveMetaStoreChecker.
 *
 */
public class TestHiveMetaStoreChecker {

  private Hive hive;
  private IMetaStoreClient msc;
  private FileSystem fs;
  private HiveMetaStoreChecker checker = null;

  private final String catName = "hive";
  private final String dbName = "testhivemetastorechecker_db";
  private final String tableName = "testhivemetastorechecker_table";

  private final String partDateName = "partdate";
  private final String partCityName = "partcity";

  private List<FieldSchema> partCols;
  private List<Map<String, String>> parts;

  @Before
  public void setUp() throws Exception {
    hive = Hive.get();
    HiveConf conf = new HiveConfForTest(hive.getConf(), getClass());
    conf.set(MetastoreConf.ConfVars.FS_HANDLER_THREADS_COUNT.getVarname(), "15");
    conf.set(MetastoreConf.ConfVars.MSCK_PATH_VALIDATION.getVarname(), "throw");
    msc = new HiveMetaStoreClient(conf);
    checker = new HiveMetaStoreChecker(msc, conf);

    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    SessionState ss = SessionState.start(conf);
    ss.initTxnMgr(conf);

    partCols = new ArrayList<>();
    partCols.add(new FieldSchema(partDateName, serdeConstants.STRING_TYPE_NAME, ""));
    partCols.add(new FieldSchema(partCityName, serdeConstants.STRING_TYPE_NAME, ""));

    parts = new ArrayList<>();
    Map<String, String> part1 = new HashMap<>();
    part1.put(partDateName, "2008-01-01");
    part1.put(partCityName, "london");
    parts.add(part1);
    Map<String, String> part2 = new HashMap<>();
    part2.put(partDateName, "2008-01-02");
    part2.put(partCityName, "stockholm");
    parts.add(part2);

    //cleanup just in case something is left over from previous run
    dropDbTable();
  }

  private void dropDbTable()  {
    // cleanup
    try {
      msc.dropTable(catName, dbName, tableName, true, true);
      msc.dropDatabase(catName, dbName, true, true, true);
    } catch (TException e) {
      // ignore
    }
  }

  @After
  public void tearDown() throws Exception {
    dropDbTable();
    Hive.closeCurrent();
  }

  @Test
  public void testTableCheck() throws HiveException, IOException, TException, MetastoreException,MetaException {
    CheckResult result = checker.checkMetastore(catName, dbName, null, null, null);
    // we haven't added anything so should return an all ok
    assertEquals(Collections.<String>emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String>emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<CheckResult.PartitionResult>emptySet(), result.getPartitionsNotOnFs());
    assertEquals(Collections.<CheckResult.PartitionResult>emptySet(), result.getPartitionsNotInMs());

    // check table only, should not exist in ms
    result = checker.checkMetastore(catName, dbName, tableName, null, null);
    assertEquals(1, result.getTablesNotInMs().size());
    assertEquals(tableName, result.getTablesNotInMs().iterator().next());
    assertEquals(Collections.<String>emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<CheckResult.PartitionResult>emptySet(), result.getPartitionsNotOnFs());
    assertEquals(Collections.<CheckResult.PartitionResult>emptySet(), result.getPartitionsNotInMs());

    Database db = new Database();
    db.setCatalogName(catName);
    db.setName(dbName);
    msc.createDatabase(db);

    Table table = new Table(dbName, tableName);
    table.setDbName(dbName);
    table.setInputFormatClass(TextInputFormat.class);
    table.setOutputFormatClass(HiveIgnoreKeyTextOutputFormat.class);

    hive.createTable(table);
    Assert.assertTrue(table.getTTable().isSetId());
    table.getTTable().unsetId();
    // now we've got a table, check that it works
    // first check all (1) tables
    result = checker.checkMetastore(catName, dbName, null, null, null);
    assertEquals(Collections.<String>emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String>emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<CheckResult.PartitionResult>emptySet(), result.getPartitionsNotOnFs());
    assertEquals(Collections.<CheckResult.PartitionResult>emptySet(), result.getPartitionsNotInMs());

    // then let's check the one we know about
    result = checker.checkMetastore(catName, dbName, tableName, null, null);
    assertEquals(Collections.<String>emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String>emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<CheckResult.PartitionResult>emptySet(), result.getPartitionsNotOnFs());
    assertEquals(Collections.<CheckResult.PartitionResult>emptySet(), result.getPartitionsNotInMs());

    // remove the table folder
    fs = table.getPath().getFileSystem(hive.getConf());
    fs.delete(table.getPath(), true);

    // now this shouldn't find the path on the fs
    result = checker.checkMetastore(catName, dbName, tableName, null, null);
    assertEquals(Collections.<String>emptySet(), result.getTablesNotInMs());
    assertEquals(1, result.getTablesNotOnFs().size());
    assertEquals(tableName, result.getTablesNotOnFs().iterator().next());
    assertEquals(Collections.<CheckResult.PartitionResult>emptySet(), result.getPartitionsNotOnFs());
    assertEquals(Collections.<CheckResult.PartitionResult>emptySet(), result.getPartitionsNotInMs());

    // put it back and one additional table
    fs.mkdirs(table.getPath());
    Path fakeTable = table.getPath().getParent().suffix(
        Path.SEPARATOR + "faketable");
    fs.mkdirs(fakeTable);
    fs.deleteOnExit(fakeTable);

    // find the extra table
    result = checker.checkMetastore(catName, dbName, null, null, null);
    assertEquals(1, result.getTablesNotInMs().size());
    assertEquals(fakeTable.getName(), Lists.newArrayList(result.getTablesNotInMs()).get(0));
    assertEquals(Collections.<String>emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<CheckResult.PartitionResult>emptySet(), result.getPartitionsNotOnFs());
    assertEquals(Collections.<CheckResult.PartitionResult>emptySet(), result.getPartitionsNotInMs());

    // create a new external table
    hive.dropTable(dbName, tableName);
    table.setProperty("EXTERNAL", "TRUE");
    hive.createTable(table);

    // should return all ok
    result = checker.checkMetastore(catName, dbName, null, null, null);
    assertEquals(Collections.<String>emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String>emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<CheckResult.PartitionResult>emptySet(), result.getPartitionsNotOnFs());
    assertEquals(Collections.<CheckResult.PartitionResult>emptySet(), result.getPartitionsNotInMs());
  }

  /*
   * Tests the case when tblPath/p1=a/p2=b/p3=c/file for a table with partition (p1, p2)
   * does not throw HiveException
   */
  @Test
  public void testAdditionalPartitionDirs()
    throws HiveException, AlreadyExistsException, IOException, MetastoreException {
    Table table = createTestTable(false);
    List<Partition> partitions = hive.getPartitions(table);
    assertEquals(2, partitions.size());
    // add a fake partition dir on fs
    fs = partitions.get(0).getDataLocation().getFileSystem(hive.getConf());
    addFolderToPath(fs, table.getDataLocation().toString(),
        partDateName + "=2017-01-01/" + partCityName + "=paloalto/fakePartCol=fakepartValue");
    CheckResult result = checker.checkMetastore(catName, dbName, tableName, null, null);
    assertEquals(Collections.<String> emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String> emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<CheckResult.PartitionResult> emptySet(), result.getPartitionsNotOnFs());
    //fakePart path partition is added since the defined partition keys are valid
    assertEquals(1, result.getPartitionsNotInMs().size());
  }

  @Test(expected = MetastoreException.class)
  public void testInvalidPartitionKeyName()
    throws HiveException, AlreadyExistsException, IOException, MetastoreException {
    Table table = createTestTable(false);
    List<Partition> partitions = hive.getPartitions(table);
    assertEquals(2, partitions.size());
    // add a fake partition dir on fs
    fs = partitions.get(0).getDataLocation().getFileSystem(hive.getConf());
    addFolderToPath(fs, table.getDataLocation().toString(),"fakedate=2009-01-01/fakecity=sanjose");
    checker.checkMetastore(catName, dbName, tableName, null, null);
  }

  /*
   * skip mode should not throw exception when a invalid partition directory
   * is found. It should just ignore it
   */
  @Test
  public void testSkipInvalidPartitionKeyName()
    throws HiveException, AlreadyExistsException, IOException, MetastoreException {
    hive.getConf().set(MetastoreConf.ConfVars.MSCK_PATH_VALIDATION.getVarname(), "skip");
    checker = new HiveMetaStoreChecker(msc, hive.getConf());
    Table table = createTestTable(false);
    List<Partition> partitions = hive.getPartitions(table);
    assertEquals(2, partitions.size());
    // add a fake partition dir on fs
    fs = partitions.get(0).getDataLocation().getFileSystem(hive.getConf());
    addFolderToPath(fs, table.getDataLocation().toString(),"fakedate=2009-01-01/fakecity=sanjose");
    createPartitionsDirectoriesOnFS(table, 2);
    CheckResult result = checker.checkMetastore(catName, dbName, tableName, null, null);
    assertEquals(Collections.<String> emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String> emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<CheckResult.PartitionResult> emptySet(), result.getPartitionsNotOnFs());
    // only 2 valid partitions should be added
    assertEquals(2, result.getPartitionsNotInMs().size());
  }

  /*
   * Tests the case when we have normal delta_dirs in the partition folder
   * does not throw HiveException
   */
  @Test
  public void testAddPartitionNormalDeltas() throws Exception {
    Table table = createTestTable(true);
    List<Partition> partitions = hive.getPartitions(table);
    assertEquals(2, partitions.size());
    // add a partition dir on fs
    fs = partitions.get(0).getDataLocation().getFileSystem(hive.getConf());
    Path newPart = addFolderToPath(fs, table.getDataLocation().toString(),
        partDateName + "=2017-01-01/" + partCityName + "=paloalto");

    // Add a few deltas
    addFolderToPath(fs, newPart.toString(), "delta_0000001_0000001_0000");
    addFolderToPath(fs, newPart.toString(), "delta_0000010_0000010_0000");
    addFolderToPath(fs, newPart.toString(), "delta_0000101_0000101_0000");
    CheckResult result = checker.checkMetastore(catName, dbName, tableName, null, null);
    assertEquals(Collections.<CheckResult.PartitionResult> emptySet(), result.getPartitionsNotOnFs());
    assertEquals(1, result.getPartitionsNotInMs().size());
    // Found the highest writeId
    assertEquals(101, result.getPartitionsNotInMs().iterator().next().getMaxWriteId());
    assertEquals(0, result.getPartitionsNotInMs().iterator().next().getMaxTxnId());
  }
  /*
   * Tests the case when we have normal delta_dirs in the partition folder
   * does not throw HiveException
   */
  @Test
  public void testAddPartitionCompactedDeltas() throws Exception {
    Table table = createTestTable(true);
    List<Partition> partitions = hive.getPartitions(table);
    assertEquals(2, partitions.size());
    // add a partition dir on fs
    fs = partitions.get(0).getDataLocation().getFileSystem(hive.getConf());
    Path newPart = addFolderToPath(fs, table.getDataLocation().toString(),
        partDateName + "=2017-01-01/" + partCityName + "=paloalto");

    // Add a few deltas
    addFolderToPath(fs, newPart.toString(), "delta_0000001_0000001_0000");
    addFolderToPath(fs, newPart.toString(), "delta_0000010_0000015_v0000067");
    addFolderToPath(fs, newPart.toString(), "delta_0000101_0000120_v0000087");
    CheckResult result = checker.checkMetastore(catName, dbName, tableName, null, null);
    assertEquals(Collections.<CheckResult.PartitionResult> emptySet(), result.getPartitionsNotOnFs());
    assertEquals(1, result.getPartitionsNotInMs().size());
    // Found the highest writeId
    assertEquals(120, result.getPartitionsNotInMs().iterator().next().getMaxWriteId());
    assertEquals(87, result.getPartitionsNotInMs().iterator().next().getMaxTxnId());
  }
  @Test
  public void testAddPartitionCompactedBase() throws Exception {
    Table table = createTestTable(true);
    List<Partition> partitions = hive.getPartitions(table);
    assertEquals(2, partitions.size());
    // add a partition dir on fs
    fs = partitions.get(0).getDataLocation().getFileSystem(hive.getConf());
    Path newPart = addFolderToPath(fs, table.getDataLocation().toString(),
        partDateName + "=2017-01-01/" + partCityName + "=paloalto");

    // Add a few deltas
    addFolderToPath(fs, newPart.toString(), "delta_0000001_0000001_0000");
    addFolderToPath(fs, newPart.toString(), "delta_0000002_0000002_0000");
    addFolderToPath(fs, newPart.toString(), "delta_0000003_0000003_0000");
    addFolderToPath(fs, newPart.toString(), "base_0000003_v0000200");
    CheckResult result = checker.checkMetastore(catName, dbName, tableName, null, null);
    assertEquals(Collections.<CheckResult.PartitionResult> emptySet(), result.getPartitionsNotOnFs());
    assertEquals(1, result.getPartitionsNotInMs().size());
    // Found the highest writeId
    assertEquals(3, result.getPartitionsNotInMs().iterator().next().getMaxWriteId());
    assertEquals(200, result.getPartitionsNotInMs().iterator().next().getMaxTxnId());
  }

  @Test
  public void testAddPartitionMMBase() throws Exception {
    Table table = createTestTable(true);
    List<Partition> partitions = hive.getPartitions(table);
    assertEquals(2, partitions.size());
    // add a partition dir on fs
    fs = partitions.get(0).getDataLocation().getFileSystem(hive.getConf());
    Path newPart = addFolderToPath(fs, table.getDataLocation().toString(),
        partDateName + "=2017-01-01/" + partCityName + "=paloalto");

    // Add a few deltas
    addFolderToPath(fs, newPart.toString(), "delta_0000001_0000001_0000");
    addFolderToPath(fs, newPart.toString(), "delta_0000002_0000002_0000");
    addFolderToPath(fs, newPart.toString(), "delta_0000003_0000003_0000");
    addFolderToPath(fs, newPart.toString(), "base_0000004");
    CheckResult result = checker.checkMetastore(catName, dbName, tableName, null, null);
    assertEquals(Collections.<CheckResult.PartitionResult> emptySet(), result.getPartitionsNotOnFs());
    assertEquals(1, result.getPartitionsNotInMs().size());
    // Found the highest writeId
    assertEquals(4, result.getPartitionsNotInMs().iterator().next().getMaxWriteId());
    assertEquals(0, result.getPartitionsNotInMs().iterator().next().getMaxTxnId());
  }

  @Test
  public void testNoNPartitionedTable() throws Exception {
    Table table = createNonPartitionedTable();
    // add a partition dir on fs
    fs = table.getDataLocation().getFileSystem(hive.getConf());

    Path tablePath = table.getDataLocation();

    // Add a few deltas
    addFolderToPath(fs, tablePath.toString(), "delta_0000001_0000001_0000");
    addFolderToPath(fs, tablePath.toString(), "delta_0000002_0000002_0000");
    addFolderToPath(fs, tablePath.toString(), "delta_0000003_0000003_0000");
    addFolderToPath(fs, tablePath.toString(), "base_0000003_v0000200");
    CheckResult result = checker.checkMetastore(catName, dbName, tableName, null, null);
    assertEquals(Collections.<CheckResult.PartitionResult> emptySet(), result.getPartitionsNotOnFs());
    assertEquals(Collections.<CheckResult.PartitionResult> emptySet(), result.getPartitionsNotInMs());
    // Found the highest writeId
    assertEquals(3, result.getMaxWriteId());
    assertEquals(200, result.getMaxTxnId());
  }

  @Test
  public void testPartitionsCheck() throws HiveException,
    IOException, TException, MetastoreException {
    Table table = createTestTable(false);

    CheckResult result = checker.checkMetastore(catName, dbName, tableName, null, null);
    // all is well
    assertEquals(Collections.<String>emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String>emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<CheckResult.PartitionResult>emptySet(), result.getPartitionsNotOnFs());
    assertEquals(Collections.<CheckResult.PartitionResult>emptySet(), result.getPartitionsNotInMs());

    List<Partition> partitions = hive.getPartitions(table);
    assertEquals(2, partitions.size());

    Partition partToRemove = partitions.get(0);
    // As this partition (partdate=2008-01-01/partcity=london) is the only
    // partition under (partdate=2008-01-01)
    // we also need to delete partdate=2008-01-01 to make it consistent.
    Path partToRemovePath = partToRemove.getDataLocation().getParent();
    fs = partToRemovePath.getFileSystem(hive.getConf());
    fs.delete(partToRemovePath, true);

    result = checker.checkMetastore(catName, dbName, tableName, null, null);
    // missing one partition on fs
    assertEquals(Collections.<String>emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String>emptySet(), result.getTablesNotOnFs());
    assertEquals(1, result.getPartitionsNotOnFs().size());
    assertEquals(partToRemove.getName(), result.getPartitionsNotOnFs().iterator().next()
        .getPartitionName());
    assertEquals(partToRemove.getTable().getTableName(),
        result.getPartitionsNotOnFs().iterator().next().getTableName());
    assertEquals(Collections.<CheckResult.PartitionResult>emptySet(), result.getPartitionsNotInMs());
    // old test is moved to msck_repair_2.q

    // cleanup
    hive.dropTable(dbName, tableName, true, true);
    hive.createTable(table);
    result = checker.checkMetastore(catName, dbName, null, null, null);
    assertEquals(Collections.<String>emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String>emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<CheckResult.PartitionResult>emptySet(), result.getPartitionsNotOnFs());
    assertEquals(Collections.<CheckResult.PartitionResult>emptySet(), result.getPartitionsNotInMs()); //--0e
    System.err.println("Test completed - partition check");
  }

  @Test
  public void testDataDeletion() throws HiveException,
    IOException, TException {

    Database db = new Database();
    db.setName(dbName);
    hive.createDatabase(db);

    Table table = new Table(dbName, tableName);
    table.setDbName(dbName);
    table.setInputFormatClass(TextInputFormat.class);
    table.setOutputFormatClass(HiveIgnoreKeyTextOutputFormat.class);
    table.setPartCols(partCols);

    hive.createTable(table);
    table = hive.getTable(dbName, tableName);

    Path fakeTable = table.getPath().getParent().suffix(
        Path.SEPARATOR + "faketable");
    fs = fakeTable.getFileSystem(hive.getConf());
    fs.mkdirs(fakeTable);
    fs.deleteOnExit(fakeTable);

    Path fakePart = new Path(table.getDataLocation().toString(),
        "fakepartition=fakevalue");
    fs.mkdirs(fakePart);
    fs.deleteOnExit(fakePart);

    hive.dropTable(dbName, tableName, true, true);
    assertFalse(fs.exists(fakePart));
    hive.dropDatabase(dbName);
    assertFalse(fs.exists(fakeTable));
  }

  /**
   * Test multi-threaded implementation of checker to find out missing partitions.
   * @throws Exception ex
   */
  @Test
  public void testPartitionsNotInMs() throws Exception {
    Table testTable = createPartitionedTestTable(dbName, tableName, 2, 0);
    // add 10 partitions on the filesystem
    createPartitionsDirectoriesOnFS(testTable, 10);
    CheckResult result = checker.checkMetastore(catName, dbName, tableName, null, null);
    assertEquals(Collections.<String>emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String>emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<CheckResult.PartitionResult>emptySet(), result.getPartitionsNotOnFs());
    assertEquals(10, result.getPartitionsNotInMs().size());
  }

  /**
   * Tests single threaded implementation of checkMetastore.
   * @throws Exception ex
   */
  @Test
  public void testSingleThreadedCheckMetastore() throws Exception {
    // set num of threads to 0 so that single-threaded checkMetastore is called
    hive.getConf().set(MetastoreConf.ConfVars.FS_HANDLER_THREADS_COUNT.getVarname(), "0");
    Table testTable = createPartitionedTestTable(dbName, tableName, 2, 0);
    // add 10 partitions on the filesystem
    createPartitionsDirectoriesOnFS(testTable, 10);
    CheckResult result = checker.checkMetastore(catName, dbName, tableName, null, null);
    assertEquals(Collections.<String> emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String> emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<CheckResult.PartitionResult> emptySet(), result.getPartitionsNotOnFs());
    assertEquals(10, result.getPartitionsNotInMs().size());
  }

  /**
   * Tests single threaded implementation for deeply nested partitioned tables
   *
   * @throws Exception ex
   */
  @Test
  public void testSingleThreadedDeeplyNestedTables() throws Exception {
    // set num of threads to 0 so that single-threaded checkMetastore is called
    hive.getConf().set(MetastoreConf.ConfVars.FS_HANDLER_THREADS_COUNT.getVarname(), "0");
    int poolSize = 2;
    // create a deeply nested table which has more partition keys than the pool size
    Table testTable = createPartitionedTestTable(dbName, tableName, poolSize + 2, 0);
    // add 10 partitions on the filesystem
    createPartitionsDirectoriesOnFS(testTable, 10);
    CheckResult result = checker.checkMetastore(catName, dbName, tableName, null, null);
    assertEquals(Collections.<String> emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String> emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<CheckResult.PartitionResult> emptySet(), result.getPartitionsNotOnFs());
    assertEquals(10, result.getPartitionsNotInMs().size());
  }

  /**
   * Tests the case when the number of partition keys are more than the threadpool size.
   *
   * @throws Exception ex
   */
  @Test
  public void testDeeplyNestedPartitionedTables() throws Exception {
    hive.getConf().set(MetastoreConf.ConfVars.FS_HANDLER_THREADS_COUNT.getVarname(), "2");
    int poolSize = 2;
    // create a deeply nested table which has more partition keys than the pool size
    Table testTable = createPartitionedTestTable(dbName, tableName, poolSize + 2, 0);
    // add 10 partitions on the filesystem
    createPartitionsDirectoriesOnFS(testTable, 10);
    CheckResult result = checker.checkMetastore(catName, dbName, tableName, null, null);
    assertEquals(Collections.<String> emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String> emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<CheckResult.PartitionResult> emptySet(), result.getPartitionsNotOnFs());
    assertEquals(10, result.getPartitionsNotInMs().size());
  }

  /**
   * Test if checker throws HiveException when the there is a dummy directory present in the nested level
   * of sub-directories
   * @throws Exception ex
   */
  @Test
  public void testErrorForMissingPartitionColumn() throws Exception {
    Table testTable = createPartitionedTestTable(dbName, tableName, 2, 0);
    // add 10 partitions on the filesystem
    createPartitionsDirectoriesOnFS(testTable, 10);
    //create a fake directory to throw exception
    StringBuilder sb = new StringBuilder(testTable.getDataLocation().toString());
    sb.append(Path.SEPARATOR);
    sb.append("dummyPart=error");
    createDirectory(sb.toString());
    //check result now
    Exception exception = null;
    try {
      checker.checkMetastore(catName, dbName, tableName, null, null);
    } catch (Exception e) {
      exception = e;
    }
    assertTrue("Expected MetastoreException", exception instanceof MetastoreException);
    createFile(sb.toString(), "dummyFile");
    exception = null;
    try {
      checker.checkMetastore(catName, dbName, tableName, null, null);
    } catch (Exception e) {
      exception = e;
    }
    assertTrue("Expected MetastoreException", exception instanceof MetastoreException);
  }

  /**
   * Tests if there exists a unknown partition directory on the FS with in-valid order of partition
   * keys than what is specified in table specification.
   *
   * @throws Exception ex
   */
  @Test(expected = MetastoreException.class)
  public void testInvalidOrderForPartitionKeysOnFS() throws Exception {
    Table testTable = createPartitionedTestTable(dbName, tableName, 2, 0);
    // add 10 partitions on the filesystem
    createInvalidPartitionDirsOnFS(testTable, 10);
    checker.checkMetastore(catName, dbName, tableName, null, null);
  }

  /**
   * In skip mode msck should ignore invalid partitions instead of throwing exception.
   * @throws Exception ex
   */
  @Test
  public void testSkipInvalidOrderForPartitionKeysOnFS() throws Exception{
    hive.getConf().set(MetastoreConf.ConfVars.MSCK_PATH_VALIDATION.getVarname(), "skip");
    checker = new HiveMetaStoreChecker(msc, hive.getConf());
    Table testTable = createPartitionedTestTable(dbName, tableName, 2, 0);
    // add 10 partitions on the filesystem
    createInvalidPartitionDirsOnFS(testTable, 2);
    // add 10 partitions on the filesystem
    createPartitionsDirectoriesOnFS(testTable, 2);
    CheckResult result = checker.checkMetastore(catName, dbName, tableName, null, null);
    assertEquals(Collections.<String> emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String> emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<CheckResult.PartitionResult> emptySet(), result.getPartitionsNotOnFs());
    // only 2 valid partitions should be added
    assertEquals(2, result.getPartitionsNotInMs().size());
  }

  /**
   * Test if single-threaded implementation checker throws HiveException when the there is a dummy
   * directory present in the nested level.
   * @throws Exception ex
   */
  @Test
  public void testErrorForMissingPartitionsSingleThreaded() throws Exception {
    // set num of threads to 0 so that single-threaded checkMetastore is called
    hive.getConf().set(MetastoreConf.ConfVars.FS_HANDLER_THREADS_COUNT.getVarname(), "0");
    Table testTable = createPartitionedTestTable(dbName, tableName, 2, 0);
    // add 10 partitions on the filesystem
    createPartitionsDirectoriesOnFS(testTable, 10);
    // create a fake directory to throw exception
    StringBuilder sb = new StringBuilder(testTable.getDataLocation().toString());
    sb.append(Path.SEPARATOR);
    sb.append("dummyPart=error");
    createDirectory(sb.toString());
    // check result now
    Exception exception = null;
    try {
      checker.checkMetastore(catName, dbName, tableName, null, null);
    } catch (Exception e) {
      exception = e;
    }
    assertTrue("Expected MetastoreException", exception instanceof MetastoreException);
    createFile(sb.toString(), "dummyFile");
    exception = null;
    try {
      checker.checkMetastore(catName, dbName, tableName, null, null);
    } catch (Exception e) {
      exception = e;
    }
    assertTrue("Expected MetastoreException", exception instanceof MetastoreException);
  }
  /**
   * Creates a test partitioned table with the required level of nested partitions and number of
   * partitions
   *
   * @param dbName - Database name
   * @param tableName - Table name
   * @param numOfPartKeys - Number of partition keys (nested levels of sub-directories in base table
   *          path)
   * @param valuesPerPartition - If greater than 0 creates valuesPerPartition dummy partitions
   * @return The new table
   * @throws Exception ex
   */
  private Table createPartitionedTestTable(String dbName, String tableName, int numOfPartKeys, int valuesPerPartition)
      throws Exception {
    Database db = new Database();
    db.setName(dbName);
    hive.createDatabase(db, true);

    Table table = new Table(dbName, tableName);
    table.setDbName(dbName);
    table.setInputFormatClass(TextInputFormat.class);
    table.setOutputFormatClass(HiveIgnoreKeyTextOutputFormat.class);
    // create partition key schema
    ArrayList<FieldSchema> partKeys = new ArrayList<>();
    for (int i = 1; i <= numOfPartKeys; i++) {
      String partName = "part" + i;
      partKeys.add(new FieldSchema(partName, serdeConstants.STRING_TYPE_NAME, ""));
    }
    table.setPartCols(partKeys);
    // create table
    hive.createTable(table, true);
    table = hive.getTable(dbName, tableName);
    if (valuesPerPartition == 0) {
      return table;
    }
    // create partition specs
    ArrayList<Map<String, String>> partitionSpecs = new ArrayList<>();
    for (int partKeyIndex = 0; partKeyIndex < numOfPartKeys; partKeyIndex++) {
      String partName = partKeys.get(partKeyIndex).getName();
      Map<String, String> partMap = new HashMap<>();
      for (int val = 1; val <= valuesPerPartition; val++) {
        partMap.put(partName, String.valueOf(val));
      }
      partitionSpecs.add(partMap);
    }

    // create partitions
    for (Map<String, String> partSpec : partitionSpecs) {
      hive.createPartition(table, partSpec);
    }

    List<Partition> partitions = hive.getPartitions(table);
    assertEquals(numOfPartKeys * valuesPerPartition, partitions.size());
    return table;
  }

  /**
   * Creates partition sub-directories for a given table on the file system. Used to test the
   * use-cases when partitions for the table are not present in the metastore db
   *
   * @param table - Table which provides the base locations and partition specs for creating the
   *          sub-directories
   * @param numPartitions - Number of partitions to be created
   * @param reverseOrder - If set to true creates the partition sub-directories in the reverse order
   *          of specified by partition keys defined for the table
   * @throws IOException ex
   */
  private void createPartitionsDirectoriesOnFS(Table table, int numPartitions, boolean reverseOrder) throws IOException {
    String path = table.getDataLocation().toString();
    fs = table.getPath().getFileSystem(hive.getConf());
    int numPartKeys = table.getPartitionKeys().size();
    for (int i = 0; i < numPartitions; i++) {
      StringBuilder partPath = new StringBuilder(path);
      partPath.append(Path.SEPARATOR);
      if (!reverseOrder) {
        for (int j = 0; j < numPartKeys; j++) {
          FieldSchema field = table.getPartitionKeys().get(j);
          partPath.append(field.getName());
          partPath.append('=');
          partPath.append("val_");
          partPath.append(i);
          if (j < (numPartKeys - 1)) {
            partPath.append(Path.SEPARATOR);
          }
        }
      } else {
        for (int j = numPartKeys - 1; j >= 0; j--) {
          FieldSchema field = table.getPartitionKeys().get(j);
          partPath.append(field.getName());
          partPath.append('=');
          partPath.append("val_");
          partPath.append(i);
          if (j > 0) {
            partPath.append(Path.SEPARATOR);
          }
        }
      }
      createDirectory(partPath.toString());
    }
  }

  private void createPartitionsDirectoriesOnFS(Table table, int numPartitions) throws IOException {
    createPartitionsDirectoriesOnFS(table, numPartitions, false);
  }
  /**
   * Creates a partition directory structure on file system but with a reverse order
   * of sub-directories compared to the partition keys defined in the table. Eg. if the
   * partition keys defined in table are (a int, b int, c int) this method will create
   * an invalid directory c=val_1/b=val_1/a=val_1
   * @param table table
   * @param numPartitions Number of partitions to create
   * @throws IOException
   */
  private void createInvalidPartitionDirsOnFS(Table table, int numPartitions) throws IOException {
    createPartitionsDirectoriesOnFS(table, numPartitions, true);
  }

  private void createFile(String partPath, String filename) throws IOException {
    Path part = new Path(partPath);
    fs.mkdirs(part);
    fs.createNewFile(new Path(partPath + Path.SEPARATOR + filename));
    fs.deleteOnExit(part);
  }

  private void createDirectory(String partPath) throws IOException {
    Path part = new Path(partPath);
    fs.mkdirs(part);
    // create files under partitions to simulate real partitions
    fs.createNewFile(new Path(partPath + Path.SEPARATOR + "dummydata1"));
    fs.createNewFile(new Path(partPath + Path.SEPARATOR + "dummydata2"));
    fs.deleteOnExit(part);
  }

  private Path addFolderToPath(FileSystem fs, String rootPath, String folder) throws IOException{
    Path folderParth = new Path(rootPath, folder);
    fs.mkdirs(folderParth);
    fs.deleteOnExit(folderParth);
    return folderParth;
  }

  private Table createTestTable(boolean transactional) throws HiveException, AlreadyExistsException {
    Database db = new Database();
    db.setName(dbName);
    hive.createDatabase(db, true);

    Table table = new Table(dbName, tableName);
    table.setDbName(dbName);
    if (transactional) {
      table.setInputFormatClass(OrcInputFormat.class);
      table.setOutputFormatClass(OrcOutputFormat.class);
    } else {
      table.setInputFormatClass(TextInputFormat.class);
      table.setOutputFormatClass(HiveIgnoreKeyTextOutputFormat.class);
    }
    table.setPartCols(partCols);
    if (transactional) {
      table.setProperty("transactional", "true");
    }

    hive.createTable(table);
    table = hive.getTable(dbName, tableName);
    Assert.assertTrue(table.getTTable().isSetId());
    table.getTTable().unsetId();

    for (Map<String, String> partSpec : parts) {
      hive.createPartition(table, partSpec);
    }
    return table;
  }
  private Table createNonPartitionedTable() throws Exception {
    Database db = new Database();
    db.setName(dbName);
    hive.createDatabase(db, true);

    Table table = new Table(dbName, tableName);
    table.setDbName(dbName);
    table.setInputFormatClass(OrcInputFormat.class);
    table.setOutputFormatClass(OrcOutputFormat.class);
    table.setProperty("transactional", "true");

    hive.createTable(table);
    table = hive.getTable(dbName, tableName);
    Assert.assertTrue(table.getTTable().isSetId());
    table.getTTable().unsetId();
    return table;
  }
}
