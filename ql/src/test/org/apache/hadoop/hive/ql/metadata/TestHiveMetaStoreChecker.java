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

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import org.mockito.Mockito;

/**
 * TestHiveMetaStoreChecker.
 *
 */
public class TestHiveMetaStoreChecker {

  private Hive hive;
  private FileSystem fs;
  private HiveMetaStoreChecker checker = null;

  private final String dbName = "testhivemetastorechecker_db";
  private final String tableName = "testhivemetastorechecker_table";

  private final String partDateName = "partdate";
  private final String partCityName = "partcity";

  private List<FieldSchema> partCols;
  private List<Map<String, String>> parts;

  @Before
  public void setUp() throws Exception {
    hive = Hive.get();
    hive.getConf().setIntVar(HiveConf.ConfVars.METASTORE_FS_HANDLER_THREADS_COUNT, 15);
    hive.getConf().set(HiveConf.ConfVars.HIVE_MSCK_PATH_VALIDATION.varname, "throw");
    checker = new HiveMetaStoreChecker(hive);

    partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema(partDateName, serdeConstants.STRING_TYPE_NAME, ""));
    partCols.add(new FieldSchema(partCityName, serdeConstants.STRING_TYPE_NAME, ""));

    parts = new ArrayList<Map<String, String>>();
    Map<String, String> part1 = new HashMap<String, String>();
    part1.put(partDateName, "2008-01-01");
    part1.put(partCityName, "london");
    parts.add(part1);
    Map<String, String> part2 = new HashMap<String, String>();
    part2.put(partDateName, "2008-01-02");
    part2.put(partCityName, "stockholm");
    parts.add(part2);

    //cleanup just in case something is left over from previous run
    dropDbTable();
  }

  private void dropDbTable()  {
    // cleanup
    try {
      hive.dropTable(dbName, tableName, true, true);
      hive.dropDatabase(dbName, true, true, true);
    } catch (NoSuchObjectException e) {
      // ignore
    } catch (HiveException e) {
      // ignore
    }
  }

  @After
  public void tearDown() throws Exception {
    dropDbTable();
    Hive.closeCurrent();
  }

  @Test
  public void testTableCheck() throws HiveException, MetaException,
      IOException, TException, AlreadyExistsException {
    CheckResult result = new CheckResult();
    checker.checkMetastore(dbName, null, null, result);
    // we haven't added anything so should return an all ok
    assertEquals(Collections.<String>emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String>emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<String>emptySet(), result.getPartitionsNotOnFs());
    assertEquals(Collections.<String>emptySet(), result.getPartitionsNotInMs());

    // check table only, should not exist in ms
    result = new CheckResult();
    checker.checkMetastore(dbName, tableName, null, result);
    assertEquals(1, result.getTablesNotInMs().size());
    assertEquals(tableName, result.getTablesNotInMs().iterator().next());
    assertEquals(Collections.<String>emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<String>emptySet(), result.getPartitionsNotOnFs());
    assertEquals(Collections.<String>emptySet(), result.getPartitionsNotInMs());

    Database db = new Database();
    db.setName(dbName);
    hive.createDatabase(db);

    Table table = new Table(dbName, tableName);
    table.setDbName(dbName);
    table.setInputFormatClass(TextInputFormat.class);
    table.setOutputFormatClass(HiveIgnoreKeyTextOutputFormat.class);

    hive.createTable(table);
    // now we've got a table, check that it works
    // first check all (1) tables
    result = new CheckResult();
    checker.checkMetastore(dbName, null, null, result);
    assertEquals(Collections.<String>emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String>emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<String>emptySet(), result.getPartitionsNotOnFs());
    assertEquals(Collections.<String>emptySet(), result.getPartitionsNotInMs());

    // then let's check the one we know about
    result = new CheckResult();
    checker.checkMetastore(dbName, tableName, null, result);
    assertEquals(Collections.<String>emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String>emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<String>emptySet(), result.getPartitionsNotOnFs());
    assertEquals(Collections.<String>emptySet(), result.getPartitionsNotInMs());

    // remove the table folder
    fs = table.getPath().getFileSystem(hive.getConf());
    fs.delete(table.getPath(), true);

    // now this shouldn't find the path on the fs
    result = new CheckResult();
    checker.checkMetastore(dbName, tableName, null, result);
    assertEquals(Collections.<String>emptySet(), result.getTablesNotInMs());;
    assertEquals(1, result.getTablesNotOnFs().size());
    assertEquals(tableName, result.getTablesNotOnFs().iterator().next());
    assertEquals(Collections.<String>emptySet(), result.getPartitionsNotOnFs());
    assertEquals(Collections.<String>emptySet(), result.getPartitionsNotInMs());

    // put it back and one additional table
    fs.mkdirs(table.getPath());
    Path fakeTable = table.getPath().getParent().suffix(
        Path.SEPARATOR + "faketable");
    fs.mkdirs(fakeTable);
    fs.deleteOnExit(fakeTable);

    // find the extra table
    result = new CheckResult();
    checker.checkMetastore(dbName, null, null, result);
    assertEquals(1, result.getTablesNotInMs().size());
    assertEquals(fakeTable.getName(), Lists.newArrayList(result.getTablesNotInMs()).get(0));
    assertEquals(Collections.<String>emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<String>emptySet(), result.getPartitionsNotOnFs());
    assertEquals(Collections.<String>emptySet(), result.getPartitionsNotInMs());

    // create a new external table
    hive.dropTable(dbName, tableName);
    table.setProperty("EXTERNAL", "TRUE");
    hive.createTable(table);

    // should return all ok
    result = new CheckResult();
    checker.checkMetastore(dbName, null, null, result);
    assertEquals(Collections.<String>emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String>emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<String>emptySet(), result.getPartitionsNotOnFs());
    assertEquals(Collections.<String>emptySet(), result.getPartitionsNotInMs());
  }

  /*
   * Tests the case when tblPath/p1=a/p2=b/p3=c/file for a table with partition (p1, p2)
   * does not throw HiveException
   */
  @Test
  public void testAdditionalPartitionDirs()
      throws HiveException, AlreadyExistsException, IOException {
    Table table = createTestTable();
    List<Partition> partitions = hive.getPartitions(table);
    assertEquals(2, partitions.size());
    // add a fake partition dir on fs
    fs = partitions.get(0).getDataLocation().getFileSystem(hive.getConf());
    Path fakePart = new Path(table.getDataLocation().toString(),
        partDateName + "=2017-01-01/" + partCityName + "=paloalto/fakePartCol=fakepartValue");
    fs.mkdirs(fakePart);
    fs.deleteOnExit(fakePart);
    CheckResult result = new CheckResult();
    checker.checkMetastore(dbName, tableName, null, result);
    assertEquals(Collections.<String> emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String> emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<String> emptySet(), result.getPartitionsNotOnFs());
    //fakePart path partition is added since the defined partition keys are valid
    assertEquals(1, result.getPartitionsNotInMs().size());
  }

  @Test(expected = HiveException.class)
  public void testInvalidPartitionKeyName() throws HiveException, AlreadyExistsException, IOException {
    Table table = createTestTable();
    List<Partition> partitions = hive.getPartitions(table);
    assertEquals(2, partitions.size());
    // add a fake partition dir on fs
    fs = partitions.get(0).getDataLocation().getFileSystem(hive.getConf());
    Path fakePart = new Path(table.getDataLocation().toString(),
        "fakedate=2009-01-01/fakecity=sanjose");
    fs.mkdirs(fakePart);
    fs.deleteOnExit(fakePart);
    checker.checkMetastore(dbName, tableName, null, new CheckResult());
  }

  /*
   * skip mode should not throw exception when a invalid partition directory
   * is found. It should just ignore it
   */
  @Test
  public void testSkipInvalidPartitionKeyName()
      throws HiveException, AlreadyExistsException, IOException {
    hive.getConf().set(HiveConf.ConfVars.HIVE_MSCK_PATH_VALIDATION.varname, "skip");
    checker = new HiveMetaStoreChecker(hive);
    Table table = createTestTable();
    List<Partition> partitions = hive.getPartitions(table);
    assertEquals(2, partitions.size());
    // add a fake partition dir on fs
    fs = partitions.get(0).getDataLocation().getFileSystem(hive.getConf());
    Path fakePart =
        new Path(table.getDataLocation().toString(), "fakedate=2009-01-01/fakecity=sanjose");
    fs.mkdirs(fakePart);
    fs.deleteOnExit(fakePart);
    createPartitionsDirectoriesOnFS(table, 2);
    CheckResult result = new CheckResult();
    checker.checkMetastore(dbName, tableName, null, result);
    assertEquals(Collections.<String> emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String> emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<String> emptySet(), result.getPartitionsNotOnFs());
    // only 2 valid partitions should be added
    assertEquals(2, result.getPartitionsNotInMs().size());
  }

  private Table createTestTable() throws AlreadyExistsException, HiveException {
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

    for (Map<String, String> partSpec : parts) {
      hive.createPartition(table, partSpec);
    }
    return table;
  }

  @Test
  public void testPartitionsCheck() throws HiveException, MetaException,
      IOException, TException, AlreadyExistsException {
    Table table = createTestTable();

    CheckResult result = new CheckResult();
    checker.checkMetastore(dbName, tableName, null, result);
    // all is well
    assertEquals(Collections.<String>emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String>emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<String>emptySet(), result.getPartitionsNotOnFs());
    assertEquals(Collections.<String>emptySet(), result.getPartitionsNotInMs());

    List<Partition> partitions = hive.getPartitions(table);
    assertEquals(2, partitions.size());

    Partition partToRemove = partitions.get(0);
    // As this partition (partdate=2008-01-01/partcity=london) is the only
    // partition under (partdate=2008-01-01)
    // we also need to delete partdate=2008-01-01 to make it consistent.
    Path partToRemovePath = partToRemove.getDataLocation().getParent();
    fs = partToRemovePath.getFileSystem(hive.getConf());
    fs.delete(partToRemovePath, true);

    result = new CheckResult();
    checker.checkMetastore(dbName, tableName, null, result);
    // missing one partition on fs
    assertEquals(Collections.<String>emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String>emptySet(), result.getTablesNotOnFs());
    assertEquals(1, result.getPartitionsNotOnFs().size());
    assertEquals(partToRemove.getName(), result.getPartitionsNotOnFs().iterator().next()
        .getPartitionName());
    assertEquals(partToRemove.getTable().getTableName(),
        result.getPartitionsNotOnFs().iterator().next().getTableName());
    assertEquals(Collections.<String>emptySet(), result.getPartitionsNotInMs());

    List<Map<String, String>> partsCopy = new ArrayList<Map<String, String>>();
    partsCopy.add(partitions.get(1).getSpec());
    // check only the partition that exists, all should be well
    result = new CheckResult();
    checker.checkMetastore(dbName, tableName, partsCopy, result);
    assertEquals(Collections.<String>emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String>emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<String>emptySet(), result.getPartitionsNotOnFs());
    assertEquals(Collections.<String>emptySet(), result.getPartitionsNotInMs());

    // old test is moved to msck_repair_2.q

    // cleanup
    hive.dropTable(dbName, tableName, true, true);
    hive.createTable(table);
    result = new CheckResult();
    checker.checkMetastore(dbName, null, null, result);
    assertEquals(Collections.<String>emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String>emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<String>emptySet(), result.getPartitionsNotOnFs());
    assertEquals(Collections.<String>emptySet(), result.getPartitionsNotInMs()); //--0e
    System.err.println("Test completed - partition check");
  }

  @Test
  public void testDataDeletion() throws HiveException, MetaException,
      IOException, TException, AlreadyExistsException, NoSuchObjectException {

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

  /*
   * Test multi-threaded implementation of checker to find out missing partitions
   */
  @Test
  public void testPartitionsNotInMs() throws HiveException, AlreadyExistsException, IOException {
    Table testTable = createPartitionedTestTable(dbName, tableName, 2, 0);
    // add 10 partitions on the filesystem
    createPartitionsDirectoriesOnFS(testTable, 10);
    CheckResult result = new CheckResult();
    checker.checkMetastore(dbName, tableName, null, result);
    assertEquals(Collections.<String>emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String>emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<String>emptySet(), result.getPartitionsNotOnFs());
    assertEquals(10, result.getPartitionsNotInMs().size());
  }

  /*
   * Tests single threaded implementation of checkMetastore
   */
  @Test
  public void testSingleThreadedCheckMetastore()
      throws HiveException, AlreadyExistsException, IOException {
    // set num of threads to 0 so that single-threaded checkMetastore is called
    hive.getConf().setIntVar(HiveConf.ConfVars.METASTORE_FS_HANDLER_THREADS_COUNT, 0);
    Table testTable = createPartitionedTestTable(dbName, tableName, 2, 0);
    // add 10 partitions on the filesystem
    createPartitionsDirectoriesOnFS(testTable, 10);
    CheckResult result = new CheckResult();
    checker.checkMetastore(dbName, tableName, null, result);
    assertEquals(Collections.<String> emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String> emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<String> emptySet(), result.getPartitionsNotOnFs());
    assertEquals(10, result.getPartitionsNotInMs().size());
  }

  /**
   * Tests single threaded implementation for deeply nested partitioned tables
   *
   * @throws HiveException
   * @throws AlreadyExistsException
   * @throws IOException
   */
  @Test
  public void testSingleThreadedDeeplyNestedTables()
      throws HiveException, AlreadyExistsException, IOException {
    // set num of threads to 0 so that single-threaded checkMetastore is called
    hive.getConf().setIntVar(HiveConf.ConfVars.METASTORE_FS_HANDLER_THREADS_COUNT, 0);
    int poolSize = 2;
    // create a deeply nested table which has more partition keys than the pool size
    Table testTable = createPartitionedTestTable(dbName, tableName, poolSize + 2, 0);
    // add 10 partitions on the filesystem
    createPartitionsDirectoriesOnFS(testTable, 10);
    CheckResult result = new CheckResult();
    checker.checkMetastore(dbName, tableName, null, result);
    assertEquals(Collections.<String> emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String> emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<String> emptySet(), result.getPartitionsNotOnFs());
    assertEquals(10, result.getPartitionsNotInMs().size());
  }

  /**
   * Tests the case when the number of partition keys are more than the threadpool size.
   *
   * @throws HiveException
   * @throws AlreadyExistsException
   * @throws IOException
   */
  @Test
  public void testDeeplyNestedPartitionedTables()
      throws HiveException, AlreadyExistsException, IOException {
    hive.getConf().setIntVar(HiveConf.ConfVars.METASTORE_FS_HANDLER_THREADS_COUNT, 2);
    int poolSize = 2;
    // create a deeply nested table which has more partition keys than the pool size
    Table testTable = createPartitionedTestTable(dbName, tableName, poolSize + 2, 0);
    // add 10 partitions on the filesystem
    createPartitionsDirectoriesOnFS(testTable, 10);
    CheckResult result = new CheckResult();
    checker.checkMetastore(dbName, tableName, null, result);
    assertEquals(Collections.<String> emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String> emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<String> emptySet(), result.getPartitionsNotOnFs());
    assertEquals(10, result.getPartitionsNotInMs().size());
  }

  /**
   * Test if checker throws HiveException when the there is a dummy directory present in the nested level
   * of sub-directories
   * @throws AlreadyExistsException
   * @throws IOException
   * @throws HiveException
   */
  @Test
  public void testErrorForMissingPartitionColumn() throws AlreadyExistsException, IOException, HiveException {
    Table testTable = createPartitionedTestTable(dbName, tableName, 2, 0);
    // add 10 partitions on the filesystem
    createPartitionsDirectoriesOnFS(testTable, 10);
    //create a fake directory to throw exception
    StringBuilder sb = new StringBuilder(testTable.getDataLocation().toString());
    sb.append(Path.SEPARATOR);
    sb.append("dummyPart=error");
    createDirectory(sb.toString());
    //check result now
    CheckResult result = new CheckResult();
    Exception exception = null;
    try {
      checker.checkMetastore(dbName, tableName, null, result);
    } catch (Exception e) {
      exception = e;
    }
    assertTrue("Expected HiveException", exception!=null && exception instanceof HiveException);
    createFile(sb.toString(), "dummyFile");
    result = new CheckResult();
    exception = null;
    try {
      checker.checkMetastore(dbName, tableName, null, result);
    } catch (Exception e) {
      exception = e;
    }
    assertTrue("Expected HiveException", exception!=null && exception instanceof HiveException);
  }

  /**
   * Tests if there exists a unknown partition directory on the FS with in-valid order of partition
   * keys than what is specified in table specification.
   *
   * @throws AlreadyExistsException
   * @throws HiveException
   * @throws IOException
   */
  @Test(expected = HiveException.class)
  public void testInvalidOrderForPartitionKeysOnFS()
      throws AlreadyExistsException, HiveException, IOException {
    Table testTable = createPartitionedTestTable(dbName, tableName, 2, 0);
    // add 10 partitions on the filesystem
    createInvalidPartitionDirsOnFS(testTable, 10);
    CheckResult result = new CheckResult();
    checker.checkMetastore(dbName, tableName, null, result);
  }

  /*
   * In skip mode msck should ignore invalid partitions instead of
   * throwing exception
   */
  @Test
  public void testSkipInvalidOrderForPartitionKeysOnFS()
      throws AlreadyExistsException, HiveException, IOException {
    hive.getConf().set(HiveConf.ConfVars.HIVE_MSCK_PATH_VALIDATION.varname, "skip");
    checker = new HiveMetaStoreChecker(hive);
    Table testTable = createPartitionedTestTable(dbName, tableName, 2, 0);
    // add 10 partitions on the filesystem
    createInvalidPartitionDirsOnFS(testTable, 2);
    // add 10 partitions on the filesystem
    createPartitionsDirectoriesOnFS(testTable, 2);
    CheckResult result = new CheckResult();
    checker.checkMetastore(dbName, tableName, null, result);
    assertEquals(Collections.<String> emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String> emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<String> emptySet(), result.getPartitionsNotOnFs());
    // only 2 valid partitions should be added
    assertEquals(2, result.getPartitionsNotInMs().size());
  }

  /*
   * Test if single-threaded implementation checker throws HiveException when the there is a dummy
   * directory present in the nested level
   */
  @Test
  public void testErrorForMissingPartitionsSingleThreaded()
      throws AlreadyExistsException, HiveException, IOException {
    // set num of threads to 0 so that single-threaded checkMetastore is called
    hive.getConf().setIntVar(HiveConf.ConfVars.METASTORE_FS_HANDLER_THREADS_COUNT, 0);
    Table testTable = createPartitionedTestTable(dbName, tableName, 2, 0);
    // add 10 partitions on the filesystem
    createPartitionsDirectoriesOnFS(testTable, 10);
    // create a fake directory to throw exception
    StringBuilder sb = new StringBuilder(testTable.getDataLocation().toString());
    sb.append(Path.SEPARATOR);
    sb.append("dummyPart=error");
    createDirectory(sb.toString());
    // check result now
    CheckResult result = new CheckResult();
    Exception exception = null;
    try {
      checker.checkMetastore(dbName, tableName, null, result);
    } catch (Exception e) {
      exception = e;
    }
    assertTrue("Expected HiveException", exception!=null && exception instanceof HiveException);
    createFile(sb.toString(), "dummyFile");
    result = new CheckResult();
    exception = null;
    try {
      checker.checkMetastore(dbName, tableName, null, result);
    } catch (Exception e) {
      exception = e;
    }
    assertTrue("Expected HiveException", exception!=null && exception instanceof HiveException);
  }

  /**
   * Test counts the number of listStatus calls in the msck core method of
   * listing sub-directories. This is important to check since it unnecessary
   * listStatus calls could cause performance degradation in remote filesystems
   * like S3. The test creates a mock FileSystem object and a mock directory structure
   * to simulate a table which has 2 partition keys and 2 partition values at each level.
   * In the end it counts how many times the listStatus is called on the mock filesystem
   * and confirm its equal to the current theoretical value.
   *
   * @throws IOException
   * @throws HiveException
   */
  @Test
  public void testNumberOfListStatusCalls() throws IOException, HiveException {
    LocalFileSystem mockFs = Mockito.mock(LocalFileSystem.class);
    Path tableLocation = new Path("mock:///tmp/testTable");

    Path countryUS = new Path(tableLocation, "country=US");
    Path countryIND = new Path(tableLocation, "country=IND");

    Path cityPA = new Path(countryUS, "city=PA");
    Path citySF = new Path(countryUS, "city=SF");
    Path cityBOM = new Path(countryIND, "city=BOM");
    Path cityDEL = new Path(countryIND, "city=DEL");

    Path paData = new Path(cityPA, "datafile");
    Path sfData = new Path(citySF, "datafile");
    Path bomData = new Path(cityBOM, "datafile");
    Path delData = new Path(cityDEL, "datafile");

    //level 1 listing
    FileStatus[] allCountries = getMockFileStatus(countryUS, countryIND);
    when(mockFs.listStatus(tableLocation, FileUtils.HIDDEN_FILES_PATH_FILTER))
        .thenReturn(allCountries);

    //level 2 listing
    FileStatus[] filesInUS = getMockFileStatus(cityPA, citySF);
    when(mockFs.listStatus(countryUS, FileUtils.HIDDEN_FILES_PATH_FILTER)).thenReturn(filesInUS);

    FileStatus[] filesInInd = getMockFileStatus(cityBOM, cityDEL);
    when(mockFs.listStatus(countryIND, FileUtils.HIDDEN_FILES_PATH_FILTER)).thenReturn(filesInInd);

    //level 3 listing
    FileStatus[] paFiles = getMockFileStatus(paData);
    when(mockFs.listStatus(cityPA, FileUtils.HIDDEN_FILES_PATH_FILTER)).thenReturn(paFiles);

    FileStatus[] sfFiles = getMockFileStatus(sfData);
    when(mockFs.listStatus(citySF, FileUtils.HIDDEN_FILES_PATH_FILTER)).thenReturn(sfFiles);

    FileStatus[] bomFiles = getMockFileStatus(bomData);
    when(mockFs.listStatus(cityBOM, FileUtils.HIDDEN_FILES_PATH_FILTER)).thenReturn(bomFiles);

    FileStatus[] delFiles = getMockFileStatus(delData);
    when(mockFs.listStatus(cityDEL, FileUtils.HIDDEN_FILES_PATH_FILTER)).thenReturn(delFiles);

    HiveMetaStoreChecker checker = new HiveMetaStoreChecker(hive);
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    Set<Path> result = new HashSet<>();
    checker.checkPartitionDirs(executorService, tableLocation, result, mockFs,
        Arrays.asList("country", "city"));
    // if there are n partition columns, then number of times listStatus should be called
    // must be equal
    // to (numDirsAtLevel1) + (numDirsAtLevel2) + ... + (numDirAtLeveln-1)
    // in this case it should 1 (table level) + 2 (US, IND)
    verify(mockFs, times(3)).listStatus(any(Path.class), any(PathFilter.class));
    Assert.assertEquals("msck should have found 4 unknown partitions", 4, result.size());
  }

  private FileStatus[] getMockFileStatus(Path... paths) throws IOException {
    FileStatus[] result = new FileStatus[paths.length];
    int i = 0;
    for (Path p : paths) {
      result[i++] = createMockFileStatus(p);
    }
    return result;
  }

  private FileStatus createMockFileStatus(Path p) {
    FileStatus mock = Mockito.mock(FileStatus.class);
    when(mock.getPath()).thenReturn(p);
    if (p.toString().contains("datafile")) {
      when(mock.isDirectory()).thenReturn(false);
    } else {
      when(mock.isDirectory()).thenReturn(true);
    }
    return mock;
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
   * @return
   * @throws AlreadyExistsException
   * @throws HiveException
   */
  private Table createPartitionedTestTable(String dbName, String tableName, int numOfPartKeys,
      int valuesPerPartition) throws AlreadyExistsException, HiveException {
    Database db = new Database();
    db.setName(dbName);
    hive.createDatabase(db);

    Table table = new Table(dbName, tableName);
    table.setDbName(dbName);
    table.setInputFormatClass(TextInputFormat.class);
    table.setOutputFormatClass(HiveIgnoreKeyTextOutputFormat.class);
    // create partition key schema
    ArrayList<FieldSchema> partKeys = new ArrayList<FieldSchema>();
    for (int i = 1; i <= numOfPartKeys; i++) {
      String partName = "part" + String.valueOf(i);
      partKeys.add(new FieldSchema(partName, serdeConstants.STRING_TYPE_NAME, ""));
    }
    table.setPartCols(partKeys);
    // create table
    hive.createTable(table);
    table = hive.getTable(dbName, tableName);
    if (valuesPerPartition == 0) {
      return table;
    }
    // create partition specs
    ArrayList<Map<String, String>> partitionSpecs = new ArrayList<Map<String, String>>();
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
   * @throws IOException
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
   * @param table
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
}
