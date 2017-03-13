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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.thrift.TException;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import junit.framework.TestCase;

/**
 * TestHiveMetaStoreChecker.
 *
 */
public class TestHiveMetaStoreChecker extends TestCase {

  private Hive hive;
  private FileSystem fs;
  private HiveMetaStoreChecker checker = null;

  private final String dbName = "testhivemetastorechecker_db";
  private final String tableName = "testhivemetastorechecker_table";

  private final String partDateName = "partdate";
  private final String partCityName = "partcity";

  private List<FieldSchema> partCols;
  private List<Map<String, String>> parts;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
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

  @Override
  protected void tearDown() throws Exception {
    dropDbTable();
    super.tearDown();
    Hive.closeCurrent();
  }

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

  public void testPartitionsCheck() throws HiveException, MetaException,
      IOException, TException, AlreadyExistsException {
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

    CheckResult result = new CheckResult();
    checker.checkMetastore(dbName, tableName, null, result);
    // all is well
    assertEquals(Collections.<String>emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String>emptySet(), result.getTablesNotOnFs());
    assertEquals(Collections.<String>emptySet(), result.getPartitionsNotOnFs());
    assertEquals(Collections.<String>emptySet(), result.getPartitionsNotInMs());

    List<Partition> partitions = hive.getPartitions(table);
    assertEquals(2, partitions.size());
    // add a fake partition dir on fs to ensure that it does not get added
    fs = partitions.get(0).getDataLocation().getFileSystem(hive.getConf());
    Path fakePart = new Path(table.getDataLocation().toString(),
        "fakedate=2009-01-01/fakecity=sanjose");
    fs.mkdirs(fakePart);
    fs.deleteOnExit(fakePart);
    checker.checkMetastore(dbName, tableName, null, result);
    assertEquals(Collections.<String>emptySet(), result.getTablesNotInMs());
    assertEquals(Collections.<String>emptySet(), result.getTablesNotOnFs());
    assertEquals(0, result.getPartitionsNotOnFs().size());
    assertEquals(0, result.getPartitionsNotInMs().size());
    assertEquals(2, partitions.size()); //no additional partitions got added

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

  /*
   * Test if single-threaded implementation checker throws HiveException when the there is a dummy
   * directory present in the nested level
   */
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
   * @throws IOException
   */
  private void createPartitionsDirectoriesOnFS(Table table, int numPartitions) throws IOException {
    String path = table.getDataLocation().toString();
    fs = table.getPath().getFileSystem(hive.getConf());
    int numPartKeys = table.getPartitionKeys().size();
    for (int i = 0; i < numPartitions; i++) {
      StringBuilder partPath = new StringBuilder(path);
      partPath.append(Path.SEPARATOR);
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
      createDirectory(partPath.toString());
    }
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
    fs.deleteOnExit(part);
  }
}
