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

package org.apache.hadoop.hive.ql.exec;

import static org.apache.hadoop.hive.ql.exec.Utilities.DEPRECATED_MAPRED_DFSCLIENT_PARALLELISM_MAX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.apache.hadoop.hive.ql.exec.Utilities.getFileExtension;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.mr.ExecDriver;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.io.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.DependencyCollectionWork;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFFromUtcTimestamp;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class TestUtilities {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  public static final Logger LOG = LoggerFactory.getLogger(TestUtilities.class);
  private static final int NUM_BUCKETS = 3;

  @Test
  public void testGetFileExtension() {
    JobConf jc = new JobConf();
    assertEquals("No extension for uncompressed unknown format", "",
        getFileExtension(jc, false, null));
    assertEquals("No extension for compressed unknown format", "",
        getFileExtension(jc, true, null));
    assertEquals("No extension for uncompressed text format", "",
        getFileExtension(jc, false, new HiveIgnoreKeyTextOutputFormat()));
    assertEquals("Deflate for uncompressed text format", ".deflate",
        getFileExtension(jc, true, new HiveIgnoreKeyTextOutputFormat()));

    String extension = ".myext";
    jc.set("hive.output.file.extension", extension);
    assertEquals("Custom extension for uncompressed unknown format", extension,
        getFileExtension(jc, false, null));
    assertEquals("Custom extension for compressed unknown format", extension,
        getFileExtension(jc, true, null));
    assertEquals("Custom extension for uncompressed text format", extension,
        getFileExtension(jc, false, new HiveIgnoreKeyTextOutputFormat()));
    assertEquals("Custom extension for uncompressed text format", extension,
        getFileExtension(jc, true, new HiveIgnoreKeyTextOutputFormat()));
  }

  @Test
  public void testSerializeTimestamp() {
    Timestamp ts = Timestamp.ofEpochMilli(1374554702000L, 123456);
    ExprNodeConstantDesc constant = new ExprNodeConstantDesc(ts);
    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>(1);
    children.add(constant);
    ExprNodeGenericFuncDesc desc = new ExprNodeGenericFuncDesc(TypeInfoFactory.timestampTypeInfo,
        new GenericUDFFromUtcTimestamp(), children);
    assertEquals(desc.getExprString(), SerializationUtilities.deserializeExpression(
        SerializationUtilities.serializeExpression(desc)).getExprString());
  }

  @Test
  public void testgetDbTableName() throws HiveException{
    String tablename;
    String [] dbtab;
    SessionState.start(new HiveConf(this.getClass()));
    String curDefaultdb = SessionState.get().getCurrentDatabase();

    //test table without db portion
    tablename = "tab1";
    dbtab = Utilities.getDbTableName(tablename);
    assertEquals("db name", curDefaultdb, dbtab[0]);
    assertEquals("table name", tablename, dbtab[1]);

    //test table with db portion
    tablename = "dab1.tab1";
    dbtab = Utilities.getDbTableName(tablename);
    assertEquals("db name", "dab1", dbtab[0]);
    assertEquals("table name", "tab1", dbtab[1]);

    //test invalid table name
    tablename = "dab1.tab1.x1";
    try {
      dbtab = Utilities.getDbTableName(tablename);
      fail("exception was expected for invalid table name");
    } catch(HiveException ex){
      assertEquals("Invalid table name " + tablename, ex.getMessage());
    }
  }

  @Test
  public void testReplaceTaskId() {
    String taskID = "000000";
    int bucketNum = 1;
    String newTaskID = Utilities.replaceTaskId(taskID, bucketNum);
    Assert.assertEquals("000001", newTaskID);
    taskID = "(ds%3D1)000001";
    newTaskID = Utilities.replaceTaskId(taskID, 5);
    Assert.assertEquals("(ds%3D1)000005", newTaskID);
  }

  @Test
  public void testRemoveTempOrDuplicateFilesOnTezNoDp() throws Exception {
    List<Path> paths = runRemoveTempOrDuplicateFilesTestCase("tez", false);
    assertEquals(0, paths.size());
  }

  @Test
  public void testRemoveTempOrDuplicateFilesOnTezWithDp() throws Exception {
    List<Path> paths = runRemoveTempOrDuplicateFilesTestCase("tez", true);
    assertEquals(0, paths.size());
  }

  @Test
  public void testRemoveTempOrDuplicateFilesOnMrNoDp() throws Exception {
    List<Path> paths = runRemoveTempOrDuplicateFilesTestCase("mr", false);
    assertEquals(NUM_BUCKETS, paths.size());
  }

  @Test
  public void testRemoveTempOrDuplicateFilesOnMrWithDp() throws Exception {
    List<Path> paths = runRemoveTempOrDuplicateFilesTestCase("mr", true);
    assertEquals(NUM_BUCKETS, paths.size());
  }

  private List<Path> runRemoveTempOrDuplicateFilesTestCase(String executionEngine, boolean dPEnabled)
      throws Exception {
    Configuration hconf = new HiveConf(this.getClass());
    // do this to verify that Utilities.removeTempOrDuplicateFiles does not revert to default scheme information
    hconf.set("fs.defaultFS", "hdfs://should-not-be-used/");
    hconf.set(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname, executionEngine);
    FileSystem localFs = FileSystem.getLocal(hconf);
    DynamicPartitionCtx dpCtx = getDynamicPartitionCtx(dPEnabled);
    Path tempDirPath = setupTempDirWithSingleOutputFile(hconf);
    FileSinkDesc conf = getFileSinkDesc(tempDirPath);

    List<Path> paths = Utilities.removeTempOrDuplicateFiles(localFs, tempDirPath, dpCtx, conf, hconf, false);

    String expectedScheme = tempDirPath.toUri().getScheme();
    String expectedAuthority = tempDirPath.toUri().getAuthority();
    assertPathsMatchSchemeAndAuthority(expectedScheme, expectedAuthority, paths);

    return paths;
  }

  private void assertPathsMatchSchemeAndAuthority(String expectedScheme, String expectedAuthority, List<Path> paths) {
    for (Path path : paths) {
      assertEquals(path.toUri().getScheme().toLowerCase(), expectedScheme.toLowerCase());
      assertEquals(path.toUri().getAuthority(), expectedAuthority);
    }
  }

  private DynamicPartitionCtx getDynamicPartitionCtx(boolean dPEnabled) {
    DynamicPartitionCtx dpCtx = null;
    if (dPEnabled) {
      dpCtx = mock(DynamicPartitionCtx.class);
      when(dpCtx.getNumDPCols()).thenReturn(0);
      when(dpCtx.getNumBuckets()).thenReturn(NUM_BUCKETS);
    }
    return dpCtx;
  }

  private FileSinkDesc getFileSinkDesc(Path tempDirPath) {
    Table table = mock(Table.class);
    when(table.getNumBuckets()).thenReturn(NUM_BUCKETS);
    FileSinkDesc conf = new FileSinkDesc(tempDirPath, null, false);
    conf.setTable(table);
    return conf;
  }

  private Path setupTempDirWithSingleOutputFile(Configuration hconf) throws IOException {
    Path tempDirPath = new Path("file://" + temporaryFolder.newFolder().getAbsolutePath());
    Path taskOutputPath = new Path(tempDirPath, Utilities.getTaskId(hconf));
    FileSystem.getLocal(hconf).create(taskOutputPath).close();
    return tempDirPath;
  }

  /**
   * Check that calling {@link Utilities#getInputPaths(JobConf, MapWork, Path, Context, boolean)}
   * can process two different tables that both have empty partitions.
   */
  @Test
  public void testGetInputPathsWithEmptyPartitions() throws Exception {
    String alias1Name = "alias1";
    String alias2Name = "alias2";

    MapWork mapWork1 = new MapWork();
    MapWork mapWork2 = new MapWork();
    JobConf jobConf = new JobConf();
    Configuration conf = new Configuration();

    Path nonExistentPath1 = new Path(UUID.randomUUID().toString());
    Path nonExistentPath2 = new Path(UUID.randomUUID().toString());

    PartitionDesc mockPartitionDesc = mock(PartitionDesc.class);
    TableDesc mockTableDesc = mock(TableDesc.class);

    when(mockTableDesc.isNonNative()).thenReturn(false);
    when(mockTableDesc.getProperties()).thenReturn(new Properties());

    when(mockPartitionDesc.getProperties()).thenReturn(new Properties());
    when(mockPartitionDesc.getTableDesc()).thenReturn(mockTableDesc);
    doReturn(HiveSequenceFileOutputFormat.class).when(
            mockPartitionDesc).getOutputFileFormatClass();

    mapWork1.setPathToAliases(new LinkedHashMap<>(
            ImmutableMap.of(nonExistentPath1, Lists.newArrayList(alias1Name))));
    mapWork1.setAliasToWork(new LinkedHashMap<>(
            ImmutableMap.of(alias1Name, (Operator<?>) mock(Operator.class))));
    mapWork1.setPathToPartitionInfo(new LinkedHashMap<>(
            ImmutableMap.of(nonExistentPath1, mockPartitionDesc)));

    mapWork2.setPathToAliases(new LinkedHashMap<>(
            ImmutableMap.of(nonExistentPath2, Lists.newArrayList(alias2Name))));
    mapWork2.setAliasToWork(new LinkedHashMap<>(
            ImmutableMap.of(alias2Name, (Operator<?>) mock(Operator.class))));
    mapWork2.setPathToPartitionInfo(new LinkedHashMap<>(
            ImmutableMap.of(nonExistentPath2, mockPartitionDesc)));

    List<Path> inputPaths = new ArrayList<>();
    try {
      Path scratchDir = new Path(HiveConf.getVar(jobConf, HiveConf.ConfVars.LOCALSCRATCHDIR));

      List<Path> inputPaths1 = Utilities.getInputPaths(jobConf, mapWork1, scratchDir,
              mock(Context.class), false);
      inputPaths.addAll(inputPaths1);
      assertEquals(inputPaths1.size(), 1);
      assertNotEquals(inputPaths1.get(0), nonExistentPath1);
      assertTrue(inputPaths1.get(0).getFileSystem(conf).exists(inputPaths1.get(0)));
      assertFalse(nonExistentPath1.getFileSystem(conf).exists(nonExistentPath1));

      List<Path> inputPaths2 = Utilities.getInputPaths(jobConf, mapWork2, scratchDir,
              mock(Context.class), false);
      inputPaths.addAll(inputPaths2);
      assertEquals(inputPaths2.size(), 1);
      assertNotEquals(inputPaths2.get(0), nonExistentPath2);
      assertTrue(inputPaths2.get(0).getFileSystem(conf).exists(inputPaths2.get(0)));
      assertFalse(nonExistentPath2.getFileSystem(conf).exists(nonExistentPath2));
    } finally {
      File file;
      for (Path path : inputPaths) {
        file = new File(path.toString());
        if (file.exists()) {
          file.delete();
        }
      }
    }
  }

  /**
   * Check that calling {@link Utilities#getInputPaths(JobConf, MapWork, Path, Context, boolean)}
   * can process two different tables that both have empty partitions when using multiple threads.
   * Some extra logic is placed at the end of the test to validate no race conditions put the
   * {@link MapWork} object in an invalid state.
   */
  @Test
  public void testGetInputPathsWithMultipleThreadsAndEmptyPartitions() throws Exception {
    int numPartitions = 15;
    JobConf jobConf = new JobConf();
    jobConf.setInt(HiveConf.ConfVars.HIVE_EXEC_INPUT_LISTING_MAX_THREADS.varname,
            Runtime.getRuntime().availableProcessors() * 2);
    MapWork mapWork = new MapWork();
    Path testTablePath = new Path("testTable");
    Path[] testPartitionsPaths = new Path[numPartitions];

    PartitionDesc mockPartitionDesc = mock(PartitionDesc.class);
    TableDesc mockTableDesc = mock(TableDesc.class);

    when(mockTableDesc.isNonNative()).thenReturn(false);
    when(mockTableDesc.getProperties()).thenReturn(new Properties());
    when(mockPartitionDesc.getProperties()).thenReturn(new Properties());
    when(mockPartitionDesc.getTableDesc()).thenReturn(mockTableDesc);
    doReturn(HiveSequenceFileOutputFormat.class).when(
            mockPartitionDesc).getOutputFileFormatClass();


    for (int i = 0; i < numPartitions; i++) {
      String testPartitionName = "p=" + i;
      testPartitionsPaths[i] = new Path(testTablePath, "p=" + i);
      mapWork.getPathToAliases().put(testPartitionsPaths[i], Lists.newArrayList(testPartitionName));
      mapWork.getAliasToWork().put(testPartitionName, mock(Operator.class));
      mapWork.getPathToPartitionInfo().put(testPartitionsPaths[i], mockPartitionDesc);

    }

    FileSystem fs = FileSystem.getLocal(jobConf);

    try {
      fs.mkdirs(testTablePath);
      List<Path> inputPaths = Utilities.getInputPaths(jobConf, mapWork,
              new Path(HiveConf.getVar(jobConf, HiveConf.ConfVars.LOCALSCRATCHDIR)), mock(Context.class), false);
      assertEquals(inputPaths.size(), numPartitions);

      for (int i = 0; i < numPartitions; i++) {
        assertNotEquals(inputPaths.get(i), testPartitionsPaths[i]);
      }

      assertEquals(mapWork.getPathToAliases().size(), numPartitions);
      assertEquals(mapWork.getPathToPartitionInfo().size(), numPartitions);
      assertEquals(mapWork.getAliasToWork().size(), numPartitions);

      for (Map.Entry<Path, List<String>> entry : mapWork.getPathToAliases().entrySet()) {
        assertNotNull(entry.getKey());
        assertNotNull(entry.getValue());
        assertEquals(entry.getValue().size(), 1);
        assertTrue(entry.getKey().getFileSystem(new Configuration()).exists(entry.getKey()));
      }
    } finally {
      if (fs.exists(testTablePath)) {
        fs.delete(testTablePath, true);
      }
    }
  }

  /**
   * Check that calling {@link Utilities#getMaxExecutorsForInputListing(Configuration, int)}
   * returns the maximum number of executors to use based on the number of input locations.
   */
  @Test
  public void testGetMaxExecutorsForInputListing() {
    Configuration conf = new Configuration();

    final int ZERO_EXECUTORS = 0;
    final int ONE_EXECUTOR = 1;
    final int TWO_EXECUTORS = 2;

    final int ZERO_THREADS = 0;
    final int ONE_THREAD = 1;
    final int TWO_THREADS = 2;

    final int ZERO_LOCATIONS = 0;
    final int ONE_LOCATION = 1;
    final int TWO_LOCATIONS = 2;
    final int THREE_LOCATIONS = 3;

    conf.setInt(HiveConf.ConfVars.HIVE_EXEC_INPUT_LISTING_MAX_THREADS.varname, ONE_THREAD);

    assertEquals(ZERO_EXECUTORS, Utilities.getMaxExecutorsForInputListing(conf, ZERO_LOCATIONS));
    assertEquals(ONE_EXECUTOR, Utilities.getMaxExecutorsForInputListing(conf, ONE_LOCATION));
    assertEquals(ONE_EXECUTOR, Utilities.getMaxExecutorsForInputListing(conf, TWO_LOCATIONS));
    assertEquals(ONE_EXECUTOR, Utilities.getMaxExecutorsForInputListing(conf, THREE_LOCATIONS));

    conf.setInt(HiveConf.ConfVars.HIVE_EXEC_INPUT_LISTING_MAX_THREADS.varname, TWO_THREADS);

    assertEquals(ZERO_EXECUTORS,  Utilities.getMaxExecutorsForInputListing(conf, ZERO_LOCATIONS));
    assertEquals(ONE_EXECUTOR,  Utilities.getMaxExecutorsForInputListing(conf, ONE_LOCATION));
    assertEquals(TWO_EXECUTORS, Utilities.getMaxExecutorsForInputListing(conf, TWO_LOCATIONS));
    assertEquals(TWO_EXECUTORS, Utilities.getMaxExecutorsForInputListing(conf, THREE_LOCATIONS));

    /*
     * The following tests will verify the deprecation variable is still usable.
     */

    conf.setInt(HiveConf.ConfVars.HIVE_EXEC_INPUT_LISTING_MAX_THREADS.varname, ZERO_THREADS);
    conf.setInt(DEPRECATED_MAPRED_DFSCLIENT_PARALLELISM_MAX, ZERO_THREADS);

    assertEquals(ZERO_EXECUTORS, Utilities.getMaxExecutorsForInputListing(conf, ZERO_LOCATIONS));
    assertEquals(ONE_EXECUTOR, Utilities.getMaxExecutorsForInputListing(conf, ONE_LOCATION));
    assertEquals(ONE_EXECUTOR, Utilities.getMaxExecutorsForInputListing(conf, TWO_LOCATIONS));
    assertEquals(ONE_EXECUTOR, Utilities.getMaxExecutorsForInputListing(conf, THREE_LOCATIONS));

    conf.setInt(HiveConf.ConfVars.HIVE_EXEC_INPUT_LISTING_MAX_THREADS.varname, ZERO_THREADS);
    conf.setInt(DEPRECATED_MAPRED_DFSCLIENT_PARALLELISM_MAX, ONE_THREAD);

    assertEquals(ZERO_EXECUTORS, Utilities.getMaxExecutorsForInputListing(conf, ZERO_LOCATIONS));
    assertEquals(ONE_EXECUTOR, Utilities.getMaxExecutorsForInputListing(conf, ONE_LOCATION));
    assertEquals(ONE_EXECUTOR, Utilities.getMaxExecutorsForInputListing(conf, TWO_LOCATIONS));
    assertEquals(ONE_EXECUTOR, Utilities.getMaxExecutorsForInputListing(conf, THREE_LOCATIONS));

    conf.setInt(HiveConf.ConfVars.HIVE_EXEC_INPUT_LISTING_MAX_THREADS.varname, ZERO_THREADS);
    conf.setInt(DEPRECATED_MAPRED_DFSCLIENT_PARALLELISM_MAX, TWO_THREADS);

    assertEquals(ZERO_EXECUTORS, Utilities.getMaxExecutorsForInputListing(conf, ZERO_LOCATIONS));
    assertEquals(ONE_EXECUTOR, Utilities.getMaxExecutorsForInputListing(conf, ONE_LOCATION));
    assertEquals(TWO_EXECUTORS, Utilities.getMaxExecutorsForInputListing(conf, TWO_LOCATIONS));
    assertEquals(TWO_EXECUTORS, Utilities.getMaxExecutorsForInputListing(conf, THREE_LOCATIONS));

    // Check that HIVE_EXEC_INPUT_LISTING_MAX_THREADS has priority overr DEPRECATED_MAPRED_DFSCLIENT_PARALLELISM_MAX

    conf.setInt(HiveConf.ConfVars.HIVE_EXEC_INPUT_LISTING_MAX_THREADS.varname, TWO_THREADS);
    conf.setInt(DEPRECATED_MAPRED_DFSCLIENT_PARALLELISM_MAX, ONE_THREAD);

    assertEquals(ZERO_EXECUTORS, Utilities.getMaxExecutorsForInputListing(conf, ZERO_LOCATIONS));
    assertEquals(ONE_EXECUTOR, Utilities.getMaxExecutorsForInputListing(conf, ONE_LOCATION));
    assertEquals(TWO_EXECUTORS, Utilities.getMaxExecutorsForInputListing(conf, TWO_LOCATIONS));
    assertEquals(TWO_EXECUTORS, Utilities.getMaxExecutorsForInputListing(conf, THREE_LOCATIONS));
  }

  /**
   * Test for {@link Utilities#getInputPaths(JobConf, MapWork, Path, Context, boolean)} with a single
   * threaded.
   */
  @Test
  public void testGetInputPathsWithASingleThread() throws Exception {
    final int NUM_PARTITIONS = 5;

    JobConf jobConf = new JobConf();

    jobConf.setInt(HiveConf.ConfVars.HIVE_EXEC_INPUT_LISTING_MAX_THREADS.varname, 1);
    runTestGetInputPaths(jobConf, NUM_PARTITIONS);
  }

  /**
   * Test for {@link Utilities#getInputPaths(JobConf, MapWork, Path, Context, boolean)} with multiple
   * threads.
   */
  @Test
  public void testGetInputPathsWithMultipleThreads() throws Exception {
    final int NUM_PARTITIONS = 5;

    JobConf jobConf = new JobConf();

    jobConf.setInt(HiveConf.ConfVars.HIVE_EXEC_INPUT_LISTING_MAX_THREADS.varname, 2);
    runTestGetInputPaths(jobConf, NUM_PARTITIONS);
  }

  private void runTestGetInputPaths(JobConf jobConf, int numOfPartitions) throws Exception {
    MapWork mapWork = new MapWork();
    Path scratchDir = new Path(HiveConf.getVar(jobConf, HiveConf.ConfVars.LOCALSCRATCHDIR));

    Map<Path, List<String>> pathToAliasTable = new LinkedHashMap<>();

    String testTableName = "testTable";

    Path testTablePath = new Path(testTableName);
    Path[] testPartitionsPaths = new Path[numOfPartitions];
    for (int i=0; i<numOfPartitions; i++) {
      String testPartitionName = "p=" + i;
      testPartitionsPaths[i] = new Path(testTablePath, "p=" + i);

      pathToAliasTable.put(testPartitionsPaths[i], Lists.newArrayList(testPartitionName));

      mapWork.getAliasToWork().put(testPartitionName, mock(Operator.class));
    }

    mapWork.setPathToAliases(pathToAliasTable);

    FileSystem fs = FileSystem.getLocal(jobConf);
    try {
      fs.mkdirs(testTablePath);

      for (int i=0; i<numOfPartitions; i++) {
        fs.mkdirs(testPartitionsPaths[i]);
        fs.create(new Path(testPartitionsPaths[i], "test1.txt")).close();
      }

      List<Path> inputPaths = Utilities.getInputPaths(jobConf, mapWork, scratchDir, mock(Context.class), false);
      assertEquals(inputPaths.size(), numOfPartitions);
      for (int i=0; i<numOfPartitions; i++) {
        assertEquals(inputPaths.get(i), testPartitionsPaths[i]);
      }
    } finally {
      if (fs.exists(testTablePath)) {
        fs.delete(testTablePath, true);
      }
    }
  }

  @Test
  public void testGetInputPathsPool() throws IOException, ExecutionException, InterruptedException {
    List<Path> pathsToAdd = new ArrayList<>();
    Path path = new Path("dummy-path");

    pathsToAdd.add(path);
    pathsToAdd.add(path);
    pathsToAdd.add(path);

    ExecutorService pool = mock(ExecutorService.class);
    Future mockFuture = mock(Future.class);

    when(mockFuture.get()).thenReturn(path);
    when(pool.submit(any(Callable.class))).thenReturn(mockFuture);

    Utilities.getInputPathsWithPool(mock(JobConf.class), mock(MapWork.class), mock(Path.class), mock(Context.class),
            false, pathsToAdd, pool);

    verify(pool, times(3)).submit(any(Callable.class));
    verify(pool).shutdown();
    verify(pool).shutdownNow();
  }

  @Test
  public void testGetInputPathsPoolAndFailure() throws IOException, ExecutionException, InterruptedException {
    List<Path> pathsToAdd = new ArrayList<>();
    Path path = new Path("dummy-path");

    pathsToAdd.add(path);
    pathsToAdd.add(path);
    pathsToAdd.add(path);

    ExecutorService pool = mock(ExecutorService.class);
    Future mockFuture = mock(Future.class);

    when(mockFuture.get()).thenThrow(new RuntimeException());
    when(pool.submit(any(Callable.class))).thenReturn(mockFuture);

    Exception e = null;
    try {
      Utilities.getInputPathsWithPool(mock(JobConf.class), mock(MapWork.class), mock(Path.class), mock(Context.class),
              false, pathsToAdd, pool);
    } catch (Exception thrownException) {
      e = thrownException;
    }
    assertNotNull(e);

    verify(pool, times(3)).submit(any(Callable.class));
    verify(pool).shutdownNow();
  }

  private Task<?> getDependencyCollectionTask(){
    return TaskFactory.get(new DependencyCollectionWork());
  }

  /**
   * Generates a task graph that looks like this:
   *
   *       ---->DTa----
   *      /            \
   * root ----->DTb-----*-->DTd---> ProvidedTask --> DTe
   *      \            /
   *       ---->DTc----
   */
  private List<Task<?>> getTestDiamondTaskGraph(Task<?> providedTask){
    // Note: never instantiate a task without TaskFactory.get() if you're not
    // okay with .equals() breaking. Doing it via TaskFactory.get makes sure
    // that an id is generated, and two tasks of the same type don't show
    // up as "equal", which is important for things like iterating over an
    // array. Without this, DTa, DTb, and DTc would show up as one item in
    // the list of children. Thus, we're instantiating via a helper method
    // that instantiates via TaskFactory.get()
    Task<?> root = getDependencyCollectionTask();
    Task<?> DTa = getDependencyCollectionTask();
    Task<?> DTb = getDependencyCollectionTask();
    Task<?> DTc = getDependencyCollectionTask();
    Task<?> DTd = getDependencyCollectionTask();
    Task<?> DTe = getDependencyCollectionTask();

    root.addDependentTask(DTa);
    root.addDependentTask(DTb);
    root.addDependentTask(DTc);

    DTa.addDependentTask(DTd);
    DTb.addDependentTask(DTd);
    DTc.addDependentTask(DTd);

    DTd.addDependentTask(providedTask);

    providedTask.addDependentTask(DTe);

    List<Task<?>> retVals = new ArrayList<Task<?>>();
    retVals.add(root);
    return retVals;
  }

  /**
   * DependencyCollectionTask that counts how often getDependentTasks on it
   * (and thus, on its descendants) is called counted via Task.getDependentTasks.
   * It is used to wrap another task to intercept calls on it.
   */
  public class CountingWrappingTask extends DependencyCollectionTask {
    int count;
    Task<?> wrappedDep = null;

    public CountingWrappingTask(Task<?> dep) {
      count = 0;
      wrappedDep = dep;
      super.addDependentTask(wrappedDep);
    }

    @Override
    public boolean addDependentTask(Task<?> dependent) {
      return wrappedDep.addDependentTask(dependent);
    }

    @Override
    public List<Task<?>> getDependentTasks() {
      count++;
      System.err.println("YAH:getDepTasks got called!");
      (new Exception()).printStackTrace(System.err);
      LOG.info("YAH!getDepTasks", new Exception());
      return super.getDependentTasks();
    }

    public int getDepCallCount() {
      return count;
    }

    @Override
    public String getName() {
      return "COUNTER_TASK";
    }

    @Override
    public String toString() {
      return getName() + "_" + wrappedDep.toString();
    }
  };

  /**
   * This test tests that Utilities.get*Tasks do not repeat themselves in the process
   * of extracting tasks from a given set of root tasks when given DAGs that can have
   * multiple paths, such as the case with Diamond-shaped DAGs common to replication.
   */
  @Test
  public void testGetTasksHaveNoRepeats() {

    CountingWrappingTask mrTask = new CountingWrappingTask(new ExecDriver());
    CountingWrappingTask tezTask = new CountingWrappingTask(new TezTask());
    CountingWrappingTask sparkTask = new CountingWrappingTask(new SparkTask());

    // First check - we should not have repeats in results
    assertEquals("No repeated MRTasks from Utilities.getMRTasks", 1,
        Utilities.getMRTasks(getTestDiamondTaskGraph(mrTask)).size());
    assertEquals("No repeated TezTasks from Utilities.getTezTasks", 1,
        Utilities.getTezTasks(getTestDiamondTaskGraph(tezTask)).size());
    assertEquals("No repeated TezTasks from Utilities.getSparkTasks", 1,
        Utilities.getSparkTasks(getTestDiamondTaskGraph(sparkTask)).size());

    // Second check - the tasks we looked for must not have been accessed more than
    // once as a result of the traversal (note that we actually wind up accessing
    // 2 times , because each visit counts twice, once to check for existence, and
    // once to visit.

    assertEquals("MRTasks should have been visited only once", 2, mrTask.getDepCallCount());
    assertEquals("TezTasks should have been visited only once", 2, tezTask.getDepCallCount());
    assertEquals("SparkTasks should have been visited only once", 2, sparkTask.getDepCallCount());

  }

  private static Task<MapredWork> getMapredWork() {
    return TaskFactory.get(MapredWork.class);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetTasksRecursion() {

    Task<MapredWork> rootTask = getMapredWork();
    Task<MapredWork> child1 = getMapredWork();
    Task<MapredWork> child2 = getMapredWork();
    Task<MapredWork> child11 = getMapredWork();

    rootTask.addDependentTask(child1);
    rootTask.addDependentTask(child2);
    child1.addDependentTask(child11);

    assertEquals(Lists.newArrayList(rootTask, child1, child2, child11),
        Utilities.getMRTasks(getTestDiamondTaskGraph(rootTask)));

  }
}
