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
package org.apache.hadoop.hive.ql.optimizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

public class TestGenMapRedUtilsCreateConditionalTask {
  private static HiveConf hiveConf;

  private Task dummyMRTask;

  @BeforeClass
  public static void initializeSessionState() {
    hiveConf = new HiveConf();
  }

  @Before
  public void setUp() {
    dummyMRTask = new MapRedTask();
  }

  @Test
  public void testMovePathsThatCannotBeMerged() {
    final Path condInputPath = new Path("s3a://bucket/scratch/-ext-10000");
    final Path condOutputPath = new Path("s3a://bucket/scratch/-ext-10002");
    final MoveWork mockWork = mock(MoveWork.class);

    assertFalse("A MoveWork null object cannot be merged.",
        GenMapRedUtils.shouldMergeMovePaths(hiveConf, condInputPath, condOutputPath, null));

    hiveConf.set(HiveConf.ConfVars.HIVE_BLOBSTORE_OPTIMIZATIONS_ENABLED.varname, "false");
    assertFalse("Merging paths is not allowed when BlobStorage optimizations are disabled.",
        GenMapRedUtils.shouldMergeMovePaths(hiveConf, condInputPath, condOutputPath, mockWork));

    // Enable BlobStore optimizations for the rest of tests
    hiveConf.set(HiveConf.ConfVars.HIVE_BLOBSTORE_OPTIMIZATIONS_ENABLED.varname, "true");

    reset(mockWork);
    when(mockWork.getLoadMultiFilesWork()).thenReturn(new LoadMultiFilesDesc());
    assertFalse("Merging paths is not allowed when MultiFileWork is found in the MoveWork object.",
        GenMapRedUtils.shouldMergeMovePaths(hiveConf, condInputPath, condOutputPath, mockWork));

    reset(mockWork);
    when(mockWork.getLoadFileWork()).thenReturn(mock(LoadFileDesc.class));
    when(mockWork.getLoadTableWork()).thenReturn(mock(LoadTableDesc.class));
    assertFalse("Merging paths is not allowed when both LoadFileWork & LoadTableWork are found in the MoveWork object.",
        GenMapRedUtils.shouldMergeMovePaths(hiveConf, condInputPath, condOutputPath, mockWork));

    reset(mockWork);
    when(mockWork.getLoadFileWork()).thenReturn(new LoadFileDesc(condInputPath, condOutputPath, false, "", ""));
    assertFalse("Merging paths is not allowed when both conditional output path is not equals to MoveWork input path.",
        GenMapRedUtils.shouldMergeMovePaths(hiveConf, condInputPath, condOutputPath, mockWork));

    reset(mockWork);
    when(mockWork.getLoadFileWork()).thenReturn(new LoadFileDesc(condOutputPath, new Path("unused"), false, "", ""));
    assertFalse("Merging paths is not allowed when conditional input path is not a BlobStore path.",
        GenMapRedUtils.shouldMergeMovePaths(hiveConf, new Path("hdfs://hdfs-path"), condOutputPath, mockWork));

    reset(mockWork);
    when(mockWork.getLoadFileWork()).thenReturn(new LoadFileDesc(condOutputPath, new Path("hdfs://hdfs-path"), false, "", ""));
    assertFalse("Merging paths is not allowed when MoveWork output path is not a BlobStore path.",
        GenMapRedUtils.shouldMergeMovePaths(hiveConf, condInputPath, condOutputPath, mockWork));
  }

  @Test
  public void testMovePathsThatCanBeMerged() {
    final Path condInputPath = new Path("s3a://bucket/scratch/-ext-10000");
    final Path condOutputPath = new Path("s3a://bucket/scratch/-ext-10002");
    final Path targetMoveWorkPath = new Path("s3a://bucket/scratch/-ext-10003");
    final MoveWork mockWork = mock(MoveWork.class);

    when(mockWork.getLoadFileWork()).thenReturn(new LoadFileDesc(condOutputPath, targetMoveWorkPath, false, "", ""));

    assertTrue("Merging BlobStore paths should be allowed.",
        GenMapRedUtils.shouldMergeMovePaths(hiveConf, condInputPath, condOutputPath, mockWork));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMergePathWithInvalidMoveWorkThrowsException() {
    final Path condInputPath = new Path("s3a://bucket/scratch/-ext-10000");
    final MoveWork mockWork = mock(MoveWork.class);

    when(mockWork.getLoadMultiFilesWork()).thenReturn(new LoadMultiFilesDesc());
    GenMapRedUtils.mergeMovePaths(condInputPath, mockWork);
  }

  @Test
  public void testMergePathValidMoveWorkReturnsNewMoveWork() {
    final Path condInputPath = new Path("s3a://bucket/scratch/-ext-10000");
    final Path condOutputPath = new Path("s3a://bucket/scratch/-ext-10002");
    final Path targetMoveWorkPath = new Path("s3a://bucket/scratch/-ext-10003");
    final MoveWork mockWork = mock(MoveWork.class);
    MoveWork newWork;

    // test using loadFileWork
    when(mockWork.getLoadFileWork()).thenReturn(new LoadFileDesc(condOutputPath, targetMoveWorkPath, false, "", ""));
    newWork = GenMapRedUtils.mergeMovePaths(condInputPath, mockWork);
    assertNotNull(newWork);
    assertNotEquals(newWork, mockWork);
    assertEquals(condInputPath, newWork.getLoadFileWork().getSourcePath());
    assertEquals(targetMoveWorkPath, newWork.getLoadFileWork().getTargetDir());

    // test using loadTableWork
    TableDesc tableDesc = new TableDesc();
    reset(mockWork);
    when(mockWork.getLoadTableWork()).thenReturn(new LoadTableDesc(condOutputPath, tableDesc, null));
    newWork = GenMapRedUtils.mergeMovePaths(condInputPath, mockWork);
    assertNotNull(newWork);
    assertNotEquals(newWork, mockWork);
    assertEquals(condInputPath, newWork.getLoadTableWork().getSourcePath());
    assertTrue(newWork.getLoadTableWork().getTable().equals(tableDesc));
  }

  @Test
  public void testConditionalMoveTaskIsOptimized() throws SemanticException {
    hiveConf.set(HiveConf.ConfVars.HIVE_BLOBSTORE_OPTIMIZATIONS_ENABLED.varname, "true");

    Path sinkDirName = new Path("s3a://bucket/scratch/-ext-10002");
    FileSinkOperator fileSinkOperator = createFileSinkOperator(sinkDirName);

    Path finalDirName = new Path("s3a://bucket/scratch/-ext-10000");
    Path tableLocation = new Path("s3a://bucket/warehouse/table");
    Task<MoveWork> moveTask = createMoveTask(finalDirName, tableLocation);
    List<Task<MoveWork>> moveTaskList = Arrays.asList(moveTask);

    GenMapRedUtils.createMRWorkForMergingFiles(fileSinkOperator, finalDirName, null, moveTaskList, hiveConf, dummyMRTask);
    ConditionalTask conditionalTask = (ConditionalTask)dummyMRTask.getChildTasks().get(0);
    Task<? extends Serializable> moveOnlyTask = conditionalTask.getListTasks().get(0);
    Task<? extends Serializable> mergeOnlyTask = conditionalTask.getListTasks().get(1);
    Task<? extends Serializable> mergeAndMoveTask = conditionalTask.getListTasks().get(2);

    /*
     * OPTIMIZATION
     * The ConditionalTask avoids linking 2 MoveTask that are expensive on blobstorage systems. Instead of
     * linking, it creates one MoveTask where the source is the first MoveTask source, and target is the
     * second MoveTask target.
     */

    // Verify moveOnlyTask is optimized
    assertNull(moveOnlyTask.getChildTasks());
    verifyMoveTask(moveOnlyTask, sinkDirName, tableLocation);

    // Verify mergeOnlyTask is NOT optimized (a merge task writes directly to finalDirName, then a MoveTask is executed)
    assertEquals(1, mergeOnlyTask.getChildTasks().size());
    verifyMoveTask(mergeOnlyTask.getChildTasks().get(0), finalDirName, tableLocation);

    // Verify mergeAndMoveTask is NOT optimized
    assertEquals(1, mergeAndMoveTask.getChildTasks().size());
    assertEquals(1, mergeAndMoveTask.getChildTasks().get(0).getChildTasks().size());
    verifyMoveTask(mergeAndMoveTask.getChildTasks().get(0), sinkDirName, finalDirName);
    verifyMoveTask(mergeAndMoveTask.getChildTasks().get(0).getChildTasks().get(0), finalDirName, tableLocation);
  }

  @Test
  public void testConditionalMoveTaskIsNotOptimized() throws SemanticException {
    hiveConf.set(HiveConf.ConfVars.HIVE_BLOBSTORE_OPTIMIZATIONS_ENABLED.varname, "false");

    Path sinkDirName = new Path("s3a://bucket/scratch/-ext-10002");
    FileSinkOperator fileSinkOperator = createFileSinkOperator(sinkDirName);

    Path finalDirName = new Path("s3a://bucket/scratch/-ext-10000");
    Path tableLocation = new Path("s3a://bucket/warehouse/table");
    Task<MoveWork> moveTask = createMoveTask(finalDirName, tableLocation);
    List<Task<MoveWork>> moveTaskList = Arrays.asList(moveTask);

    GenMapRedUtils.createMRWorkForMergingFiles(fileSinkOperator, finalDirName, null, moveTaskList, hiveConf, dummyMRTask);
    ConditionalTask conditionalTask = (ConditionalTask)dummyMRTask.getChildTasks().get(0);
    Task<? extends Serializable> moveOnlyTask = conditionalTask.getListTasks().get(0);
    Task<? extends Serializable> mergeOnlyTask = conditionalTask.getListTasks().get(1);
    Task<? extends Serializable> mergeAndMoveTask = conditionalTask.getListTasks().get(2);

    // Verify moveOnlyTask is NOT optimized
    assertEquals(1, moveOnlyTask.getChildTasks().size());
    verifyMoveTask(moveOnlyTask, sinkDirName, finalDirName);
    verifyMoveTask(moveOnlyTask.getChildTasks().get(0), finalDirName, tableLocation);

    // Verify mergeOnlyTask is NOT optimized
    assertEquals(1, mergeOnlyTask.getChildTasks().size());
    verifyMoveTask(mergeOnlyTask.getChildTasks().get(0), finalDirName, tableLocation);

    // Verify mergeAndMoveTask is NOT optimized
    assertEquals(1, mergeAndMoveTask.getChildTasks().size());
    assertEquals(1, mergeAndMoveTask.getChildTasks().get(0).getChildTasks().size());
    verifyMoveTask(mergeAndMoveTask.getChildTasks().get(0), sinkDirName, finalDirName);
    verifyMoveTask(mergeAndMoveTask.getChildTasks().get(0).getChildTasks().get(0), finalDirName, tableLocation);
  }

  @Test
  public void testConditionalMoveOnHdfsIsNotOptimized() throws SemanticException {
    hiveConf.set(HiveConf.ConfVars.HIVE_BLOBSTORE_OPTIMIZATIONS_ENABLED.varname, "true");

    Path sinkDirName = new Path("hdfs://bucket/scratch/-ext-10002");
    FileSinkOperator fileSinkOperator = createFileSinkOperator(sinkDirName);

    Path finalDirName = new Path("hdfs://bucket/scratch/-ext-10000");
    Path tableLocation = new Path("hdfs://bucket/warehouse/table");
    Task<MoveWork> moveTask = createMoveTask(finalDirName, tableLocation);
    List<Task<MoveWork>> moveTaskList = Arrays.asList(moveTask);

    GenMapRedUtils.createMRWorkForMergingFiles(fileSinkOperator, finalDirName, null, moveTaskList, hiveConf, dummyMRTask);
    ConditionalTask conditionalTask = (ConditionalTask)dummyMRTask.getChildTasks().get(0);
    Task<? extends Serializable> moveOnlyTask = conditionalTask.getListTasks().get(0);
    Task<? extends Serializable> mergeOnlyTask = conditionalTask.getListTasks().get(1);
    Task<? extends Serializable> mergeAndMoveTask = conditionalTask.getListTasks().get(2);

    // Verify moveOnlyTask is NOT optimized
    assertEquals(1, moveOnlyTask.getChildTasks().size());
    verifyMoveTask(moveOnlyTask, sinkDirName, finalDirName);
    verifyMoveTask(moveOnlyTask.getChildTasks().get(0), finalDirName, tableLocation);

    // Verify mergeOnlyTask is NOT optimized
    assertEquals(1, mergeOnlyTask.getChildTasks().size());
    verifyMoveTask(mergeOnlyTask.getChildTasks().get(0), finalDirName, tableLocation);

    // Verify mergeAndMoveTask is NOT optimized
    assertEquals(1, mergeAndMoveTask.getChildTasks().size());
    assertEquals(1, mergeAndMoveTask.getChildTasks().get(0).getChildTasks().size());
    verifyMoveTask(mergeAndMoveTask.getChildTasks().get(0), sinkDirName, finalDirName);
    verifyMoveTask(mergeAndMoveTask.getChildTasks().get(0).getChildTasks().get(0), finalDirName, tableLocation);
  }

  private FileSinkOperator createFileSinkOperator(Path finalDirName) {
    FileSinkOperator fileSinkOperator = mock(FileSinkOperator.class);

    TableDesc tableDesc = new TableDesc(HiveInputFormat.class, HiveOutputFormat.class, new Properties());
    FileSinkDesc fileSinkDesc = new FileSinkDesc(finalDirName, tableDesc, false);
    fileSinkDesc.setDirName(finalDirName);

    when(fileSinkOperator.getConf()).thenReturn(fileSinkDesc);
    when(fileSinkOperator.getSchema()).thenReturn(mock(RowSchema.class));
    fileSinkDesc.setTableInfo(tableDesc);

    when(fileSinkOperator.getCompilationOpContext()).thenReturn(mock(CompilationOpContext.class));

    return fileSinkOperator;
  }

  private Task<MoveWork> createMoveTask(Path source, Path destination) {
    Task<MoveWork> moveTask = mock(MoveTask.class);
    MoveWork moveWork = new MoveWork();
    moveWork.setLoadFileWork(new LoadFileDesc(source, destination, true, null, null));

    when(moveTask.getWork()).thenReturn(moveWork);

    return moveTask;
  }

  private void verifyMoveTask(Task<? extends Serializable> task, Path source, Path target) {
    MoveTask moveTask = (MoveTask)task;
    assertEquals(source, moveTask.getWork().getLoadFileWork().getSourcePath());
    assertEquals(target, moveTask.getWork().getLoadFileWork().getTargetDir());
  }
}
