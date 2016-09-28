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

package org.apache.hadoop.hive.ql.exec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.apache.hadoop.hive.ql.exec.Utilities.getFileExtension;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.io.FileUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
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
import com.google.common.collect.Sets;
import com.google.common.io.Files;

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
    assertEquals("No extension for uncompressed default format", "",
        getFileExtension(jc, false));
    assertEquals("Deflate for uncompressed default format", ".deflate",
        getFileExtension(jc, true));

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
    Timestamp ts = new Timestamp(1374554702000L);
    ts.setNanos(123456);
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
  public void testGetJarFilesByPath() {
    File f = Files.createTempDir();
    String jarFileName1 = f.getAbsolutePath() + File.separator + "a.jar";
    String jarFileName2 = f.getAbsolutePath() + File.separator + "b.jar";
    File jarFile = new File(jarFileName1);
    try {
      FileUtils.touch(jarFile);
      HashSet<String> jars = (HashSet) Utilities.getJarFilesByPath(f.getAbsolutePath());
      Assert.assertEquals(Sets.newHashSet(jarFile.getAbsolutePath()),jars);

      File jarFile2 = new File(jarFileName2);
      FileUtils.touch(jarFile2);
      String newPath = "file://" + jarFileName1 + "," + "file://" + jarFileName2;
      jars = (HashSet) Utilities.getJarFilesByPath(newPath);

      Assert.assertEquals(Sets.newHashSet("file://" + jarFileName1, "file://" + jarFileName2), jars);
    } catch (IOException e) {
      LOG.error("failed to copy file to reloading folder", e);
      Assert.fail(e.getMessage());
    } finally {
      FileUtils.deleteQuietly(f);
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

    List<Path> paths = Utilities.removeTempOrDuplicateFiles(localFs, tempDirPath, dpCtx, conf, hconf);

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
   * can process two different empty tables without throwing any exceptions.
   */
  @Test
  public void testGetInputPathsWithEmptyTables() throws Exception {
    String alias1Name = "alias1";
    String alias2Name = "alias2";

    MapWork mapWork1 = new MapWork();
    MapWork mapWork2 = new MapWork();
    JobConf jobConf = new JobConf();

    String nonExistentPath1 = UUID.randomUUID().toString();
    String nonExistentPath2 = UUID.randomUUID().toString();

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
    mapWork1.setAliasToWork(new LinkedHashMap<String, Operator<? extends OperatorDesc>>(
            ImmutableMap.of(alias1Name, (Operator<?>) mock(Operator.class))));
    mapWork1.setPathToPartitionInfo(new LinkedHashMap<>(
            ImmutableMap.of(nonExistentPath1, mockPartitionDesc)));

    mapWork2.setPathToAliases(new LinkedHashMap<>(
            ImmutableMap.of(nonExistentPath2, Lists.newArrayList(alias2Name))));
    mapWork2.setAliasToWork(new LinkedHashMap<String, Operator<? extends OperatorDesc>>(
            ImmutableMap.of(alias2Name, (Operator<?>) mock(Operator.class))));
    mapWork2.setPathToPartitionInfo(new LinkedHashMap<>(
            ImmutableMap.of(nonExistentPath2, mockPartitionDesc)));

    List<Path> inputPaths = new ArrayList<>();
    try {
      Path scratchDir = new Path(HiveConf.getVar(jobConf, HiveConf.ConfVars.LOCALSCRATCHDIR));
      inputPaths.addAll(Utilities.getInputPaths(jobConf, mapWork1, scratchDir,
              mock(Context.class), false));
      inputPaths.addAll(Utilities.getInputPaths(jobConf, mapWork2, scratchDir,
              mock(Context.class), false));
      assertEquals(inputPaths.size(), 2);
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
}
