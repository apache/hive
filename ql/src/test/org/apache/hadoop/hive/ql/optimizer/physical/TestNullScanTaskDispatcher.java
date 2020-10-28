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

package org.apache.hadoop.hive.ql.optimizer.physical;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.common.util.MockFileSystem;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestNullScanTaskDispatcher {

  private static final Path WAREHOUSE_DIR = new Path("mock:///warehouse");
  private final String PARTITION_FIELD = "part";

  private HiveConf hiveConf;
  private Context context;
  private ParseContext parseContext;
  private SessionState sessionState;
  private MockFileSystem fs;

  private void setup(float ratio, String testTable, int partitionCount) throws IOException {
    hiveConf = new HiveConf();
    HiveConf.setFloatVar(hiveConf, HiveConf.ConfVars.HIVENULLSCAN_RECURSIVE_LISTING_RATIO, ratio);
    hiveConf.set("fs.mock.impl", MockFileSystem.class.getName());
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVEMETADATAONLYQUERIES, true);
    sessionState = SessionState.start(hiveConf);
    parseContext = new ParseContext();
    context = new Context(hiveConf);


    Path testTablePath = new Path(WAREHOUSE_DIR, testTable);
    fs = (MockFileSystem) WAREHOUSE_DIR.getFileSystem(hiveConf);
    for (int i = 0; i < partitionCount; i++) {
      Path p = new Path(testTablePath, PARTITION_FIELD + "=" + i);
      fs.create(p);
    }
  }

  @After
  public void tearDown() throws IOException {
    if (sessionState != null) {
      sessionState.close();
    }
    StorageStatistics storageStatistics = FileSystem.getGlobalStorageStatistics().get("mock");
    if (storageStatistics != null) {
      storageStatistics.reset();
    }
    if (fs != null) {
      fs.clear();
      fs.close();
    }
  }

  @Test
  public void testListFilesRecursiveIsUsedWhenQueryingMostPartitionDirectories() throws IOException, SemanticException {
    testRecursiveListing(0.2f, 100, 100, 2);
  }

  @Test
  public void testListStatusIsUsedWhenQueryingFewPartitionDirectories() throws IOException, SemanticException {
    testRecursiveListing(0.2f, 100, 10, 11);
  }

  @Test
  public void testListStatusIsUsedWhenPartitionDirectoryThresholdIsNotMet() throws IOException, SemanticException {
    testRecursiveListing(1, 100, 100, 101);
  }

  private void testRecursiveListing(float recursiveRatio, int partitionCount,
                                    int queriedPartitionCount, int expectedReadOps)
      throws IOException, SemanticException {
    final String TEST_TABLE = "testTable";

    setup(recursiveRatio, TEST_TABLE, partitionCount);

    MapWork mapWork = new MapWork();
    ReduceWork reduceWork = new ReduceWork();

    Path testTablePath = new Path(WAREHOUSE_DIR, TEST_TABLE);
    for (int i = 0; i < queriedPartitionCount; i++) {
      addPartitionPath(mapWork, TEST_TABLE, new Path(testTablePath, PARTITION_FIELD + "=" + i));
    }
    Map aliasToWork = new HashMap<>();

    // operator setup
    parseContext.setTopOps(aliasToWork);
    TableScanOperator tso = new TableScanOperator(mock(CompilationOpContext.class));
    TableScanDesc tableScanDesc = mock(TableScanDesc.class);
    Table table = mock(Table.class);
    when(tableScanDesc.getTableMetadata()).thenReturn(table);
    tso.setConf(tableScanDesc);
    GroupByOperator gbo = new GroupByOperator(mock(CompilationOpContext.class));
    GroupByDesc groupByDesc = mock(GroupByDesc.class);
    when(groupByDesc.isDistinctLike()).thenReturn(true);
    gbo.setConf(groupByDesc);
    FileSinkOperator fso = new FileSinkOperator(mock(CompilationOpContext.class));
    gbo.setChildOperators(Collections.singletonList(fso));
    aliasToWork.put(TEST_TABLE, tso);
    mapWork.setAliasToWork(aliasToWork);
    reduceWork.setReducer(gbo);

    PhysicalContext physicalContext = new PhysicalContext(hiveConf, parseContext, context, getAsRootTaskList(mapWork, reduceWork), null);

    new MetadataOnlyOptimizer().resolve(physicalContext);

    assertEquals(1, mapWork.getPathToPartitionInfo().size());
    StorageStatistics statistics = FileSystem.getGlobalStorageStatistics().get("mock");
    assertEquals(expectedReadOps , (long) statistics.getLong("readOps"));
  }

  private void addPartitionPath(MapWork mapWork, String table, Path path) {
    mapWork.addPathToAlias(path, table);
    PartitionDesc partitionDesc = new PartitionDesc();
    partitionDesc.setProperties(new Properties());
    partitionDesc.setPartSpec(new LinkedHashMap<>());
    partitionDesc.setTableDesc(mock(TableDesc.class));
    mapWork.addPathToPartitionInfo(path, partitionDesc);
  }

  private List<Task<?>> getAsRootTaskList(MapWork mapWork, ReduceWork reduceWork) {
    MapredWork mapredWork = new MapredWork();
    mapredWork.setMapWork(mapWork);
    mapredWork.setReduceWork(reduceWork);
    Task<MapredWork> task = TaskFactory.get(mapredWork);
    return Collections.singletonList(task);
  }

}
