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
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
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
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.hadoop.hive.common.StatsSetupConst.COLUMN_STATS_ACCURATE;
import static org.apache.hadoop.hive.common.StatsSetupConst.ROW_COUNT;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestNullScanTaskDispatcher {

  private static final Path WAREHOUSE_DIR = new Path("mock:///warehouse");
  private final String PARTITION_FIELD = "part";

  private HiveConf hiveConf;
  private Context context;
  private ParseContext parseContext;
  private SessionState sessionState;
  private MockFileSystem fs;
  private MapWork mapWork = new MapWork();
  private ReduceWork reduceWork = new ReduceWork();
  private Map aliasToWork = new HashMap();

  @Before
  public void setup() {
    hiveConf = new HiveConf();
    hiveConf.set("fs.mock.impl", MockFileSystem.class.getName());
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_METADATA_ONLY_QUERIES, true);
    sessionState = SessionState.start(hiveConf);
    parseContext = spy(new ParseContext());
    context = new Context(hiveConf);

    parseContext.setTopOps(aliasToWork);
    mapWork.setAliasToWork(aliasToWork);
    createReduceWork();
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
  public void testNumberOfListStatusCalls1() throws IOException, SemanticException {
    verifyNumberOfReads(3, 10, 10, 3);
  }

  @Test
  public void testNumberOfListStatusCalls2() throws IOException, SemanticException {
    verifyNumberOfReads(3, 10, 5, 8);
  }

  @Test
  public void testNumberOfListStatusCalls3() throws IOException, SemanticException {
    verifyNumberOfReads(3, 10, 0, 13);
  }

  @Test
  public void testNumberOfListStatusCalls4() throws IOException, SemanticException {
    verifyNumberOfReads(0, 10, 0, 10);
  }

  @Test
  public void testNumberOfListStatusCalls5() throws IOException, SemanticException {
    verifyNumberOfReads(0, 10, 10, 0);
  }

  @Test
  public void testNumberOfListStatusCalls_whenExternalLookupRunsInCaller() throws IOException, SemanticException {
    verifyNumberOfReads(1, 0, 0, 1);
  }

  @Test
  public void testTwoManagedTables() throws IOException, SemanticException {
    final String managedTable1 = "table1";
    final String managedTable2 = "table2";

    createTable(managedTable1, 100, 99);
    createTable(managedTable2, 200, 190);

    // operator setup
    TableScanOperator tso1 = createTableScanOperator(false);
    TableScanOperator tso2 = createTableScanOperator(false);

    aliasToWork.put(managedTable1, tso1);
    aliasToWork.put(managedTable2, tso2);

    PhysicalContext physicalContext = new PhysicalContext(hiveConf, parseContext, context, getAsRootTaskList(mapWork, reduceWork), null);

    new MetadataOnlyOptimizer().resolve(physicalContext);

    assertEquals(1, mapWork.getPathToPartitionInfo().size());
    StorageStatistics statistics = FileSystem.getGlobalStorageStatistics().get("mock");
    assertEquals(11 , (long) statistics.getLong("readOps"));
  }

  private void verifyNumberOfReads(int externalPartitionCount, int managedPartitionCount, int upToDateManagedPartitions, int expectedReadOps)
      throws IOException, SemanticException {
    final String MANAGED_TEST_TABLE = "managedTestTable";
    final String EXTERNAL_TEST_TABLE = "externalTestTable";

    createTable(EXTERNAL_TEST_TABLE, externalPartitionCount, 0);
    createTable(MANAGED_TEST_TABLE, managedPartitionCount, upToDateManagedPartitions);

    // operator setup
    TableScanOperator tsoManaged = createTableScanOperator(false);
    TableScanOperator tsoExternal = createTableScanOperator(true);

    aliasToWork.put(MANAGED_TEST_TABLE, tsoManaged);
    aliasToWork.put(EXTERNAL_TEST_TABLE, tsoExternal);

    PhysicalContext physicalContext = new PhysicalContext(hiveConf, parseContext, context, getAsRootTaskList(mapWork, reduceWork), null);

    new MetadataOnlyOptimizer().resolve(physicalContext);

    assertEquals(1, mapWork.getPathToPartitionInfo().size());
    StorageStatistics statistics = FileSystem.getGlobalStorageStatistics().get("mock");
    assertEquals(expectedReadOps , (long) statistics.getLong("readOps"));
  }

  private void createReduceWork() {
    GroupByOperator gbo = new GroupByOperator(mock(CompilationOpContext.class));
    GroupByDesc groupByDesc = mock(GroupByDesc.class);
    when(groupByDesc.isDistinctLike()).thenReturn(true);
    gbo.setConf(groupByDesc);
    FileSinkOperator fso = new FileSinkOperator(mock(CompilationOpContext.class));
    gbo.setChildOperators(Collections.singletonList(fso));
    reduceWork.setReducer(gbo);
  }

  private TableScanOperator createTableScanOperator(boolean isExternal) {
    TableScanOperator tso = new TableScanOperator(mock(CompilationOpContext.class));
    TableScanDesc tableScanDesc = mock(TableScanDesc.class);
    Table table = mock(Table.class);
    Map<String, String> parameterMap = new HashMap<>();
    if (isExternal) {
      parameterMap.put("EXTERNAL", "TRUE");
    }
    org.apache.hadoop.hive.metastore.api.Table ttable = mock(org.apache.hadoop.hive.metastore.api.Table.class);
    when(ttable.getParameters()).thenReturn(parameterMap);
    when(table.getTTable()).thenReturn(ttable);
    when(tableScanDesc.getTableMetadata()).thenReturn(table);
    tso.setConf(tableScanDesc);
    return tso;
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

  private Partition createMockPartitionObject(Path path, boolean isUpToDate) {
    Partition partition = mock(Partition.class);
    when(partition.getPartitionPath()).thenReturn(path);
    Map<String, String> parameters = new HashMap<>();
    when(partition.getParameters()).thenReturn(parameters);
    parameters.put(COLUMN_STATS_ACCURATE, Boolean.toString(isUpToDate).toUpperCase());
    parameters.put(ROW_COUNT, "0");
    return partition;
  }

  private void createTable(String tableName, int partitionCount, int upToDatePartitions) throws IOException, SemanticException {
    Path tablePath = new Path(WAREHOUSE_DIR, tableName);
    Set<Partition> partitionSet = new HashSet<>();
    fs = (MockFileSystem) WAREHOUSE_DIR.getFileSystem(hiveConf);
    for (int i = 0; i < partitionCount; i++) {
      Path p = new Path(tablePath, PARTITION_FIELD + "=" + i);
      addPartitionPath(mapWork, tableName, p);
      fs.create(p);
      boolean upToDate = i < upToDatePartitions;
      partitionSet.add(createMockPartitionObject(p, upToDate));
    }

    PrunedPartitionList ppl = mock(PrunedPartitionList.class);
    when(ppl.getPartitions()).thenReturn(partitionSet);
    doReturn(ppl).when(parseContext).getPrunedPartitions(startsWith(tableName), any());
  }
}
