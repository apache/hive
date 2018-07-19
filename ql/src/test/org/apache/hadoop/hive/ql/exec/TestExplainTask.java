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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Before;
import org.junit.Test;

import java.io.PrintStream;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestExplainTask {

  private static final String BACKUP_ID = "backup-id-mock";
  private static final String AST = "ast-mock";

  private PrintStream out;
  private ExplainTask uut;
  private ObjectMapper objectMapper = new ObjectMapper();

  @Before
  public void setUp() {
    uut = new ExplainTask();
    uut.conf = mock(HiveConf.class);
    out = mock(PrintStream.class);
    QueryState qs = mock(QueryState.class);
    HiveConf hiveConf = new HiveConf();
    when(qs.getConf()).thenReturn(hiveConf);
    uut.queryState = qs;
  }

  public static class DummyExplainDesc<K, V> extends TableScanDesc {
    private static final long serialVersionUID = 1L;
    private Map<K, V> explainResult;

    public DummyExplainDesc(Map<K, V> explainResult) {
      this.explainResult = explainResult;
    }

    @Explain(displayName = "test", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
    public Map<K, V> explainMethod() {
      return explainResult;
    }
  }

  public static class DummyOperator extends TableScanOperator {
    private static final long serialVersionUID = 1L;

    public DummyOperator(TableScanDesc conf) {
      super();
      setConf(conf);
    }

  }

  @Test
  public void testExplainDoesSortTopLevelMapEntries() throws Exception {
    LinkedHashMap<String, String> explainMap1 = new LinkedHashMap<>();
    explainMap1.put("/k1", "v");
    explainMap1.put("k3", "v");
    explainMap1.put("hdfs:///k2", "v");
    explainMap1.put("hdfs:///k1", "v");

    LinkedHashMap<String, String> explainMap2 = new LinkedHashMap<>();
    explainMap2.put("hdfs:///k1", "v");
    explainMap2.put("hdfs:///k2", "v");
    explainMap2.put("/k1", "v");
    explainMap2.put("k3", "v");

    String result1 = explainToString(explainMap1);
    String result2 = explainToString(explainMap2);

    assertEquals("both maps should be ordered, regardless of input order", result1, result2);
  }

  @Test
  public void testExplainDoesSortPathAsStrings() throws Exception {
    LinkedHashMap<String, String> explainMap1 = new LinkedHashMap<>();
    explainMap1.put("/k1", "v");
    explainMap1.put("k3", "v");
    explainMap1.put("hdfs:/k2", "v");
    explainMap1.put("hdfs:/k1", "v");

    LinkedHashMap<Path, String> explainMap2 = new LinkedHashMap<>();
    explainMap2.put(new Path("hdfs:/k1"), "v");
    explainMap2.put(new Path("hdfs:/k2"), "v");
    explainMap2.put(new Path("/k1"), "v");
    explainMap2.put(new Path("k3"), "v");

    String result1 = explainToString(explainMap1);
    String result2 = explainToString(explainMap2);

    assertEquals("both maps should be sorted the same way", result1, result2);
  }

  @Test
  public void testExplainDoesSortMapValues() throws Exception {
    LinkedHashMap<String, String> explainMap1Val = new LinkedHashMap<>();
    explainMap1Val.put("a", "v");
    explainMap1Val.put("b", "v");

    LinkedHashMap<String, Map<String, String>> explainMap1 = new LinkedHashMap<>();
    explainMap1.put("k", explainMap1Val);

    LinkedHashMap<String, String> explainMap2Val = new LinkedHashMap<>();
    explainMap2Val.put("b", "v");
    explainMap2Val.put("a", "v");

    LinkedHashMap<String, Map<String, String>> explainMap2 = new LinkedHashMap<>();
    explainMap2.put("k", explainMap2Val);

    String result1 = explainToString(explainMap1);
    String result2 = explainToString(explainMap2);

    assertEquals("both maps should be sorted the same way", result1, result2);
  }

  private <K, V> String explainToString(Map<K, V> explainMap) throws Exception {
    ExplainWork work = new ExplainWork();
    ParseContext pCtx = new ParseContext();
    HashMap<String, TableScanOperator> topOps = new HashMap<>();
    TableScanOperator scanOp = new DummyOperator(new DummyExplainDesc<K, V>(explainMap));
    topOps.put("sample", scanOp);
    pCtx.setTopOps(topOps);
    work.setParseContext(pCtx);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    work.setConfig(new ExplainConfiguration());
    ExplainTask newExplainTask = new ExplainTask();
    newExplainTask.queryState = uut.queryState;
    newExplainTask.getJSONLogicalPlan(new PrintStream(baos), work);
    baos.close();
    return baos.toString();
  }

  @Test
  public void testGetJSONDependenciesJsonShhouldMatch() throws Exception {
    ExplainWork work = mockExplainWork();

    when(work.getDependency()).thenReturn(true);

    // Mock inputs
    HashSet<ReadEntity> inputs = new HashSet<>();

    // One input table
    Table table = mock(Table.class);
    when(table.getCompleteName()).thenReturn("table-name-mock");
    when(table.getTableType()).thenReturn(TableType.EXTERNAL_TABLE);
    ReadEntity input1 = mock(ReadEntity.class);
    when(input1.getType()).thenReturn(Entity.Type.TABLE);
    when(input1.getTable()).thenReturn(table);
    inputs.add(input1);

    // And one partition
    Partition partition = mock(Partition.class);
    when(partition.getCompleteName()).thenReturn("partition-name-mock");
    ReadEntity input2 = mock(ReadEntity.class);
    when(input2.getType()).thenReturn(Entity.Type.PARTITION);
    when(input2.getPartition()).thenReturn(partition);
    inputs.add(input2);

    when(work.getInputs()).thenReturn(inputs);

    JsonNode result = objectMapper.readTree(ExplainTask.getJSONDependencies(work).toString());
    JsonNode expected = objectMapper.readTree("{\"input_partitions\":[{\"partitionName\":" +
            "\"partition-name-mock\"}],\"input_tables\":[{\"tablename\":\"table-name-mock\"," +
            "\"tabletype\":\"EXTERNAL_TABLE\"}]}");

    assertEquals(expected, result);
  }

  @Test
  public void testGetJSONPlan() throws Exception {
    when(uut.conf.getVar(HiveConf.ConfVars.HIVESTAGEIDREARRANGE)).thenReturn("EXECUTION");
    Task mockTask = mockTask();
    when(mockTask.getId()).thenReturn("mockTaskId");
    ExplainWork explainWorkMock = mockExplainWork();
    when(mockTask.getWork()).thenReturn(explainWorkMock);
    List<Task<?>> tasks = Arrays.<Task<?>>asList(mockTask);


    JsonNode result = objectMapper.readTree(uut.getJSONPlan(null, tasks, null, true,
            false, false, null).toString());
    JsonNode expected = objectMapper.readTree("{\"STAGE DEPENDENCIES\":{\"mockTaskId\":" +
            "{\"ROOT STAGE\":\"TRUE\",\"BACKUP STAGE\":\"backup-id-mock\"}},\"STAGE PLANS\":" +
            "{\"mockTaskId\":{}}}");

    assertEquals(expected, result);
  }

  @Test
  public void testOutputDependenciesJsonShouldMatch() throws Exception {
    Task<? extends ExplainTask> task = mockTask();

    JsonNode result = objectMapper.readTree(
            uut.outputDependencies(task, out, null, true, true, 0).toString());
    JsonNode expected = objectMapper.readTree("{\"ROOT STAGE\":\"TRUE\",\"BACKUP STAGE\":" +
            "\""+BACKUP_ID+"\",\"TASK TYPE\":\"EXPLAIN\"}");

    assertEquals(expected, result);
  }

  @Test
  public void testGetJSONLogicalPlanJsonShouldMatch() throws Exception {
    JsonNode result = objectMapper.readTree(
            uut.getJSONLogicalPlan(null, mockExplainWork()).toString());
    JsonNode expected = objectMapper.readTree("{}");

    assertEquals(expected, result);
  }

  @Test
  public void testOutputMapJsonShouldMatch() throws Exception {
    Map<Object, Object> map = new LinkedHashMap<>();

    // String
    map.put("key-1", "value-1");

    // SparkWork
    map.put("spark-work", new SparkWork("spark-work"));

    // Empty list
    List<Object> emptList = Collections.emptyList();
    map.put("empty-list", emptList);

    // List of TezWork.Dependency
    List<Object> tezList1 = new ArrayList<>(Arrays.asList(new Object[] {mockTezWorkDependency()}));
    map.put("tez-list-1", tezList1);
    List<Object> tezList2 = new ArrayList<>(
            Arrays.asList(new Object[] {mockTezWorkDependency(), mockTezWorkDependency()}));
    map.put("tez-list-2", tezList2);

    // List of SparkWork.Dependency
    List<Object> sparkList1 = new ArrayList<>(
            Arrays.asList(new Object[]{mockSparkWorkDependency()}));
    map.put("spark-list-1", sparkList1);
    List<Object> sparkList2 = new ArrayList<>(
            Arrays.asList(new Object[]{mockSparkWorkDependency(), mockSparkWorkDependency()}));
    map.put("spark-list-2", sparkList2);

    // inner Map
    Map<Object, Object> innerMap = new LinkedHashMap<>();
    innerMap.put("inner-key-1", "inner-value-1");
    innerMap.put("inner-key-2", tezList1);
    map.put("map-1", innerMap);

    JsonNode result = objectMapper.readTree(
            uut.outputMap(map, false, null, false, true, 0).toString());
    JsonNode expected = objectMapper.readTree("{\"key-1\":\"value-1\",\"tez-list-2\":" +
            "[{\"parent\":\"name\"}," + "{\"parent\":\"name\"}],\"tez-list-1\":" +
            "{\"parent\":\"name\"},\"empty-list\":\"[]\",\"spark-list-2\":" +
            "[{\"parent\":\"mock-name\"},{\"parent\":\"mock-name\"}]," +
            "\"spark-list-1\":{\"parent\":" +
            "\"mock-name\"}, \"map-1\":\"{inner-key-1=inner-value-1, " +
            "inner-key-2=[mock-tez-dependency]}\",\"spark-work\":" +
            "{\"Spark\":{\"DagName:\":\"spark-work:2\"}}}");

    assertEquals(expected, result);
  }

  @Test
  public void testOutputPlanJsonShouldMatch() throws Exception {
    // SparkWork
    SparkWork work = new SparkWork("spark-work");

    JsonNode result = objectMapper.readTree(
            uut.outputPlan(work, null, false, true, 0, null).toString());
    JsonNode expected = objectMapper.readTree("{\"Spark\":{\"DagName:\":\"spark-work:1\"}}");
    assertEquals(expected, result);

    // Operator with single child
    CollectOperator parentCollectOperator1 = new CollectOperator();
    CollectOperator child1 = new CollectOperator();
    parentCollectOperator1.setChildOperators(new ArrayList<Operator<? extends OperatorDesc>>(
            Arrays.asList(new CollectOperator[] {child1})));
    parentCollectOperator1.setConf(new CollectDesc());

    result = objectMapper.readTree(
            uut.outputPlan(parentCollectOperator1, null, false, true, 0, null).toString());
    expected = objectMapper.readTree("{\"Collect\":{\"children\":{}}}");
    assertEquals(expected, result);

    // Operator with 2 children
    CollectOperator parentCollectOperator2 = new CollectOperator();
    CollectOperator child2 = new CollectOperator();
    parentCollectOperator2.setChildOperators(new ArrayList<Operator<? extends OperatorDesc>>(
            Arrays.asList(new CollectOperator[] {child1, child2})));
    parentCollectOperator2.setConf(new CollectDesc());
    result = objectMapper.readTree(
            uut.outputPlan(parentCollectOperator2, null, false, true, 0, null).toString());
    expected = objectMapper.readTree("{\"Collect\":{\"children\":[{},{}]}}");
    assertEquals(expected, result);
  }

  @Test
  public void testCollectAuthRelatedEntitiesJsonShouldMatch() throws Exception {
    QueryState qs = mock(QueryState.class);
    when(qs.getHiveOperation()).thenReturn(HiveOperation.EXPLAIN);
    uut.queryState = qs;

    SessionState.start(new HiveConf(ExplainTask.class));
    // SessionState.get().setCommandType(HiveOperation.EXPLAIN);
    HiveAuthenticationProvider authenticationProviderMock = mock(HiveAuthenticationProvider.class);
    when(authenticationProviderMock.getUserName()).thenReturn("test-user");
    SessionState.get().setAuthenticator(authenticationProviderMock);
    SessionState.get().setAuthorizer(mock(HiveAuthorizationProvider.class));
    ExplainWork work = mockExplainWork();

    JsonNode result = objectMapper.readTree(uut.collectAuthRelatedEntities(null, work).toString());
    JsonNode expected = objectMapper.readTree("{\"CURRENT_USER\":\"test-user\"," +
            "\"OPERATION\":\"EXPLAIN\",\"INPUTS\":[],\"OUTPUTS\":[]}");
    assertEquals(expected, result);
  }

  @Test
  public void testOutputPlanVectorizationJsonShouldMatch() throws Exception {
    QueryState qs = mock(QueryState.class);
    when(qs.getHiveOperation()).thenReturn(HiveOperation.EXPLAIN);
    HiveConf hiveConf = new HiveConf();
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    when(qs.getConf()).thenReturn(hiveConf);
    uut.queryState = qs;

    JsonNode result = objectMapper.readTree(uut.outputPlanVectorization(null, true).getRight().toString());
    JsonNode expected = objectMapper.readTree("{\"enabled\":true,\"enabledConditionsMet\":[\"hive.vectorized.execution.enabled IS true\"]}");
    assertEquals(expected, result);


    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, false);
    result = objectMapper.readTree(uut.outputPlanVectorization(null, true).getRight().toString());
    expected = objectMapper.readTree("{\"enabled\":false,\"enabledConditionsNotMet\":[\"hive.vectorized.execution.enabled IS false\"]}");
    assertEquals(expected, result);

  }

  private TezWork.Dependency mockTezWorkDependency() {
    TezWork.Dependency dep = mock(TezWork.Dependency.class);
    when(dep.getName()).thenReturn("name");
    when(dep.toString()).thenReturn("mock-tez-dependency");
    return dep;
  }

  private SparkWork.Dependency mockSparkWorkDependency() {
    SparkWork.Dependency dep = mock(SparkWork.Dependency.class);
    when(dep.getName()).thenReturn("mock-name");
    when(dep.toString()).thenReturn("mock-spark-dependency");
    return dep;
  }

  private ExplainWork mockExplainWork() {
    ExplainWork explainWork = mock(ExplainWork.class);

    // Should produce JSON
    when(explainWork.isFormatted()).thenReturn(true);

    // Should have some AST
    // when(explainWork.getAstStringTree()).thenReturn(AST);

    when(explainWork.getAnalyzer()).thenReturn(mock(BaseSemanticAnalyzer.class));

    return explainWork;
  }

  private Task<ExplainTask> mockTask() {
    Task<ExplainTask> task = mock(Task.class);

    // Explain type
    when(task.getType()).thenReturn(StageType.EXPLAIN);

    // This is a root task
    when(task.isRootTask()).thenReturn(true);

    // Set up backup task
    Task backupTask = mock(Task.class);
    when(backupTask.getId()).thenReturn(BACKUP_ID);
    when(task.getBackupTask()).thenReturn(backupTask);

    return task;
  }

}
