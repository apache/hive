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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.vector.VectorSelectOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.io.IOContextMap;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.ConvertJoinMapJoin;
import org.apache.hadoop.hive.ql.optimizer.physical.LlapClusterStateForCompile;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.type.ExprNodeTypeCheck;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.CollectDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ScriptDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.VectorSelectDesc;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.junit.Assert;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import org.junit.Before;
import org.junit.Test;

/**
 * TestOperators.
 *
 */
public class TestOperators {

  // this is our row to test expressions on
  protected InspectableObject[] r;

  @Before
  public void setUp() {
    r = new InspectableObject[5];
    ArrayList<String> names = new ArrayList<String>(3);
    names.add("col0");
    names.add("col1");
    names.add("col2");
    ArrayList<ObjectInspector> objectInspectors = new ArrayList<ObjectInspector>(
        3);
    objectInspectors
        .add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    objectInspectors
        .add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    objectInspectors
        .add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    for (int i = 0; i < 5; i++) {
      ArrayList<String> data = new ArrayList<String>();
      data.add("" + i);
      data.add("" + (i + 1));
      data.add("" + (i + 2));
      try {
        r[i] = new InspectableObject();
        r[i].o = data;
        r[i].oi = ObjectInspectorFactory.getStandardStructObjectInspector(
            names, objectInspectors);
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void testTaskIds(String[] taskIds, String expectedAttemptId, String expectedTaskId) {
    Configuration conf = new JobConf(TestOperators.class);
    for (String one: taskIds) {
      conf.set("mapred.task.id", one);
      String attemptId = Utilities.getTaskId(conf);
      assertEquals(expectedAttemptId, attemptId);
      assertEquals(Utilities.getTaskIdFromFilename(attemptId), expectedTaskId);
      assertEquals(Utilities.getTaskIdFromFilename(attemptId + ".gz"), expectedTaskId);
      assertEquals(Utilities.getTaskIdFromFilename(
                   Utilities.toTempPath(new Path(attemptId + ".gz")).toString()), expectedTaskId);
    }
  }

  /**
   * More stuff needs to be added here. Currently it only checks some basic
   * file naming libraries
   * The old test was deactivated as part of hive-405
   */
  @Test
  public void testFileSinkOperator() throws Throwable {

    try {
      testTaskIds(new String[] {
          "attempt_200707121733_0003_m_000005_0",
          "attempt_local_0001_m_000005_0",
          "task_200709221812_0001_m_000005_0",
          "task_local_0001_m_000005_0"
          }, "000005_0", "000005");

      testTaskIds(new String[] {
          "job_local_0001_map_000005",
          "job_local_0001_reduce_000005",
          }, "000005", "000005");

      testTaskIds(new String[] {"1234567"},
                   "1234567", "1234567");

      assertEquals(Utilities.getTaskIdFromFilename(
                   "/mnt/dev005/task_local_0001_m_000005_0"),
                   "000005");

      System.out.println("FileSink Operator ok");
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  /**
   *  When ScriptOperator runs external script, it passes job configuration as environment
   *  variables. But environment variables have some system limitations and we have to check
   *  job configuration properties firstly. This test checks that staff.
   */
  @Test
  public void testScriptOperatorEnvVarsProcessing() throws Throwable {
    try {
      ScriptOperator scriptOperator = new ScriptOperator(new CompilationOpContext());

      //Environment Variables name
      assertEquals("a_b_c", scriptOperator.safeEnvVarName("a.b.c"));
      assertEquals("a_b_c", scriptOperator.safeEnvVarName("a-b-c"));

      //Environment Variables short values
      assertEquals("value", scriptOperator.safeEnvVarValue("value", "name", false));
      assertEquals("value", scriptOperator.safeEnvVarValue("value", "name", true));

      //Environment Variables long values
      char[] array = new char[20*1024+1];
      Arrays.fill(array, 'a');
      String hugeEnvVar = new String(array);
      assertEquals(20*1024+1, hugeEnvVar.length());
      assertEquals(20*1024+1, scriptOperator.safeEnvVarValue(hugeEnvVar, "name", false).length());
      assertEquals(20*1024, scriptOperator.safeEnvVarValue(hugeEnvVar, "name", true).length());

      //Full test
      Configuration hconf = new JobConf(ScriptOperator.class);
      hconf.set("name", hugeEnvVar);
      Map<String, String> env = new HashMap<String, String>();

      HiveConf.setBoolVar(hconf, HiveConf.ConfVars.HIVE_SCRIPT_TRUNCATE_ENV, false);
      scriptOperator.addJobConfToEnvironment(hconf, env);
      assertEquals(20*1024+1, env.get("name").length());

      HiveConf.setBoolVar(hconf, HiveConf.ConfVars.HIVE_SCRIPT_TRUNCATE_ENV, true);
      scriptOperator.addJobConfToEnvironment(hconf, env);
      assertEquals(20*1024, env.get("name").length());

      System.out.println("Script Operator Environment Variables processing ok");
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  public void testScriptOperatorBlacklistedEnvVarsProcessing() {
    ScriptOperator scriptOperator = new ScriptOperator(new CompilationOpContext());

    Configuration hconf = new JobConf(ScriptOperator.class);

    Map<String, String> env = new HashMap<String, String>();

    HiveConf.setVar(hconf, HiveConf.ConfVars.HIVE_SCRIPT_ENV_BLACKLIST, "foobar");
    hconf.set("foobar", "foobar");
    hconf.set("barfoo", "barfoo");
    scriptOperator.addJobConfToEnvironment(hconf, env);
    Assert.assertFalse(env.containsKey("foobar"));
    Assert.assertTrue(env.containsKey("barfoo"));
  }

  @Test
  public void testScriptOperator() throws Throwable {
    try {
      System.out.println("Testing Script Operator");
      // col1
      ExprNodeDesc exprDesc1 = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "col1", "",
          false);
      // col2
      ExprNodeDesc expr1 = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "col0", "",
          false);
      ExprNodeDesc expr2 = new ExprNodeConstantDesc("1");
      ExprNodeDesc exprDesc2 = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
          .getFuncExprNodeDesc("concat", expr1, expr2);

      // select operator to project these two columns
      ArrayList<ExprNodeDesc> earr = new ArrayList<ExprNodeDesc>();
      earr.add(exprDesc1);
      earr.add(exprDesc2);
      ArrayList<String> outputCols = new ArrayList<String>();
      for (int i = 0; i < earr.size(); i++) {
        outputCols.add("_col" + i);
      }
      SelectDesc selectCtx = new SelectDesc(earr, outputCols);
      Operator<SelectDesc> op = OperatorFactory.get(new CompilationOpContext(), SelectDesc.class);
      op.setConf(selectCtx);

      // scriptOperator to echo the output of the select
      TableDesc scriptOutput = PlanUtils.getDefaultTableDesc(""
          + Utilities.tabCode, "a,b");
      TableDesc scriptInput = PlanUtils.getDefaultTableDesc(""
          + Utilities.tabCode, "a,b");
      ScriptDesc sd = new ScriptDesc("cat", scriptOutput,
                                     TextRecordWriter.class, scriptInput,
                                     TextRecordReader.class, TextRecordReader.class,
                                     PlanUtils.getDefaultTableDesc("" + Utilities.tabCode, "key"));
      Operator<ScriptDesc> sop = OperatorFactory.getAndMakeChild(sd, op);

      // Collect operator to observe the output of the script
      CollectDesc cd = new CollectDesc(Integer.valueOf(10));
      CollectOperator cdop = (CollectOperator) OperatorFactory.getAndMakeChild(cd, sop);

      op.initialize(new JobConf(TestOperators.class),
          new ObjectInspector[]{r[0].oi});

      // evaluate on row
      for (int i = 0; i < 5; i++) {
        op.process(r[i].o, 0);
      }
      op.close(false);

      InspectableObject io = new InspectableObject();
      for (int i = 0; i < 5; i++) {
        cdop.retrieve(io);
        System.out.println("[" + i + "] io.o=" + io.o);
        System.out.println("[" + i + "] io.oi=" + io.oi);
        StructObjectInspector soi = (StructObjectInspector) io.oi;
        assert (soi != null);
        StructField a = soi.getStructFieldRef("a");
        StructField b = soi.getStructFieldRef("b");
        assertEquals("" + (i + 1), ((PrimitiveObjectInspector) a
            .getFieldObjectInspector()).getPrimitiveJavaObject(soi
            .getStructFieldData(io.o, a)));
        assertEquals((i) + "1", ((PrimitiveObjectInspector) b
            .getFieldObjectInspector()).getPrimitiveJavaObject(soi
            .getStructFieldData(io.o, b)));
      }

      System.out.println("Script Operator ok");

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  public void testMapOperator() throws Throwable {
    try {
      System.out.println("Testing Map Operator");
      // initialize configuration
      JobConf hconf = new JobConf(TestOperators.class);
      hconf.set(MRJobConfig.MAP_INPUT_FILE, "hdfs:///testDir/testFile");
      IOContextMap.get(hconf).setInputPath(
          new Path("hdfs:///testDir/testFile"));

      // initialize pathToAliases
      List<String> aliases = new ArrayList<String>();
      aliases.add("a");
      aliases.add("b");
      Map<Path, List<String>> pathToAliases = new LinkedHashMap<>();
      pathToAliases.put(new Path("hdfs:///testDir"), aliases);

      // initialize pathToTableInfo
      // Default: treat the table as a single column "col"
      TableDesc td = Utilities.defaultTd;
      PartitionDesc pd = new PartitionDesc(td, null);
      LinkedHashMap<Path, org.apache.hadoop.hive.ql.plan.PartitionDesc> pathToPartitionInfo =
          new LinkedHashMap<>();
      pathToPartitionInfo.put(new Path("hdfs:///testDir"), pd);

      // initialize aliasToWork
      CompilationOpContext ctx = new CompilationOpContext();
      CollectDesc cd = new CollectDesc(Integer.valueOf(1));
      CollectOperator cdop1 = (CollectOperator) OperatorFactory
          .get(ctx, CollectDesc.class);
      cdop1.setConf(cd);
      CollectOperator cdop2 = (CollectOperator) OperatorFactory
          .get(ctx, CollectDesc.class);
      cdop2.setConf(cd);
      LinkedHashMap<String, Operator<? extends OperatorDesc>> aliasToWork =
          new LinkedHashMap<String, Operator<? extends OperatorDesc>>();
      aliasToWork.put("a", cdop1);
      aliasToWork.put("b", cdop2);

      // initialize mapredWork
      MapredWork mrwork = new MapredWork();
      mrwork.getMapWork().setPathToAliases(pathToAliases);
      mrwork.getMapWork().setPathToPartitionInfo(pathToPartitionInfo);
      mrwork.getMapWork().setAliasToWork(aliasToWork);

      // get map operator and initialize it
      MapOperator mo = new MapOperator(new CompilationOpContext());
      mo.initializeAsRoot(hconf, mrwork.getMapWork());

      Text tw = new Text();
      InspectableObject io1 = new InspectableObject();
      InspectableObject io2 = new InspectableObject();
      for (int i = 0; i < 5; i++) {
        String answer = "[[" + i + ", " + (i + 1) + ", " + (i + 2) + "]]";

        tw.set("" + i + "\u0001" + (i + 1) + "\u0001" + (i + 2));
        mo.process(tw);
        cdop1.retrieve(io1);
        cdop2.retrieve(io2);
        System.out.println("io1.o.toString() = " + io1.o.toString());
        System.out.println("io2.o.toString() = " + io2.o.toString());
        System.out.println("answer.toString() = " + answer.toString());
        assertEquals(answer.toString(), io1.o.toString());
        assertEquals(answer.toString(), io2.o.toString());
      }

      System.out.println("Map Operator ok");

    } catch (Throwable e) {
      e.printStackTrace();
      throw (e);
    }
  }

  @Test
  public void testFetchOperatorContextQuoting() throws Exception {
    JobConf conf = new JobConf();
    ArrayList<Path> list = new ArrayList<Path>();
    list.add(new Path("hdfs://nn.example.com/fi\tl\\e\t1"));
    list.add(new Path("hdfs://nn.example.com/file\t2"));
    list.add(new Path("file:/file3"));
    FetchOperator.setFetchOperatorContext(conf, list);
    String[] parts =
        conf.get(FetchOperator.FETCH_OPERATOR_DIRECTORY_LIST).split("\t");
    assertEquals(3, parts.length);
    assertEquals("hdfs://nn.example.com/fi\\tl\\\\e\\t1", parts[0]);
    assertEquals("hdfs://nn.example.com/file\\t2", parts[1]);
    assertEquals("file:/file3", parts[2]);
  }

  /**
   * A custom input format that checks to make sure that the fetch operator
   * sets the required attributes.
   */
  public static class CustomInFmt extends TextInputFormat {

    @Override
    public InputSplit[] getSplits(JobConf job, int splits) throws IOException {

      // ensure that the table properties were copied
      assertEquals("val1", job.get("myprop1"));
      assertEquals("val2", job.get("myprop2"));

      // ensure that both of the partitions are in the complete list.
      String[] dirs = job.get("hive.complete.dir.list").split("\t");
      assertEquals(2, dirs.length);
      Arrays.sort(dirs);
      assertEquals(true, dirs[0].endsWith("/state=CA"));
      assertEquals(true, dirs[1].endsWith("/state=OR"));
      return super.getSplits(job, splits);
    }
  }

  @Test
  public void testFetchOperatorContext() throws Exception {
    HiveConf conf = new HiveConf();
    conf.set("hive.support.concurrency", "false");
    conf.setVar(HiveConf.ConfVars.HIVE_MAPRED_MODE, "nonstrict");
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    SessionState.start(conf);
    String cmd = "create table fetchOp (id int, name string) " +
        "partitioned by (state string) " +
        "row format delimited fields terminated by '|' " +
        "stored as " +
        "inputformat 'org.apache.hadoop.hive.ql.exec.TestOperators$CustomInFmt' " +
        "outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' " +
        "tblproperties ('myprop1'='val1', 'myprop2' = 'val2')";
    Driver driver = new Driver(conf);
    CommandProcessorResponse response = driver.run(cmd);
    List<Object> result = new ArrayList<Object>();

    cmd = "load data local inpath '../data/files/employee.dat' " +
        "overwrite into table fetchOp partition (state='CA')";
    response = driver.run(cmd);

    cmd = "load data local inpath '../data/files/employee2.dat' " +
        "overwrite into table fetchOp partition (state='OR')";
    response = driver.run(cmd);

    cmd = "select * from fetchOp";
    driver.setMaxRows(500);
    response = driver.run(cmd);
    driver.getResults(result);
    assertEquals(20, result.size());
    driver.close();
  }

  @Test
  public void testNoConditionalTaskSizeForLlap() {
    ConvertJoinMapJoin convertJoinMapJoin = new ConvertJoinMapJoin();
    long defaultNoConditionalTaskSize = 1024L * 1024L * 1024L;
    HiveConf hiveConf = new HiveConf();
    hiveConf.setLongVar(HiveConf.ConfVars.HIVE_CONVERT_JOIN_NOCONDITIONAL_TASK_THRESHOLD, defaultNoConditionalTaskSize);

    LlapClusterStateForCompile llapInfo = null;
    if ("llap".equalsIgnoreCase(hiveConf.getVar(HiveConf.ConfVars.HIVE_EXECUTION_MODE))) {
      llapInfo = LlapClusterStateForCompile.getClusterInfo(hiveConf);
      llapInfo.initClusterInfo();
    }
    // execution mode not set, null is returned
    assertEquals(defaultNoConditionalTaskSize,
        convertJoinMapJoin.getMemoryMonitorInfo(hiveConf, llapInfo).getAdjustedNoConditionalTaskSize());
    hiveConf.set(HiveConf.ConfVars.HIVE_EXECUTION_MODE.varname, "llap");

    if ("llap".equalsIgnoreCase(hiveConf.getVar(HiveConf.ConfVars.HIVE_EXECUTION_MODE))) {
      llapInfo = LlapClusterStateForCompile.getClusterInfo(hiveConf);
      llapInfo.initClusterInfo();
    }

    // default executors is 4, max slots is 3. so 3 * 20% of noconditional task size will be oversubscribed
    hiveConf.set(HiveConf.ConfVars.LLAP_MAPJOIN_MEMORY_OVERSUBSCRIBE_FACTOR.varname, "0.2");
    hiveConf.set(HiveConf.ConfVars.LLAP_MEMORY_OVERSUBSCRIPTION_MAX_EXECUTORS_PER_QUERY.varname, "3");
    double fraction = hiveConf.getFloatVar(HiveConf.ConfVars.LLAP_MAPJOIN_MEMORY_OVERSUBSCRIBE_FACTOR);
    int maxSlots = 3;
    long expectedSize = (long) (defaultNoConditionalTaskSize + (defaultNoConditionalTaskSize * fraction * maxSlots));
    assertEquals(expectedSize,
        convertJoinMapJoin.getMemoryMonitorInfo(hiveConf, llapInfo)
        .getAdjustedNoConditionalTaskSize());

    // num executors is less than max executors per query (which is not expected case), default executors will be
    // chosen. 4 * 20% of noconditional task size will be oversubscribed
    int chosenSlots = hiveConf.getIntVar(HiveConf.ConfVars.LLAP_DAEMON_NUM_EXECUTORS);
    hiveConf.set(HiveConf.ConfVars.LLAP_MEMORY_OVERSUBSCRIPTION_MAX_EXECUTORS_PER_QUERY.varname, "5");
    expectedSize = (long) (defaultNoConditionalTaskSize + (defaultNoConditionalTaskSize * fraction * chosenSlots));
    assertEquals(expectedSize,
        convertJoinMapJoin.getMemoryMonitorInfo(hiveConf, llapInfo)
        .getAdjustedNoConditionalTaskSize());

    // disable memory checking
    hiveConf.set(HiveConf.ConfVars.LLAP_MAPJOIN_MEMORY_MONITOR_CHECK_INTERVAL.varname, "0");
    assertFalse(
        convertJoinMapJoin.getMemoryMonitorInfo(hiveConf, llapInfo).doMemoryMonitoring());

    // invalid inflation factor
    hiveConf.set(HiveConf.ConfVars.LLAP_MAPJOIN_MEMORY_MONITOR_CHECK_INTERVAL.varname, "10000");
    hiveConf.set(HiveConf.ConfVars.HIVE_HASH_TABLE_INFLATION_FACTOR.varname, "0.0f");
    assertFalse(
        convertJoinMapJoin.getMemoryMonitorInfo(hiveConf, llapInfo).doMemoryMonitoring());
  }

  @Test
  public void testLlapMemoryOversubscriptionMaxExecutorsPerQueryCalculation() {
    ConvertJoinMapJoin convertJoinMapJoin = new ConvertJoinMapJoin();
    HiveConf hiveConf = new HiveConf();

    LlapClusterStateForCompile llapInfo = Mockito.mock(LlapClusterStateForCompile.class);

    when(llapInfo.getNumExecutorsPerNode()).thenReturn(1);
    assertEquals(1,
        convertJoinMapJoin.getMemoryMonitorInfo(hiveConf, llapInfo).getMaxExecutorsOverSubscribeMemory());
    assertEquals(3,
        convertJoinMapJoin.getMemoryMonitorInfo(hiveConf, null).getMaxExecutorsOverSubscribeMemory());

    when(llapInfo.getNumExecutorsPerNode()).thenReturn(6);
    assertEquals(2,
        convertJoinMapJoin.getMemoryMonitorInfo(hiveConf, llapInfo).getMaxExecutorsOverSubscribeMemory());

    when(llapInfo.getNumExecutorsPerNode()).thenReturn(30);
    assertEquals(8,
        convertJoinMapJoin.getMemoryMonitorInfo(hiveConf, llapInfo).getMaxExecutorsOverSubscribeMemory());

    hiveConf.set(HiveConf.ConfVars.LLAP_MEMORY_OVERSUBSCRIPTION_MAX_EXECUTORS_PER_QUERY.varname, "5");
    assertEquals(5,
        convertJoinMapJoin.getMemoryMonitorInfo(hiveConf, llapInfo).getMaxExecutorsOverSubscribeMemory());
    assertEquals(5,
        convertJoinMapJoin.getMemoryMonitorInfo(hiveConf, null).getMaxExecutorsOverSubscribeMemory());
  }

  @Test public void testHashGroupBy() throws HiveException {
    InspectableObject[] input = constructHashAggrInputData(5, 3);
    System.out.println("---------------Begin to Construct Groupby Desc-------------");
    // 1. Build AggregationDesc
    String aggregate = "MAX";
    ExprNodeDesc inputColumn = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "col0", "table", false);
    ArrayList<ExprNodeDesc> params = new ArrayList<ExprNodeDesc>();
    params.add(inputColumn);
    GenericUDAFEvaluator genericUDAFEvaluator =
        SemanticAnalyzer.getGenericUDAFEvaluator(aggregate, params, null, false, false);
    AggregationDesc agg =
        new AggregationDesc(aggregate, genericUDAFEvaluator, params, false, GenericUDAFEvaluator.Mode.PARTIAL1);
    ArrayList<AggregationDesc> aggs = new ArrayList<AggregationDesc>();
    aggs.add(agg);

    // 2. aggr keys
    ExprNodeDesc key1 = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "col1", "table", false);
    ExprNodeDesc key2 = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "col2", "table", false);
    ArrayList<ExprNodeDesc> keys = new ArrayList<>();
    keys.add(key1);
    keys.add(key2);

    // 3. outputCols
    // @see org.apache.hadoop.hive.ql.exec.GroupByOperator.forward
    // outputColumnNames, including: group by keys, agg evaluators output cols.
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    for (int i = 0; i < keys.size() + aggs.size(); i++) {
      outputColumnNames.add("_col" + i);
    }
    // 4. build GroupByDesc desc
    GroupByDesc desc = new GroupByDesc();
    desc.setOutputColumnNames(outputColumnNames);
    desc.setAggregators(aggs);
    desc.setKeys(keys);
    desc.setMode(GroupByDesc.Mode.HASH);
    desc.setMemoryThreshold(1.0f);
    desc.setGroupByMemoryUsage(1.0f);
    // minReductionHashAggr
    desc.setMinReductionHashAggr(0.5f);

    // 5. Configure hive conf and  Build group by operator
    HiveConf hconf = new HiveConf();
    HiveConf.setIntVar(hconf, HiveConf.ConfVars.HIVE_GROUPBY_MAP_INTERVAL, 1);

    // 6. test hash aggr without grouping sets
    System.out.println("---------------Begin to test hash group by without grouping sets-------------");
    int withoutGroupingSetsExpectSize = 3;
    GroupByOperator op = new GroupByOperator(new CompilationOpContext());
    op.setConf(desc);
    testHashAggr(op, hconf, input, withoutGroupingSetsExpectSize);

    // 7. test hash aggr with  grouping sets
    System.out.println("---------------Begin to test hash group by with grouping sets------------");
    int groupingSetsExpectSize = 6;

    desc.setGroupingSetsPresent(true);
    ArrayList<Long> groupingSets = new ArrayList<>();
    // groupingSets
    groupingSets.add(1L);
    groupingSets.add(2L);
    desc.setListGroupingSets(groupingSets);
    // add grouping sets dummy key
    ExprNodeDesc groupingSetDummyKey = new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, 0L);
    keys.add(groupingSetDummyKey);
    desc.setKeys(keys);
    // groupingSet Position
    desc.setGroupingSetPosition(2);
    op = new GroupByOperator(new CompilationOpContext());
    op.setConf(desc);
    testHashAggr(op, hconf, input, groupingSetsExpectSize);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testSetDoneFromChildOperators() throws HiveException {
    VectorSelectDesc vectorSelectDesc = new VectorSelectDesc();
    vectorSelectDesc.setProjectedOutputColumns(new int[0]);
    vectorSelectDesc.setSelectExpressions(new VectorExpression[0]);
    VectorSelectOperator selOp = new VectorSelectOperator(new CompilationOpContext(),
        new SelectDesc(), new VectorizationContext("dummy"), vectorSelectDesc);
    VectorSelectOperator childOp = new VectorSelectOperator(new CompilationOpContext(),
        new SelectDesc(), new VectorizationContext("dummy"), vectorSelectDesc);

    selOp.childOperatorsArray = new Operator[1];
    selOp.childOperatorsArray[0] = childOp;
    selOp.childOperatorsTag = new int[1];
    selOp.childOperatorsTag[0] = 0;

    childOp.childOperatorsArray = new Operator[0];

    Assert.assertFalse(selOp.getDone());
    Assert.assertFalse(childOp.getDone());

    selOp.process(new VectorizedRowBatch(1), 0);
    childOp.setDone(true);

    // selOp is not done, it will detect child's done=true during the next process(batch) call
    Assert.assertFalse(selOp.getDone());
    Assert.assertTrue(childOp.getDone());

    selOp.process(new VectorizedRowBatch(1), 0);

    // selOp detects child's done=true, so it turns to done=true
    Assert.assertTrue(selOp.getDone());
    Assert.assertTrue(childOp.getDone());
  }

  private void testHashAggr(GroupByOperator op, HiveConf hconf, InspectableObject[] r, int expectOutputSize)
      throws HiveException {
    // 1. Collect operator to observe the output of the group by operator
    CollectDesc cd = new CollectDesc(expectOutputSize + 10);
    CollectOperator cdop = (CollectOperator) OperatorFactory.getAndMakeChild(cd, op);
    op.initialize(hconf, new ObjectInspector[] { r[0].oi });
    // 2. Evaluate on rows and check hashAggr flag
    for (int i = 0; i < r.length; i++) {
      op.process(r[i].o, 0);
    }
    op.close(false);
    InspectableObject io = new InspectableObject();
    int output = 0;
    // 3. Print group by results
    do {
      cdop.retrieve(io);
      if (io.o != null) {
        System.out.println("io.o = " + io.o);
        output++;
      }
    } while (io.o != null);
    // 4. Check partial result size
    assertEquals(expectOutputSize, output);
  }

  private InspectableObject[] constructHashAggrInputData(int rowNum, int rowNumWithSameKeys) {
    InspectableObject[] r;
    r = new InspectableObject[rowNum];
    ArrayList<String> names = new ArrayList<String>(3);
    names.add("col0");
    names.add("col1");
    names.add("col2");
    ArrayList<ObjectInspector> objectInspectors = new ArrayList<ObjectInspector>(3);
    objectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    objectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    objectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    // 3 rows with the same col1, col2
    for (int i = 0; i < rowNum; i++) {
      ArrayList<String> data = new ArrayList<String>();
      data.add("" + i);
      data.add("" + (i < rowNumWithSameKeys ? -1 : i));
      data.add("" + (i < rowNumWithSameKeys ? -1 : i));
      try {
        r[i] = new InspectableObject();
        r[i].o = data;
        r[i].oi = ObjectInspectorFactory.getStandardStructObjectInspector(names, objectInspectors);
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    }
    return r;
  }

}
