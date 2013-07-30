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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory;
import org.apache.hadoop.hive.ql.plan.CollectDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ScriptDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

/**
 * TestOperators.
 *
 */
public class TestOperators extends TestCase {

  // this is our row to test expressions on
  protected InspectableObject[] r;

  @Override
  protected void setUp() {
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

  public void testBaseFilterOperator() throws Throwable {
    try {
      System.out.println("Testing Filter Operator");
      ExprNodeDesc col0 = TestExecDriver.getStringColumn("col0");
      ExprNodeDesc col1 = TestExecDriver.getStringColumn("col1");
      ExprNodeDesc col2 = TestExecDriver.getStringColumn("col2");
      ExprNodeDesc zero = new ExprNodeConstantDesc("0");
      ExprNodeDesc func1 = TypeCheckProcFactory.DefaultExprProcessor
          .getFuncExprNodeDesc(">", col2, col1);
      ExprNodeDesc func2 = TypeCheckProcFactory.DefaultExprProcessor
          .getFuncExprNodeDesc("==", col0, zero);
      ExprNodeDesc func3 = TypeCheckProcFactory.DefaultExprProcessor
          .getFuncExprNodeDesc("and", func1, func2);
      assert (func3 != null);
      FilterDesc filterCtx = new FilterDesc(func3, false);

      // Configuration
      Operator<FilterDesc> op = OperatorFactory.get(FilterDesc.class);
      op.setConf(filterCtx);

      // runtime initialization
      op.initialize(new JobConf(TestOperators.class),
          new ObjectInspector[] {r[0].oi});

      for (InspectableObject oner : r) {
        op.process(oner.o, 0);
      }

      Map<Enum<?>, Long> results = op.getStats();
      System.out.println("filtered = "
          + results.get(FilterOperator.Counter.FILTERED));
      assertEquals(Long.valueOf(4), results
          .get(FilterOperator.Counter.FILTERED));
      System.out.println("passed = "
          + results.get(FilterOperator.Counter.PASSED));
      assertEquals(Long.valueOf(1), results.get(FilterOperator.Counter.PASSED));

      /*
       * for(Enum e: results.keySet()) { System.out.println(e.toString() + ":" +
       * results.get(e)); }
       */
      System.out.println("Filter Operator ok");

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  private void testTaskIds(String [] taskIds, String expectedAttemptId, String expectedTaskId) {
    Configuration conf = new JobConf(TestOperators.class);
    for (String one: taskIds) {
      conf.set("mapred.task.id", one);
      String attemptId = Utilities.getTaskId(conf);
      assertEquals(expectedAttemptId, attemptId);
      assertEquals(Utilities.getTaskIdFromFilename(attemptId), expectedTaskId);
      assertEquals(Utilities.getTaskIdFromFilename(attemptId + ".gz"), expectedTaskId);
      assertEquals(Utilities.getTaskIdFromFilename
                   (Utilities.toTempPath(new Path(attemptId + ".gz")).toString()), expectedTaskId);
    }
  }

  /**
   * More stuff needs to be added here. Currently it only checks some basic
   * file naming libraries
   * The old test was deactivated as part of hive-405
   */
  public void testFileSinkOperator() throws Throwable {

    try {
      testTaskIds (new String [] {
          "attempt_200707121733_0003_m_000005_0",
          "attempt_local_0001_m_000005_0",
          "task_200709221812_0001_m_000005_0",
          "task_local_0001_m_000005_0"
        }, "000005_0", "000005");

      testTaskIds (new String [] {
          "job_local_0001_map_000005",
          "job_local_0001_reduce_000005",
        }, "000005", "000005");

      testTaskIds (new String [] {"1234567"},
                   "1234567", "1234567");

      assertEquals(Utilities.getTaskIdFromFilename
                   ("/mnt/dev005/task_local_0001_m_000005_0"),
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
  public void testScriptOperatorEnvVarsProcessing() throws Throwable {
    try {
      ScriptOperator scriptOperator = new ScriptOperator();

      //Environment Variables name
      assertEquals("a_b_c", scriptOperator.safeEnvVarName("a.b.c"));
      assertEquals("a_b_c", scriptOperator.safeEnvVarName("a-b-c"));

      //Environment Variables short values
      assertEquals("value", scriptOperator.safeEnvVarValue("value", "name", false));
      assertEquals("value", scriptOperator.safeEnvVarValue("value", "name", true));

      //Environment Variables long values
      char [] array = new char[20*1024+1];
      Arrays.fill(array, 'a');
      String hugeEnvVar = new String(array);
      assertEquals(20*1024+1, hugeEnvVar.length());
      assertEquals(20*1024+1, scriptOperator.safeEnvVarValue(hugeEnvVar, "name", false).length());
      assertEquals(20*1024, scriptOperator.safeEnvVarValue(hugeEnvVar, "name", true).length());

      //Full test
      Configuration hconf = new JobConf(ScriptOperator.class);
      hconf.set("name", hugeEnvVar);
      Map<String, String> env = new HashMap<String, String>();

      HiveConf.setBoolVar(hconf, HiveConf.ConfVars.HIVESCRIPTTRUNCATEENV, false);
      scriptOperator.addJobConfToEnvironment(hconf, env);
      assertEquals(20*1024+1, env.get("name").length());

      HiveConf.setBoolVar(hconf, HiveConf.ConfVars.HIVESCRIPTTRUNCATEENV, true);
      scriptOperator.addJobConfToEnvironment(hconf, env);
      assertEquals(20*1024, env.get("name").length());

      System.out.println("Script Operator Environment Variables processing ok");
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void testScriptOperator() throws Throwable {
    try {
      System.out.println("Testing Script Operator");
      // col1
      ExprNodeDesc exprDesc1 = TestExecDriver.getStringColumn("col1");

      // col2
      ExprNodeDesc expr1 = TestExecDriver.getStringColumn("col0");
      ExprNodeDesc expr2 = new ExprNodeConstantDesc("1");
      ExprNodeDesc exprDesc2 = TypeCheckProcFactory.DefaultExprProcessor
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
      Operator<SelectDesc> op = OperatorFactory.get(SelectDesc.class);
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
      CollectOperator cdop = (CollectOperator) OperatorFactory.getAndMakeChild(
          cd, sop);

      op.initialize(new JobConf(TestOperators.class),
          new ObjectInspector[] {r[0].oi});

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

  public void testMapOperator() throws Throwable {
    try {
      System.out.println("Testing Map Operator");
      // initialize configuration
      Configuration hconf = new JobConf(TestOperators.class);
      HiveConf.setVar(hconf, HiveConf.ConfVars.HADOOPMAPFILENAME,
          "hdfs:///testDir/testFile");

      // initialize pathToAliases
      ArrayList<String> aliases = new ArrayList<String>();
      aliases.add("a");
      aliases.add("b");
      LinkedHashMap<String, ArrayList<String>> pathToAliases =
        new LinkedHashMap<String, ArrayList<String>>();
      pathToAliases.put("hdfs:///testDir", aliases);

      // initialize pathToTableInfo
      // Default: treat the table as a single column "col"
      TableDesc td = Utilities.defaultTd;
      PartitionDesc pd = new PartitionDesc(td, null);
      LinkedHashMap<String, org.apache.hadoop.hive.ql.plan.PartitionDesc> pathToPartitionInfo =
        new LinkedHashMap<String, org.apache.hadoop.hive.ql.plan.PartitionDesc>();
      pathToPartitionInfo.put("hdfs:///testDir", pd);

      // initialize aliasToWork
      CollectDesc cd = new CollectDesc(Integer.valueOf(1));
      CollectOperator cdop1 = (CollectOperator) OperatorFactory
          .get(CollectDesc.class);
      cdop1.setConf(cd);
      CollectOperator cdop2 = (CollectOperator) OperatorFactory
          .get(CollectDesc.class);
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
      MapOperator mo = new MapOperator();
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
}
