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

import java.util.ArrayList;



import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.type.ExprNodeTypeCheck;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

/**
 * TestExpressionEvaluator.
 *
 */
public class TestExpressionEvaluator {

  // this is our row to test expressions on
  protected InspectableObject r;

  ArrayList<Text> col1;
  TypeInfo col1Type;
  ArrayList<Text> cola;
  TypeInfo colaType;
  ArrayList<Object> data;
  ArrayList<String> names;
  ArrayList<TypeInfo> typeInfos;
  TypeInfo dataType;

  public TestExpressionEvaluator() {
    // Arithmetic operations rely on getting conf from SessionState, need to initialize here.
    SessionState ss = new SessionState(new HiveConf());
    SessionState.setCurrentSessionState(ss);

    col1 = new ArrayList<Text>();
    col1.add(new Text("0"));
    col1.add(new Text("1"));
    col1.add(new Text("2"));
    col1.add(new Text("3"));
    col1Type = TypeInfoFactory.getListTypeInfo(TypeInfoFactory.stringTypeInfo);
    cola = new ArrayList<Text>();
    cola.add(new Text("a"));
    cola.add(new Text("b"));
    cola.add(new Text("c"));
    colaType = TypeInfoFactory.getListTypeInfo(TypeInfoFactory.stringTypeInfo);
    try {
      data = new ArrayList<Object>();
      data.add(col1);
      data.add(cola);
      names = new ArrayList<String>();
      names.add("col1");
      names.add("cola");
      typeInfos = new ArrayList<TypeInfo>();
      typeInfos.add(col1Type);
      typeInfos.add(colaType);
      dataType = TypeInfoFactory.getStructTypeInfo(names, typeInfos);

      r = new InspectableObject();
      r.o = data;
      r.oi = TypeInfoUtils
          .getStandardWritableObjectInspectorFromTypeInfo(dataType);
    } catch (Throwable e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Before
  public void setUp() {
  }

  @Test
  public void testExprNodeColumnEvaluator() throws Throwable {
    try {
      // get a evaluator for a simple field expression
      ExprNodeDesc exprDesc = new ExprNodeColumnDesc(colaType, "cola", "",
          false);
      ExprNodeEvaluator eval = ExprNodeEvaluatorFactory.get(exprDesc);

      // evaluate on row
      ObjectInspector resultOI = eval.initialize(r.oi);
      Object resultO = eval.evaluate(r.o);

      Object standardResult = ObjectInspectorUtils.copyToStandardObject(
          resultO, resultOI, ObjectInspectorCopyOption.WRITABLE);
      assertEquals(cola, standardResult);
      System.out.println("ExprNodeColumnEvaluator ok");
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  private static ExprNodeDesc getListIndexNode(ExprNodeDesc node, int index) throws Exception {
    return getListIndexNode(node, new ExprNodeConstantDesc(index));
  }

  private static ExprNodeDesc getListIndexNode(ExprNodeDesc node,
      ExprNodeDesc index) throws Exception {
    ArrayList<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>(2);
    children.add(node);
    children.add(index);
    return new ExprNodeGenericFuncDesc(((ListTypeInfo) node.getTypeInfo())
        .getListElementTypeInfo(), FunctionRegistry.getGenericUDFForIndex(),
        children);
  }

  @Test
  public void testExprNodeFuncEvaluator() throws Throwable {
    try {
      // get a evaluator for a string concatenation expression
      ExprNodeDesc col1desc = new ExprNodeColumnDesc(col1Type, "col1", "",
          false);
      ExprNodeDesc coladesc = new ExprNodeColumnDesc(colaType, "cola", "",
          false);
      ExprNodeDesc col11desc = getListIndexNode(col1desc, 1);
      ExprNodeDesc cola0desc = getListIndexNode(coladesc, 0);
      ExprNodeDesc func1 = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
          .getFuncExprNodeDesc("concat", col11desc, cola0desc);
      ExprNodeEvaluator eval = ExprNodeEvaluatorFactory.get(func1);

      // evaluate on row
      ObjectInspector resultOI = eval.initialize(r.oi);
      Object resultO = eval.evaluate(r.o);
      assertEquals("1a", ObjectInspectorUtils.copyToStandardObject(resultO,
          resultOI, ObjectInspectorCopyOption.JAVA));
      System.out.println("ExprNodeFuncEvaluator ok");
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  public void testExprNodeConversionEvaluator() throws Throwable {
    try {
      // get a evaluator for a string concatenation expression
      ExprNodeDesc col1desc = new ExprNodeColumnDesc(col1Type, "col1", "",
          false);
      ExprNodeDesc col11desc = getListIndexNode(col1desc, 1);
      ExprNodeDesc func1 = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
          .getFuncExprNodeDesc(serdeConstants.DOUBLE_TYPE_NAME, col11desc);
      ExprNodeEvaluator eval = ExprNodeEvaluatorFactory.get(func1);

      // evaluate on row
      ObjectInspector resultOI = eval.initialize(r.oi);
      Object resultO = eval.evaluate(r.o);
      assertEquals(Double.valueOf("1"), ObjectInspectorUtils
          .copyToStandardObject(resultO, resultOI,
          ObjectInspectorCopyOption.JAVA));
      System.out.println("testExprNodeConversionEvaluator ok");
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  private static void measureSpeed(String expr, int times,
      ExprNodeEvaluator eval, InspectableObject input, Object standardJavaOutput)
      throws HiveException {
    System.out.println("Evaluating " + expr + " for " + times + " times");
    new InspectableObject();
    ObjectInspector resultOI = eval.initialize(input.oi);
    Object resultO = null;
    long start = System.currentTimeMillis();
    for (int i = 0; i < times; i++) {
      resultO = eval.evaluate(input.o);
    }
    long end = System.currentTimeMillis();
    assertEquals(standardJavaOutput, ObjectInspectorUtils.copyToStandardObject(
        resultO, resultOI, ObjectInspectorCopyOption.JAVA));
    System.out.println("Evaluation finished: "
        + String.format("%2.3f", (end - start) * 0.001) + " seconds, "
        + String.format("%2.3f", (end - start) * 1000.0 / times)
        + " seconds/million call.");
  }

  @Test
  public void testExprNodeSpeed() throws Throwable {
    try {
      int basetimes = 100000;
      measureSpeed("1 + 2", basetimes * 100, ExprNodeEvaluatorFactory
          .get(ExprNodeTypeCheck.getExprNodeDefaultExprProcessor().getFuncExprNodeDesc(
          "+", new ExprNodeConstantDesc(1), new ExprNodeConstantDesc(2))),
          r, Integer.valueOf(1 + 2));
      measureSpeed("1 + 2 - 3", basetimes * 100, ExprNodeEvaluatorFactory
          .get(ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
          .getFuncExprNodeDesc("-",
          ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
          .getFuncExprNodeDesc("+", new ExprNodeConstantDesc(1),
          new ExprNodeConstantDesc(2)),
          new ExprNodeConstantDesc(3))), r, Integer.valueOf(1 + 2 - 3));
      measureSpeed("1 + 2 - 3 + 4", basetimes * 100, ExprNodeEvaluatorFactory
          .get(ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
          .getFuncExprNodeDesc("+",
          ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
          .getFuncExprNodeDesc("-",
          ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
          .getFuncExprNodeDesc("+",
          new ExprNodeConstantDesc(1),
          new ExprNodeConstantDesc(2)),
          new ExprNodeConstantDesc(3)),
          new ExprNodeConstantDesc(4))), r, Integer
          .valueOf(1 + 2 - 3 + 4));
      measureSpeed("concat(\"1\", \"2\")", basetimes * 100,
          ExprNodeEvaluatorFactory
          .get(ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
          .getFuncExprNodeDesc("concat", new ExprNodeConstantDesc("1"),
          new ExprNodeConstantDesc("2"))), r, "12");
      measureSpeed("concat(concat(\"1\", \"2\"), \"3\")", basetimes * 100,
          ExprNodeEvaluatorFactory
          .get(ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
          .getFuncExprNodeDesc("concat",
          ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
          .getFuncExprNodeDesc("concat",
          new ExprNodeConstantDesc("1"),
          new ExprNodeConstantDesc("2")),
          new ExprNodeConstantDesc("3"))), r, "123");
      measureSpeed("concat(concat(concat(\"1\", \"2\"), \"3\"), \"4\")",
          basetimes * 100, ExprNodeEvaluatorFactory
          .get(ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
          .getFuncExprNodeDesc("concat",
          ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
          .getFuncExprNodeDesc("concat",
          ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
          .getFuncExprNodeDesc("concat",
          new ExprNodeConstantDesc("1"),
          new ExprNodeConstantDesc("2")),
          new ExprNodeConstantDesc("3")),
          new ExprNodeConstantDesc("4"))), r, "1234");
      ExprNodeDesc constant1 = new ExprNodeConstantDesc(1);
      ExprNodeDesc constant2 = new ExprNodeConstantDesc(2);
      measureSpeed("concat(col1[1], cola[1])", basetimes * 10,
          ExprNodeEvaluatorFactory
          .get(ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
          .getFuncExprNodeDesc("concat", getListIndexNode(
          new ExprNodeColumnDesc(col1Type, "col1", "", false),
          constant1), getListIndexNode(new ExprNodeColumnDesc(
          colaType, "cola", "", false), constant1))), r, "1b");
      measureSpeed("concat(concat(col1[1], cola[1]), col1[2])", basetimes * 10,
          ExprNodeEvaluatorFactory
          .get(ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
          .getFuncExprNodeDesc("concat",
          ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
          .getFuncExprNodeDesc("concat", getListIndexNode(
          new ExprNodeColumnDesc(col1Type, "col1", "",
          false), constant1), getListIndexNode(
          new ExprNodeColumnDesc(colaType, "cola", "",
          false), constant1)), getListIndexNode(
          new ExprNodeColumnDesc(col1Type, "col1", "", false),
          constant2))), r, "1b2");
      measureSpeed(
          "concat(concat(concat(col1[1], cola[1]), col1[2]), cola[2])",
          basetimes * 10, ExprNodeEvaluatorFactory
          .get(ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
          .getFuncExprNodeDesc("concat",
          ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
          .getFuncExprNodeDesc("concat",
          ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
          .getFuncExprNodeDesc("concat",
          getListIndexNode(new ExprNodeColumnDesc(
          col1Type, "col1", "", false),
          constant1), getListIndexNode(
          new ExprNodeColumnDesc(colaType,
          "cola", "", false), constant1)),
          getListIndexNode(new ExprNodeColumnDesc(col1Type,
          "col1", "", false), constant2)),
          getListIndexNode(new ExprNodeColumnDesc(colaType, "cola",
          "", false), constant2))), r, "1b2c");

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

}
