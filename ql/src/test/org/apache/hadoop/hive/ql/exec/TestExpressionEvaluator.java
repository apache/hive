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

import junit.framework.TestCase;
import java.io.*;
import java.util.*;

import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils.ExpressionTypes;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.Text;

public class TestExpressionEvaluator extends TestCase {

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
    col1 = new ArrayList<Text> ();
    col1.add(new Text("0"));
    col1.add(new Text("1"));
    col1.add(new Text("2"));
    col1.add(new Text("3"));
    col1Type = TypeInfoFactory.getListTypeInfo(
        TypeInfoFactory.stringTypeInfo);
    cola = new ArrayList<Text> ();
    cola.add(new Text("a"));
    cola.add(new Text("b"));
    cola.add(new Text("c"));
    colaType = TypeInfoFactory.getListTypeInfo(
        TypeInfoFactory.stringTypeInfo);
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
      r.oi = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(dataType);
    } catch (Throwable e) {
      e.printStackTrace();
      throw new RuntimeException (e);
    }
  }

  protected void setUp() {
  }

  public void testExprNodeColumnEvaluator() throws Throwable {
    try {
      // get a evaluator for a simple field expression
      exprNodeDesc exprDesc = new exprNodeColumnDesc(colaType, "cola", "", false);
      ExprNodeEvaluator eval = ExprNodeEvaluatorFactory.get(exprDesc);

      // evaluate on row
      ObjectInspector resultOI = eval.initialize(r.oi);
      Object resultO = eval.evaluate(r.o);
      
      Object standardResult = ObjectInspectorUtils.copyToStandardObject(resultO, resultOI, ObjectInspectorCopyOption.WRITABLE);   
      assertEquals(cola, standardResult);
      System.out.println("ExprNodeColumnEvaluator ok");
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  private static exprNodeDesc getListIndexNode(exprNodeDesc node, int index) {
    return getListIndexNode(node, new exprNodeConstantDesc(index));
  }
  
  private static exprNodeDesc getListIndexNode(exprNodeDesc node, exprNodeDesc index) {
    ArrayList<exprNodeDesc> children = new ArrayList<exprNodeDesc>(2);
    children.add(node);
    children.add(index);
    return new exprNodeGenericFuncDesc(
          ((ListTypeInfo)node.getTypeInfo()).getListElementTypeInfo(),
          FunctionRegistry.getGenericUDFForIndex(),
          children);
  }
  
  public void testExprNodeFuncEvaluator() throws Throwable {
    try {
      // get a evaluator for a string concatenation expression
      exprNodeDesc col1desc = new exprNodeColumnDesc(col1Type, "col1", "", false);
      exprNodeDesc coladesc = new exprNodeColumnDesc(colaType, "cola", "", false);
      exprNodeDesc col11desc = getListIndexNode(col1desc, 1);
      exprNodeDesc cola0desc = getListIndexNode(coladesc, 0);
      exprNodeDesc func1 = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat", col11desc, cola0desc);
      ExprNodeEvaluator eval = ExprNodeEvaluatorFactory.get(func1);

      // evaluate on row
      ObjectInspector resultOI = eval.initialize(r.oi);
      Object resultO = eval.evaluate(r.o);
      assertEquals("1a",
          ObjectInspectorUtils.copyToStandardObject(resultO, resultOI, ObjectInspectorCopyOption.JAVA));
      System.out.println("ExprNodeFuncEvaluator ok");
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void testExprNodeConversionEvaluator() throws Throwable {
    try {
      // get a evaluator for a string concatenation expression
      exprNodeDesc col1desc = new exprNodeColumnDesc(col1Type, "col1", "", false);
      exprNodeDesc col11desc = getListIndexNode(col1desc, 1);
      exprNodeDesc func1 = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(Constants.DOUBLE_TYPE_NAME, col11desc);
      ExprNodeEvaluator eval = ExprNodeEvaluatorFactory.get(func1);

      // evaluate on row
      ObjectInspector resultOI = eval.initialize(r.oi);
      Object resultO = eval.evaluate(r.o);
      assertEquals(Double.valueOf("1"),
          ObjectInspectorUtils.copyToStandardObject(resultO, resultOI, ObjectInspectorCopyOption.JAVA));
      System.out.println("testExprNodeConversionEvaluator ok");
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  private static void measureSpeed(String expr, int times, ExprNodeEvaluator eval, InspectableObject input, Object standardJavaOutput) throws HiveException {
    System.out.println("Evaluating " + expr + " for " + times + " times");
    // evaluate on row
    InspectableObject output = new InspectableObject(); 
    ObjectInspector resultOI = eval.initialize(input.oi);
    Object resultO = null;
    long start = System.currentTimeMillis();
    for (int i=0; i<times; i++) {
      resultO = eval.evaluate(input.o);
    }
    long end = System.currentTimeMillis();
    assertEquals(standardJavaOutput,
        ObjectInspectorUtils.copyToStandardObject(resultO, resultOI, ObjectInspectorCopyOption.JAVA));
    System.out.println("Evaluation finished: " + String.format("%2.3f", (end - start)*0.001) + " seconds, " 
        + String.format("%2.3f", (end - start)*1000.0/times) + " seconds/million call.");
  }
  
  public void testExprNodeSpeed() throws Throwable {
    try {
      int basetimes = 100000;
      measureSpeed("1 + 2",
          basetimes * 100,
          ExprNodeEvaluatorFactory.get(
              TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", 
                  new exprNodeConstantDesc(1), 
                  new exprNodeConstantDesc(2))),
          r,
          Integer.valueOf(1 + 2));
      measureSpeed("1 + 2 - 3",
          basetimes * 100,
          ExprNodeEvaluatorFactory.get(
              TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("-", 
                  TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+",
                      new exprNodeConstantDesc(1), 
                      new exprNodeConstantDesc(2)),
                  new exprNodeConstantDesc(3))),
          r,
          Integer.valueOf(1 + 2 - 3));
      measureSpeed("1 + 2 - 3 + 4",
          basetimes * 100,
          ExprNodeEvaluatorFactory.get(
              TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+",
                  TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("-", 
                      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+",
                          new exprNodeConstantDesc(1), 
                          new exprNodeConstantDesc(2)),
                      new exprNodeConstantDesc(3)),
                  new exprNodeConstantDesc(4))),                      
          r,
          Integer.valueOf(1 + 2 - 3 + 4));
      measureSpeed("concat(\"1\", \"2\")",
          basetimes * 100,
          ExprNodeEvaluatorFactory.get(
              TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat", 
                  new exprNodeConstantDesc("1"), 
                  new exprNodeConstantDesc("2"))),
          r,
          "12");
      measureSpeed("concat(concat(\"1\", \"2\"), \"3\")",
          basetimes * 100,
          ExprNodeEvaluatorFactory.get(
              TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat", 
                  TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat",
                      new exprNodeConstantDesc("1"), 
                      new exprNodeConstantDesc("2")),
                  new exprNodeConstantDesc("3"))),
          r,
          "123");
      measureSpeed("concat(concat(concat(\"1\", \"2\"), \"3\"), \"4\")",
          basetimes * 100,
          ExprNodeEvaluatorFactory.get(
              TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat", 
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat", 
                    TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat",
                        new exprNodeConstantDesc("1"), 
                        new exprNodeConstantDesc("2")),
                    new exprNodeConstantDesc("3")),
                new exprNodeConstantDesc("4"))),
          r,
          "1234");
      exprNodeDesc constant1 = new exprNodeConstantDesc(1);
      exprNodeDesc constant2 = new exprNodeConstantDesc(2);
      measureSpeed("concat(col1[1], cola[1])", 
          basetimes * 10,
          ExprNodeEvaluatorFactory.get(
              TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat",
                  getListIndexNode(new exprNodeColumnDesc(col1Type, "col1", "", false), constant1), 
                  getListIndexNode(new exprNodeColumnDesc(colaType, "cola", "", false), constant1))),
          r,
          "1b");
      measureSpeed("concat(concat(col1[1], cola[1]), col1[2])", 
          basetimes * 10,
          ExprNodeEvaluatorFactory.get(
              TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat", 
                  TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat", 
                      getListIndexNode(new exprNodeColumnDesc(col1Type, "col1", "", false), constant1), 
                      getListIndexNode(new exprNodeColumnDesc(colaType, "cola", "", false), constant1)),
                  getListIndexNode(new exprNodeColumnDesc(col1Type, "col1", "", false), constant2))),
          r,
          "1b2");
      measureSpeed("concat(concat(concat(col1[1], cola[1]), col1[2]), cola[2])", 
          basetimes * 10,
          ExprNodeEvaluatorFactory.get(
              TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat", 
                  TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat", 
                      TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat", 
                          getListIndexNode(new exprNodeColumnDesc(col1Type, "col1", "", false), constant1), 
                          getListIndexNode(new exprNodeColumnDesc(colaType, "cola", "", false), constant1)),
                      getListIndexNode(new exprNodeColumnDesc(col1Type, "col1", "", false), constant2)),
                  getListIndexNode(new exprNodeColumnDesc(colaType, "cola", "", false), constant2))),
          r,
          "1b2c");
      
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

}
