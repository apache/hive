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

import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFPower;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestColumnPrunerProcCtx {
  // struct<a:boolean,b:double>
  private static TypeInfo col1Type;
  // double
  private static TypeInfo col2Type;
  // struct<col1:struct<a:boolean,b:double>,col2:double>
  private static TypeInfo col3Type;

  @BeforeClass
  public static void setup(){
    List<String> ns = new ArrayList<>();
    ns.add("a");
    ns.add("b");
    List<TypeInfo> tis = new ArrayList<>();
    TypeInfo aType = TypeInfoFactory.booleanTypeInfo;
    TypeInfo bType = TypeInfoFactory.doubleTypeInfo;
    tis.add(aType);
    tis.add(bType);
    col1Type = TypeInfoFactory.getStructTypeInfo(ns, tis);
    col2Type = TypeInfoFactory.doubleTypeInfo;

    List<String> names = new ArrayList<>();
    names.add("col1");
    names.add("col2");

    List<TypeInfo> typeInfos = new ArrayList<>();
    typeInfos.add(col1Type);
    typeInfos.add(col2Type);
    col3Type = TypeInfoFactory.getStructTypeInfo(names, typeInfos);
  }

  // Test select root.col1.a from root:struct<col1:struct<a:boolean,b:double>,col2:double>
  @Test
  public void testGetSelectNestedColPathsFromChildren1() {
    ColumnPrunerProcCtx ctx = new ColumnPrunerProcCtx(null);

    ExprNodeDesc colDesc = new ExprNodeColumnDesc(col3Type, "root", "test", false);
    ExprNodeDesc col1 = new ExprNodeFieldDesc(col1Type, colDesc, "col1", false);
    ExprNodeDesc fieldDesc = new ExprNodeFieldDesc(TypeInfoFactory.booleanTypeInfo, col1, "a", false);
    final List<FieldNode> paths = Arrays.asList(new FieldNode("_col0"));

    SelectOperator selectOperator = buildSelectOperator(Arrays.asList(fieldDesc), paths);
    List<FieldNode> groups = ctx.getSelectColsFromChildren(selectOperator, paths);
    compareTestResults(groups, "root.col1.a");
  }

  // Test select root.col1 from root:struct<col1:struct<a:boolean,b:double>,col2:double>
  @Test
  public void testGetSelectNestedColPathsFromChildren2() {
    ColumnPrunerProcCtx ctx = new ColumnPrunerProcCtx(null);

    ExprNodeDesc colDesc = new ExprNodeColumnDesc(col3Type, "root", "test", false);
    ExprNodeDesc fieldDesc = new ExprNodeFieldDesc(col1Type, colDesc, "col1", false);
    final List<FieldNode> paths = Arrays.asList(new FieldNode("_col0"));

    SelectOperator selectOperator = buildSelectOperator(Arrays.asList(fieldDesc), paths);
    List<FieldNode> groups = ctx.getSelectColsFromChildren(selectOperator, paths);
    compareTestResults(groups, "root.col1");
  }

  // Test select root.col2 from root:struct<col1:struct<a:boolean,b:double>,col2:double>
  @Test
  public void testGetSelectNestedColPathsFromChildren3() {
    ColumnPrunerProcCtx ctx = new ColumnPrunerProcCtx(null);

    ExprNodeDesc colDesc = new ExprNodeColumnDesc(col3Type, "root", "test", false);
    ExprNodeDesc fieldDesc = new ExprNodeFieldDesc(col1Type, colDesc, "col2", false);
    final List<FieldNode> paths = Arrays.asList(new FieldNode("_col0"));

    SelectOperator selectOperator = buildSelectOperator(Arrays.asList(fieldDesc), paths);
    List<FieldNode> groups = ctx.getSelectColsFromChildren(selectOperator, paths);
    compareTestResults(groups, "root.col2");
  }

  // Test select root from root:struct<col1:struct<a:boolean,b:double>,col2:double>
  @Test
  public void testGetSelectNestedColPathsFromChildren4() {
    ColumnPrunerProcCtx ctx = new ColumnPrunerProcCtx(null);

    ExprNodeDesc colDesc = new ExprNodeColumnDesc(col3Type, "root", "test", false);
    final List<FieldNode> paths = Arrays.asList(new FieldNode("_col0"));

    SelectOperator selectOperator = buildSelectOperator(Arrays.asList(colDesc), paths);
    List<FieldNode> groups = ctx.getSelectColsFromChildren(selectOperator, paths);
    compareTestResults(groups, "root");
  }

  // Test select named_struct from named_struct:struct<a:boolean,b:double>
  @Test
  public void testGetSelectNestedColPathsFromChildren5(){
    ColumnPrunerProcCtx ctx = new ColumnPrunerProcCtx(null);

    ExprNodeConstantDesc constADesc = new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, "a");
    ExprNodeConstantDesc constBDesc = new ExprNodeConstantDesc(TypeInfoFactory.doubleTypeInfo, "b");
    List<ExprNodeDesc> list = new ArrayList<>();
    list.add(constADesc);
    list.add(constBDesc);
    GenericUDF udf = mock(GenericUDF.class);
    ExprNodeDesc funcDesc = new ExprNodeGenericFuncDesc(col1Type, udf, "named_struct", list);
    ExprNodeDesc fieldDesc = new ExprNodeFieldDesc(TypeInfoFactory.doubleTypeInfo, funcDesc, "foo",
      false);

    final List<FieldNode> paths = Arrays.asList(new FieldNode("_col0"));
    SelectOperator selectOperator = buildSelectOperator(Arrays.asList(fieldDesc), paths);
    List<FieldNode> groups = ctx.getSelectColsFromChildren(selectOperator, paths);
    // Return empty result since only constant Desc exists
    assertEquals(0, groups.size());
  }

  // Test select abs(root.col1.b) from table test(root struct<col1:struct<a:boolean,b:double>,
  // col2:double>);
  @Test
  public void testGetSelectNestedColPathsFromChildren6(){
    ColumnPrunerProcCtx ctx = new ColumnPrunerProcCtx(null);

    ExprNodeDesc colDesc = new ExprNodeColumnDesc(col3Type, "root", "test", false);
    ExprNodeDesc col1 = new ExprNodeFieldDesc(col1Type, colDesc, "col1", false);
    ExprNodeDesc fieldDesc = new ExprNodeFieldDesc(TypeInfoFactory.doubleTypeInfo, col1, "b",
      false);
    final List<FieldNode> paths = Arrays.asList(new FieldNode("_col0"));

    GenericUDF udf = mock(GenericUDFBridge.class);

    List<ExprNodeDesc> list = new ArrayList<>();
    list.add(fieldDesc);
    ExprNodeDesc funcDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.binaryTypeInfo, udf, "abs",
      list);

    SelectOperator selectOperator = buildSelectOperator(Arrays.asList(funcDesc), paths);
    List<FieldNode> groups = ctx.getSelectColsFromChildren(selectOperator, paths);
    compareTestResults(groups, "root.col1.b");
  }

  // Test select pow(root.col1.b, root.col2) from table test(root
  // struct<col1:struct<a:boolean,b:double>, col2:double>);
  @Test
  public void testGetSelectNestedColPathsFromChildren7(){
    ColumnPrunerProcCtx ctx = new ColumnPrunerProcCtx(null);

    ExprNodeDesc colDesc = new ExprNodeColumnDesc(col3Type, "root", "test", false);
    ExprNodeDesc col1 = new ExprNodeFieldDesc(col1Type, colDesc, "col1", false);
    ExprNodeDesc fieldDesc1 =
      new ExprNodeFieldDesc(TypeInfoFactory.doubleTypeInfo, col1, "b", false);

    colDesc = new ExprNodeColumnDesc(col3Type, "root", "test", false);
    ExprNodeDesc col2 = new ExprNodeFieldDesc(col2Type, colDesc, "col2", false);
    final List<FieldNode> paths = Arrays.asList(new FieldNode("_col0"));

    GenericUDF udf = mock(GenericUDFPower.class);

    List<ExprNodeDesc> list = new ArrayList<>();
    list.add(fieldDesc1);
    list.add(col2);
    ExprNodeDesc funcDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.doubleTypeInfo, udf, "pow",
      list);

    SelectOperator selectOperator = buildSelectOperator(Arrays.asList(funcDesc), paths);
    List<FieldNode> groups = ctx.getSelectColsFromChildren(selectOperator, paths);
    compareTestResults(groups, "root.col1.b", "root.col2");
  }

  @Test
  public void testFieldNodeFromString() {
    FieldNode fn = FieldNode.fromPath("s.a.b");
    assertEquals("s", fn.getFieldName());
    assertEquals(1, fn.getNodes().size());
    FieldNode childFn = fn.getNodes().get(0);
    assertEquals("a", childFn.getFieldName());
    assertEquals(1, childFn.getNodes().size());
    assertEquals("b", childFn.getNodes().get(0).getFieldName());
  }

  @Test
  public void testMergeFieldNode() {
    FieldNode fn1 = FieldNode.fromPath("s.a.b");
    FieldNode fn2 = FieldNode.fromPath("s.a");
    assertEquals(fn2, FieldNode.mergeFieldNode(fn1, fn2));
    assertEquals(fn2, FieldNode.mergeFieldNode(fn2, fn1));

    fn1 = FieldNode.fromPath("s.a");
    fn2 = FieldNode.fromPath("p.b");
    assertNull(FieldNode.mergeFieldNode(fn1, fn2));

    fn1 = FieldNode.fromPath("s.a.b");
    fn2 = FieldNode.fromPath("s.a.c");
    FieldNode fn = FieldNode.mergeFieldNode(fn1, fn2);
    assertEquals("s", fn.getFieldName());
    FieldNode childFn = fn.getNodes().get(0);
    assertEquals("a", childFn.getFieldName());
    assertEquals(2, childFn.getNodes().size());
    assertEquals("b", childFn.getNodes().get(0).getFieldName());
    assertEquals("c", childFn.getNodes().get(1).getFieldName());
  }

  private void compareTestResults(List<FieldNode> fieldNodes, String... paths) {
    List<String> expectedPaths = new ArrayList<>();
    for (FieldNode fn : fieldNodes) {
      expectedPaths.addAll(fn.toPaths());
    }
    assertEquals("Expected paths to have length " + expectedPaths + ", but got "
        + paths.length, expectedPaths.size(), paths.length);
    for (int i = 0; i < expectedPaths.size(); ++i) {
      assertEquals("Element at index " + i + " doesn't match", expectedPaths.get(i), paths[i]);
    }
  }

  private SelectOperator buildSelectOperator(
    List<ExprNodeDesc> colList,
    List<FieldNode> outputCols) {
    SelectOperator selectOperator = mock(SelectOperator.class);
    SelectDesc selectDesc = new SelectDesc(colList, ColumnPrunerProcCtx.toColumnNames(outputCols));
    selectDesc.setSelStarNoCompute(false);
    when(selectOperator.getConf()).thenReturn(selectDesc);
    return selectOperator;
  }
}
