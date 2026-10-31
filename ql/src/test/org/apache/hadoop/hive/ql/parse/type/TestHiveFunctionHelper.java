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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.parse.type;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveComponentAccess;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import org.junit.Before;
import org.junit.Test;

public class TestHiveFunctionHelper {

  private RexBuilder rexBuilder;
  private FunctionHelper functionHelper;

  @Before
  public void setUp() {
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    rexBuilder = new RexBuilder(typeFactory);
    functionHelper = new HiveFunctionHelper(rexBuilder);
  }

  @Test
  public void testGetUDTFFunction() throws SemanticException {
    List<RexNode> operands =
        Lists.newArrayList(rexBuilder.makeLiteral("hello"), rexBuilder.makeLiteral("world"));
    List<RexNode> arrayNode =
        Lists.newArrayList(rexBuilder.makeCall(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, operands));

    RexCall explodeNode = (RexCall) functionHelper.getUDTFFunction("explode", arrayNode);

    assertEquals(explodeNode.toString(), "explode(ARRAY('hello', 'world'))");
    assertEquals(explodeNode.getType().toString(), "RecordType(CHAR(5) col)");
  }

  @Test(expected = Exception.class)
  public void testGetUDTFFunctionThrowingException() throws SemanticException {
    List<RexNode> operands = Lists.newArrayList(rexBuilder.makeLiteral("hello"));
    // 'upper' is not a udtf so should throw exception
    functionHelper.getUDTFFunction("upper", operands);
  }

  @Test
  public void testCoalesceWithComponentAccessDoesNotAssert() throws SemanticException {
    // Simulate nested field access over a collection, which is represented in Calcite by
    // a COMPONENT_ACCESS operator and can appear inside NVL/COALESCE rewrites to CASE.
    RexNode array =
        rexBuilder.makeCall(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
            Lists.newArrayList(rexBuilder.makeLiteral("hello")));
    RexNode componentAccess =
        rexBuilder.makeCall(array.getType().getComponentType(), HiveComponentAccess.COMPONENT_ACCESS,
            Lists.newArrayList(array));

    FunctionInfo fi = functionHelper.getFunctionInfo("coalesce");
    List<RexNode> inputs = Lists.newArrayList(componentAccess, rexBuilder.makeNullLiteral(componentAccess.getType()));
    RexNode expr = functionHelper.getExpression("coalesce", fi, inputs, componentAccess.getType());
    assertNotNull(expr);
    assertTrue(expr instanceof RexCall);
    // COALESCE is rewritten to CASE; the stateful-functions checker walks this tree.
    assertEquals(SqlKind.CASE, ((RexCall) expr).getOperator().getKind());
  }

  @Test
  public void testGetReturnTypeModernGenericUDTF() throws SemanticException {
    RelDataType returnType =
        getUDTFReturnType(new MockModernGenericUDTF(), rexBuilder.makeLiteral("hello"));
    assertNotNull(returnType);
    assertEquals("RecordType(VARCHAR(65536) val)", returnType.toString());
  }

  private RelDataType getUDTFReturnType(GenericUDTF udtf, RexNode... operands)
      throws SemanticException {
    FunctionInfo functionInfo =
        new FunctionInfo(FunctionInfo.FunctionType.BUILTIN, "mock_udtf", udtf);
    return functionHelper.getReturnType(functionInfo, Arrays.asList(operands));
  }

  /**
   * Mock UDTF implementing only the non-deprecated initialize(StructObjectInspector) method.
   * Deliberately leaves initialize(ObjectInspector[]) un-overridden so that any direct invocation
   * of the deprecated overload triggers the base IllegalStateException.
   */
  public static class MockModernGenericUDTF extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs)
        throws UDFArgumentException {
      List<String> fieldNames = Collections.singletonList("val");
      List<ObjectInspector> fieldOIs =
          Collections.singletonList(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
      return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {}

    @Override
    public void close() throws HiveException {}
  }
}
