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
package org.apache.hadoop.hive.ql.parse.type;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveComponentAccess;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import com.google.common.collect.Lists;

import java.util.List;


import org.junit.Test;

public class TestHiveFunctionHelper {

  @Test
  public void testGetUDTFFunction() throws SemanticException {
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    List<RexNode> operands =
            Lists.newArrayList(rexBuilder.makeLiteral("hello"), rexBuilder.makeLiteral("world"));
    List<RexNode> arrayNode =
            Lists.newArrayList(rexBuilder.makeCall(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, operands));

    FunctionHelper functionHelper = new HiveFunctionHelper(rexBuilder);
    RexCall explodeNode = (RexCall) functionHelper.getUDTFFunction("explode", arrayNode);

    assertEquals(explodeNode.toString(), "explode(ARRAY('hello', 'world'))");
    assertEquals(explodeNode.getType().toString(), "RecordType(CHAR(5) col)");
  }

  @Test(expected = Exception.class)
  public void testGetUDTFFunctionThrowingException() throws SemanticException {
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    List<RexNode> operands = Lists.newArrayList(rexBuilder.makeLiteral("hello"));

    FunctionHelper functionHelper = new HiveFunctionHelper(rexBuilder);
    // 'upper' is not a udtf so should throw exception
    functionHelper.getUDTFFunction("upper", operands);
  }

  @Test
  public void testCoalesceWithComponentAccessDoesNotAssert() throws SemanticException {
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    FunctionHelper functionHelper = new HiveFunctionHelper(rexBuilder);

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
}
