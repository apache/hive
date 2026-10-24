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

package org.apache.hadoop.hive.ql.optimizer.calcite;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHiveRexExecutor {

  private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(new HiveTypeSystemImpl());
  private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
  private final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

  private RexNode intLiteral(int value) {
    return rexBuilder.makeExactLiteral(BigDecimal.valueOf(value), intType);
  }

  private RexNode equalsCall(RexNode left, RexNode right) {
    return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, left, right);
  }

  @Test
  public void testReduceAndWithDuplicateConstantOperands() {
    assertSpecifiedOperatorWithDuplicateConstantOperands(SqlStdOperatorTable.AND);
  }

  @Test
  public void testReduceOrWithDuplicateConstantOperands() {
    assertSpecifiedOperatorWithDuplicateConstantOperands(SqlStdOperatorTable.OR);
  }

  private void assertSpecifiedOperatorWithDuplicateConstantOperands(SqlBinaryOperator operator) {
    RexNode eq = equalsCall(intLiteral(1), intLiteral(1));
    RexNode and = rexBuilder.makeCall(operator, eq, eq);
    List<RexNode> reduced = new ArrayList<>();

    new HiveRexExecutorImpl().reduce(rexBuilder, Collections.singletonList(and), reduced);

    assertEquals(1, reduced.size());
    assertTrue(reduced.get(0) instanceof RexLiteral);
    assertEquals(Boolean.TRUE, ((RexLiteral) reduced.get(0)).getValueAs(Boolean.class));
  }
}
