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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sarg;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.calcite.rex.RexUnknownAs.FALSE;
import static org.apache.calcite.rex.RexUnknownAs.TRUE;
import static org.apache.calcite.rex.RexUnknownAs.UNKNOWN;
import static org.apache.calcite.rex.RexUnknownAs.values;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.COALESCE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SEARCH;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestSearchTransformerShuttle {

  private final RexNode node;
  private final RexUnknownAs shuttleContext;
  private final String expected;

  public TestSearchTransformerShuttle(RexNode node, RexUnknownAs shuttleContext, String expected) {
    this.node = node;
    this.shuttleContext = shuttleContext;
    this.expected = expected;
  }

  @Test
  public void testAccept() {
    RexBuilder rexBuilder = new RexBuilder(new JavaTypeFactoryImpl(new HiveTypeSystemImpl()));
    SearchTransformer.Shuttle shuttle = new SearchTransformer.Shuttle(rexBuilder, shuttleContext);
    assertEquals(expected, node.accept(shuttle).toString());
  }

  @Parameterized.Parameters(name = "expression={0}, shuttleCtx={1}")
  public static Collection<Object[]> generateExpressions() {
    RexBuilder rexBuilder = new RexBuilder(new JavaTypeFactoryImpl(new HiveTypeSystemImpl()));
    List<Object[]> expressions = new ArrayList<>();
    RexLiteral rexTrue = rexBuilder.makeLiteral(true);
    RexLiteral rexFalse = rexBuilder.makeLiteral(false);

    final RexNode searchUnknown = createSearchNode(rexBuilder, 10, 20, UNKNOWN);
    final RexNode searchFalse = createSearchNode(rexBuilder, 10, 20, FALSE);
    final RexNode searchTrue = createSearchNode(rexBuilder, 10, 20, TRUE);
    for (RexUnknownAs nullAs : values()) {
      // COALESCE is a special function so it's unsafe to drop IS [NOT] NULL, no matter the context
      expressions.add(new Object[] { rexBuilder.makeCall(COALESCE, searchFalse, rexTrue), nullAs,
          "COALESCE(AND(IS NOT NULL($0), BETWEEN(false, $0, 10, 20)), true)" });
      expressions.add(new Object[] { rexBuilder.makeCall(COALESCE, searchTrue, rexFalse), nullAs,
          "COALESCE(OR(IS NULL($0), BETWEEN(false, $0, 10, 20)), false)" });
      expressions.add(new Object[] { rexBuilder.makeCall(COALESCE, searchUnknown, rexFalse), nullAs,
          "COALESCE(BETWEEN(false, $0, 10, 20), false)" });
      // Search with null as unknown does not generate extra IS [NOT] NULL predicates
      expressions.add(new Object[] { searchUnknown, nullAs, "BETWEEN(false, $0, 10, 20)" });
    }

    // In FALSE/FALSE and TRUE/TRUE cases, the IS [NOT] NULL predicates are dropped
    expressions.add(new Object[] { searchFalse, FALSE, "BETWEEN(false, $0, 10, 20)" });
    expressions.add(new Object[] { searchTrue, TRUE, "BETWEEN(false, $0, 10, 20)" });

    expressions.add(new Object[] { searchTrue, FALSE, "OR(IS NULL($0), BETWEEN(false, $0, 10, 20))" });
    expressions.add(new Object[] { searchTrue, UNKNOWN, "OR(IS NULL($0), BETWEEN(false, $0, 10, 20))" });

    expressions.add(new Object[] { searchFalse, TRUE, "AND(IS NOT NULL($0), BETWEEN(false, $0, 10, 20))" });
    expressions.add(new Object[] { searchFalse, UNKNOWN, "AND(IS NOT NULL($0), BETWEEN(false, $0, 10, 20))" });
    return expressions;
  }

  private static RexNode createSearchNode(RexBuilder builder, int lower, int upper, RexUnknownAs nullAs) {
    final RelDataType intType = builder.getTypeFactory()
        .createTypeWithNullability(builder.getTypeFactory().createSqlType(SqlTypeName.INTEGER), true);
    RangeSet<BigDecimal> rangeSet =
        ImmutableRangeSet.of(Range.closed(BigDecimal.valueOf(lower), BigDecimal.valueOf(upper)));
    Sarg sarg = Sarg.of(nullAs, rangeSet);
    return builder.makeCall(SEARCH, builder.makeInputRef(intType, 0), builder.makeSearchArgumentLiteral(sarg, intType));
  }

}

