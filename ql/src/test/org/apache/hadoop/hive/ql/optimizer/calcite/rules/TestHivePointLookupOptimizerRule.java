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

package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.plan.AbstractRelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ConversionUtil;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveValues;
import org.apache.hadoop.hive.ql.parse.type.RexNodeExprFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Collections;

import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.buildPlanner;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.buildRelBuilder;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.and;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.eq;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.or;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class TestHivePointLookupOptimizerRule {

  @Mock
  private RelOptSchema schemaMock;
  @Mock
  RelOptHiveTable tableMock;
  @Mock
  Table hiveTableMDMock;

  private AbstractRelOptPlanner planner;
  private RelBuilder relBuilder;

  @SuppressWarnings("unused")
  private static class MyRecord {
    public int f1;
    public int f2;
    public int f3;
    public double f4;
  }

  @Before
  public void before() {
    planner = buildPlanner(Collections.singletonList(new HivePointLookupOptimizerRule.FilterCondition(2)));
    relBuilder = buildRelBuilder(planner, schemaMock, tableMock, hiveTableMDMock, MyRecord.class);
  }

  @Test
  public void testSimpleCase() {

    // @formatter:off
    final RelNode basePlan = relBuilder
          .scan("t")
          .filter(
              and(relBuilder,
                or(relBuilder,
                    eq(relBuilder, "f1",1),
                    eq(relBuilder, "f1",2)
                    ),
                or(relBuilder,
                    eq(relBuilder, "f2",3),
                    eq(relBuilder, "f2",4)
                    )
                )
              )
          .build();
    // @formatter:on

    planner.setRoot(basePlan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    assertEquals("AND(SEARCH($0, Sarg[1, 2]), SEARCH($1, Sarg[3, 4]))", condition.toString());
  }

  @Test
  public void testInExprsMergedSingleOverlap() {

    // @formatter:off
    final RelNode basePlan = relBuilder
        .scan("t")
        .filter(
            and(relBuilder,
                or(relBuilder,
                    eq(relBuilder,"f1",1),
                    eq(relBuilder,"f1",2)
                ),
                or(relBuilder,
                    eq(relBuilder,"f1",1),
                    eq(relBuilder,"f1",3)
                )
            )
        )
        .build();
    // @formatter:on

    planner.setRoot(basePlan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    assertEquals("=($0, 1)", condition.toString());
  }

  @Test
  public void testInExprsAndEqualsMerged() {

    // @formatter:off
    final RelNode basePlan = relBuilder
        .scan("t")
        .filter(
            and(relBuilder,
                or(relBuilder,
                    eq(relBuilder,"f1",1),
                    eq(relBuilder,"f1",2)
                ),
                or(relBuilder,
                    eq(relBuilder,"f1",1),
                    eq(relBuilder,"f1",3)
                ),
                eq(relBuilder,"f1",1)
            )
        )
        .build();
    // @formatter:on

    planner.setRoot(basePlan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    assertEquals("=($0, 1)", condition.toString());
  }

  @Test
  public void testInExprsMergedMultipleOverlap() {

    // @formatter:off
    final RelNode basePlan = relBuilder
        .scan("t")
        .filter(
            and(relBuilder,
                or(relBuilder,
                    eq(relBuilder,"f1",1),
                    eq(relBuilder,"f1",2),
                    eq(relBuilder,"f1",4),
                    eq(relBuilder,"f1",3)
                ),
                or(relBuilder,
                    eq(relBuilder,"f1",5),
                    eq(relBuilder,"f1",1),
                    eq(relBuilder,"f1",2),
                    eq(relBuilder,"f1",3)
                )
            )
        )
        .build();
    // @formatter:on

    planner.setRoot(basePlan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    assertEquals("SEARCH($0, Sarg[1, 2, 3])", condition.toString());
  }

  @Test
  public void testCaseWithConstantsOfDifferentType() {

    // @formatter:off
    final RelNode basePlan = relBuilder
        .scan("t")
        .filter(
            and(relBuilder,
                or(relBuilder,
                    eq(relBuilder,"f1",1),
                    eq(relBuilder,"f1",2)
                ),
                eq(relBuilder,"f1", 1.0),
                or(relBuilder,
                    eq(relBuilder,"f4",3.0),
                    eq(relBuilder,"f4",4.1)
                )
            )
        )
        .build();
    // @formatter:on

    planner.setRoot(basePlan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();

    assertEquals("AND(SEARCH($0, Sarg[[1E0:DOUBLE..1.0E0:DOUBLE]]:DOUBLE), " +
            "SEARCH($3, Sarg[3.0E0:DOUBLE, 4.1E0:DOUBLE]:DOUBLE))",
        condition.toString());
  }

  @Test
  public void testCaseInAndEqualsWithConstantsOfDifferentType() {

    // @formatter:off
    final RelNode basePlan = relBuilder
        .scan("t")
        .filter(
            and(relBuilder,
                or(relBuilder,
                    eq(relBuilder,"f1",1),
                    eq(relBuilder,"f1",2)
                ),
                eq(relBuilder,"f1",1),
                or(relBuilder,
                    eq(relBuilder,"f4",3.0),
                    eq(relBuilder,"f4",4.1)
                ),
                eq(relBuilder,"f4",4.1)
            )
        )
        .build();
    // @formatter:on

    planner.setRoot(basePlan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    assertEquals("AND(=($0, 1), =($3, 4.1E0:DOUBLE))", condition.toString());
  }

  @Test
  public void testSimpleStructCase() {

    // @formatter:off
    final RelNode basePlan = relBuilder
          .scan("t")
          .filter(
              or(relBuilder,
                  and(relBuilder,
                      eq(relBuilder,"f1",1), eq(relBuilder,"f2",1)),
                  and(relBuilder,
                      eq(relBuilder,"f1",2), eq(relBuilder,"f2",2))
                  )
              )
          .build();
    // @formatter:on

    planner.setRoot(basePlan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    assertEquals("SEARCH(ROW($0, $1), " +
        "Sarg[[1:INTEGER, 1:INTEGER]:RecordType(INTEGER NOT NULL EXPR$0, INTEGER NOT NULL EXPR$1), " +
        "[2:INTEGER, 2:INTEGER]:RecordType(INTEGER NOT NULL EXPR$0, INTEGER NOT NULL EXPR$1)]" +
        ":RecordType(INTEGER NOT NULL EXPR$0, INTEGER NOT NULL EXPR$1))", condition.toString());
  }

  /** Despite the fact that f2=99 is there...the extraction should happen */
  @Test
  public void testObscuredSimple() {

    // @formatter:off
    final RelNode basePlan = relBuilder
          .scan("t")
          .filter(
              or(relBuilder,
                  eq(relBuilder,"f2",99),
                  eq(relBuilder,"f1",1),
                  eq(relBuilder,"f1",2)
                  )
              )
          .build();
    // @formatter:on

    planner.setRoot(basePlan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    System.out.println(condition);
    assertEquals("OR(=($1, 99), SEARCH($0, Sarg[1, 2]))", condition.toString());
  }

  /** Despite that extraction happen at a higher level; nested parts should also be handled */
  @Test
  public void testRecursionIsNotObstructed() {

    // @formatter:off
    final RelNode basePlan = relBuilder
          .scan("t")
          .filter(
              and(relBuilder,
                or(relBuilder,
                    eq(relBuilder,"f1",1),
                    eq(relBuilder,"f1",2)
                    )
                ,
                or(relBuilder,
                    and(relBuilder,
                        or(relBuilder,
                            eq(relBuilder,"f2",1), eq(relBuilder,"f2",2)),
                        or(relBuilder,
                            eq(relBuilder,"f3",1), eq(relBuilder,"f3",2))
                        ),
                    and(relBuilder,
                        or(relBuilder,
                            eq(relBuilder,"f2",3),eq(relBuilder,"f2",4)),
                        or(relBuilder,
                            eq(relBuilder,"f3",3),eq(relBuilder,"f3",4))
                        )
                )
              ))
          .build();
    // @formatter:on

    planner.setRoot(basePlan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    System.out.println(condition);

    assertEquals("AND(SEARCH($0, Sarg[1, 2]), OR(AND(SEARCH($1, Sarg[1, 2]), SEARCH($2, Sarg[1, 2])), " +
            "AND(SEARCH($1, Sarg[3, 4]), SEARCH($2, Sarg[3, 4]))))",
        condition.toString());
  }

  @Test
  public void testSameVarcharLiteralDifferentPrecision() {

    final RexBuilder rexBuilder = relBuilder.getRexBuilder();
    RelDataType stringType30 = rexBuilder.getTypeFactory().createTypeWithCharsetAndCollation(
            rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR, 30),
            Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME), SqlCollation.IMPLICIT);
    RexNode lita30 = rexBuilder.makeLiteral(RexNodeExprFactory.makeHiveUnicodeString("AAA111"), stringType30, true);
    RexNode litb30 = rexBuilder.makeLiteral(RexNodeExprFactory.makeHiveUnicodeString("BBB222"), stringType30, true);

    RelDataType stringType14 = rexBuilder.getTypeFactory().createTypeWithCharsetAndCollation(
            rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR, 14),
            Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME), SqlCollation.IMPLICIT);
    RexNode lita14 = rexBuilder.makeLiteral(RexNodeExprFactory.makeHiveUnicodeString("AAA111"), stringType14, true);
    RexNode litb14 = rexBuilder.makeLiteral(RexNodeExprFactory.makeHiveUnicodeString("BBB222"), stringType14, true);

    final RelNode basePlan = relBuilder
          .scan("t")
          .filter(and(relBuilder,
                  relBuilder.in(relBuilder.field("f2"), lita30, litb30),
                  relBuilder.in(relBuilder.field("f2"), lita14, litb14)))
          .build();

    planner.setRoot(basePlan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    System.out.println(condition);
    assertEquals("SEARCH($1, " +
                    "Sarg[_UTF-16LE'AAA111':VARCHAR(30) CHARACTER SET \"UTF-16LE\", " +
                    "_UTF-16LE'BBB222':VARCHAR(30) CHARACTER SET \"UTF-16LE\"]:" +
                    "VARCHAR(30) CHARACTER SET \"UTF-16LE\")",
            condition.toString());
  }

  @Test
  public void testSameVarcharLiteralDifferentPrecisionValueOverflow() {

    final RexBuilder rexBuilder = relBuilder.getRexBuilder();
    RelDataType stringType30 = rexBuilder.getTypeFactory().createTypeWithCharsetAndCollation(
            rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR, 30),
            Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME), SqlCollation.IMPLICIT);
    RexNode lita30 = rexBuilder.makeLiteral(RexNodeExprFactory.makeHiveUnicodeString("AAA111"), stringType30, true);
    RexNode litb30 = rexBuilder.makeLiteral(RexNodeExprFactory.makeHiveUnicodeString("BBB222"), stringType30, true);

    RelDataType stringType4 = rexBuilder.getTypeFactory().createTypeWithCharsetAndCollation(
            rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR, 4),
            Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME), SqlCollation.IMPLICIT);
    RexNode litaOverflow =
            rexBuilder.makeLiteral(RexNodeExprFactory.makeHiveUnicodeString("AAA111"), stringType4, true);
    RexNode litbOverflow =
            rexBuilder.makeLiteral(RexNodeExprFactory.makeHiveUnicodeString("BBB222"), stringType4, true);

    final RelNode basePlan = relBuilder
          .scan("t")
          .filter(and(relBuilder,
                  relBuilder.in(relBuilder.field("f2"), lita30, litb30),
                  relBuilder.in(relBuilder.field("f2"), litaOverflow, litbOverflow)))
          .build();

    assertTrue(basePlan instanceof HiveValues && ((HiveValues) basePlan).getTuples().isEmpty());
  }

  @Test
  public void testSameVarcharAndNullLiterals() {

    final RexBuilder rexBuilder = relBuilder.getRexBuilder();
    RelDataType stringType30 = rexBuilder.getTypeFactory().createTypeWithCharsetAndCollation(
            rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR, 30),
            Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME), SqlCollation.IMPLICIT);
    RexNode lita30 = rexBuilder.makeNullLiteral(stringType30);
    RexNode litb30 = rexBuilder.makeNullLiteral(stringType30);

    RelDataType stringType14 = rexBuilder.getTypeFactory().createTypeWithCharsetAndCollation(
            rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR, 14),
            Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME), SqlCollation.IMPLICIT);
    RexNode lita14 = rexBuilder.makeLiteral(RexNodeExprFactory.makeHiveUnicodeString("AAA111"), stringType14, true);
    RexNode litb14 = rexBuilder.makeLiteral(RexNodeExprFactory.makeHiveUnicodeString("BBB222"), stringType14, true);

    final RelNode basePlan = relBuilder
            .scan("t")
            .filter(and(relBuilder,
                    relBuilder.in(relBuilder.field("f2"), lita30, litb30),
                    relBuilder.in(relBuilder.field("f2"), lita14, litb14)))
            .build();

    assertTrue(basePlan instanceof HiveValues && ((HiveValues) basePlan).getTuples().isEmpty());
  }

  @Test
  public void testSameVarcharLiteralsDifferentPrecisionInOrExpression() {

    final RexBuilder rexBuilder = relBuilder.getRexBuilder();
    RelDataType stringType30 = rexBuilder.getTypeFactory().createTypeWithCharsetAndCollation(
            rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR, 30),
            Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME), SqlCollation.IMPLICIT);
    RexNode lita30 = rexBuilder.makeLiteral(RexNodeExprFactory.makeHiveUnicodeString("AAA111"), stringType30, true);
    RexNode litb30 = rexBuilder.makeLiteral(RexNodeExprFactory.makeHiveUnicodeString("BBB222"), stringType30, true);

    RelDataType stringType14 = rexBuilder.getTypeFactory().createTypeWithCharsetAndCollation(
            rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR, 14),
            Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME), SqlCollation.IMPLICIT);
    RexNode lita14 = rexBuilder.makeLiteral(RexNodeExprFactory.makeHiveUnicodeString("AAA111"), stringType14, true);
    RexNode litb14 = rexBuilder.makeLiteral(RexNodeExprFactory.makeHiveUnicodeString("BBB222"), stringType14, true);

    final RelNode basePlan = relBuilder
            .scan("t")
            .filter(or(relBuilder,
                    relBuilder.in(relBuilder.field("f2"), lita30, litb30),
                    relBuilder.in(relBuilder.field("f2"), lita14, litb14)))
            .build();

    planner.setRoot(basePlan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    System.out.println(condition);
    assertEquals("SEARCH($1, " +
                    "Sarg[_UTF-16LE'AAA111':VARCHAR(30) CHARACTER SET \"UTF-16LE\", " +
                    "_UTF-16LE'BBB222':VARCHAR(30) CHARACTER SET \"UTF-16LE\"]" +
                    ":VARCHAR(30) CHARACTER SET \"UTF-16LE\")",
            condition.toString());
  }

  @Test
  public void testSameDecimalLiteralDifferentPrecision() {

    final RexBuilder rexBuilder = relBuilder.getRexBuilder();
    RelDataType decimalType30 = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DECIMAL, 30, 5);
    RexNode lita30 = rexBuilder.makeLiteral(10000, decimalType30, true);
    RexNode litb30 = rexBuilder.makeLiteral(11000, decimalType30, true);

    RelDataType decimalType14 = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DECIMAL, 14, 5);
    RexNode lita14 = rexBuilder.makeLiteral(10000, decimalType14, true);
    RexNode litb14 = rexBuilder.makeLiteral(11000, decimalType14, true);

    final RelNode basePlan = relBuilder
          .scan("t")
          .filter(and(relBuilder,
                  relBuilder.in(relBuilder.field("f2"), lita30, litb30),
                  relBuilder.in(relBuilder.field("f2"), lita14, litb14)))
          .build();

    planner.setRoot(basePlan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    System.out.println(condition);
    assertEquals("SEARCH($1, Sarg[10000:DECIMAL(19, 5), 11000:DECIMAL(19, 5)]:DECIMAL(19, 5))",
        condition.toString());
  }
}
