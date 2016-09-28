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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import static org.junit.Assert.assertEquals;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

public class TestCBOMaxNumToCNF {

  final int maxNumNodesCNF = 8;

  @Test
  public void testCBOMaxNumToCNF1() {
    // OR(=($0, 1), AND(=($0, 0), =($1, 8)))
    // transformation creates 7 nodes AND(OR(=($0, 1), =($0, 0)), OR(=($0, 1), =($1, 8)))
    // thus, it is triggered
    final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    final RexNode cond = rexBuilder.makeCall(SqlStdOperatorTable.OR,
            rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                    rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
                    rexBuilder.makeLiteral(1, typeFactory.createSqlType(SqlTypeName.INTEGER), false)),
            rexBuilder.makeCall(SqlStdOperatorTable.AND,
                    rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
                            rexBuilder.makeLiteral(0, typeFactory.createSqlType(SqlTypeName.INTEGER), false)),
                    rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1),
                            rexBuilder.makeLiteral(8, typeFactory.createSqlType(SqlTypeName.INTEGER), false))));
    final RexNode newCond = RexUtil.toCnf(rexBuilder, maxNumNodesCNF, cond);

    assertEquals(newCond.toString(), "AND(OR(=($0, 1), =($0, 0)), OR(=($0, 1), =($1, 8)))");
  }

  @Test
  public void testCBOMaxNumToCNF2() {
    // OR(=($0, 1), =($0, 2), AND(=($0, 0), =($1, 8)))
    // transformation creates 9 nodes AND(OR(=($0, 1), =($0, 2), =($0, 0)), OR(=($0, 1), =($0, 2), =($1, 8)))
    // thus, it is NOT triggered
    final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    final RexNode cond = rexBuilder.makeCall(SqlStdOperatorTable.OR,
            rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                    rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
                    rexBuilder.makeLiteral(1, typeFactory.createSqlType(SqlTypeName.INTEGER), false)),
            rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                    rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
                    rexBuilder.makeLiteral(2, typeFactory.createSqlType(SqlTypeName.INTEGER), false)),
            rexBuilder.makeCall(SqlStdOperatorTable.AND,
                    rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
                            rexBuilder.makeLiteral(0, typeFactory.createSqlType(SqlTypeName.INTEGER), false)),
                    rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1),
                            rexBuilder.makeLiteral(8, typeFactory.createSqlType(SqlTypeName.INTEGER), false))));
    final RexNode newCond = RexUtil.toCnf(rexBuilder, maxNumNodesCNF, cond);

    assertEquals(newCond.toString(), "OR(=($0, 1), =($0, 2), AND(=($0, 0), =($1, 8)))");
  }

}
