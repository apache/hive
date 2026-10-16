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

package org.apache.hadoop.hive.ql.optimizer.calcite.translator;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestRexNodeConverter {
  private final RexBuilder rexBuilder = new RexBuilder(new JavaTypeFactoryImpl(new HiveTypeSystemImpl()));

  @Test
  void variantNullConstantIsAVariantNullLiteral() throws Exception {
    RexNode node = new RexNodeConverter(rexBuilder).convert(
        new ExprNodeConstantDesc(TypeInfoFactory.getVariantTypeInfo(), null));
    assertTrue(node instanceof RexLiteral);
    assertTrue(((RexLiteral) node).isNull());
    assertEquals(SqlTypeName.VARIANT, node.getType().getSqlTypeName());
    assertTrue(node.getType().isNullable());
  }
}
