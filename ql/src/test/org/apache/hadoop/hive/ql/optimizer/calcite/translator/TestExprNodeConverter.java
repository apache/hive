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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class TestExprNodeConverter {
  private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(new HiveTypeSystemImpl());

  @Test
  void variantNullLiteralIsAVariantNullConstant() {
    RexLiteral nullVariant =
        new RexBuilder(typeFactory).makeNullLiteral(typeFactory.createSqlType(SqlTypeName.VARIANT));
    ExprNodeConstantDesc constant = ExprNodeConverter.toExprNodeConstantDesc(nullVariant);
    assertEquals(TypeInfoFactory.getVariantTypeInfo(), constant.getTypeInfo());
    assertNull(constant.getValue());
    assertEquals(ObjectInspector.Category.VARIANT, constant.getWritableObjectInspector().getCategory());
  }
}
