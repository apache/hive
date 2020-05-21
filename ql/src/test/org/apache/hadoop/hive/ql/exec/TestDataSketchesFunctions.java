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

package org.apache.hadoop.hive.ql.exec;

import static org.junit.Assert.assertTrue;

import java.util.Optional;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.exec.DataSketchesFunctions.SketchFunctionDescriptor;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSqlFunction;
import org.junit.Test;

/**
 * Registers functions from the DataSketches library as builtin functions.
 *
 * In an effort to show a more consistent
 */
public final class TestDataSketchesFunctions {

  @Test
  public void testKllGetCdfReturnType() {
    SketchFunctionDescriptor cf =
        DataSketchesFunctions.INSTANCE.getSketchFunction("kll", DataSketchesFunctions.GET_CDF);

    Optional<RelDataType> retType = cf.getReturnRelDataType2();

    assertTrue(retType.get().getComponentType() != null);

  }


  public class MySqlFunction extends HiveSqlFunction {

    public MySqlFunction(SqlReturnTypeInference returnTypeInference) {
      super("x", SqlKind.OTHER_FUNCTION, returnTypeInference, InferTypes.ANY_NULLABLE, OperandTypes.ANY,
          SqlFunctionCategory.USER_DEFINED_FUNCTION, true, false);
    }
  }


  @Test
  public void tArrayRet() {
    JavaTypeFactoryImpl tf = new JavaTypeFactoryImpl();
    RelDataType sqlType = tf.createSqlType(SqlTypeName.BIGINT);
    RelDataType arrayType = tf.createArrayType(sqlType, -1);
    MySqlFunction op = new MySqlFunction(ReturnTypes.explicit(arrayType));
    RexBuilder rexBuilder = new RexBuilder(tf);
    rexBuilder.makeCall(op);
    rexBuilder.makeCall(op, rexBuilder.makeLiteral("x"));

  }
}
