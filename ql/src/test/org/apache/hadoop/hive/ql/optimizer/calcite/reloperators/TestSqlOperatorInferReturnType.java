/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@RunWith(Parameterized.class)
public class TestSqlOperatorInferReturnType {
  private static final RelDataTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl(new HiveTypeSystemImpl());
  final SqlOperator op;
  final List<RelDataType> inputTypes;
  final RelDataType returnType;

  public TestSqlOperatorInferReturnType(final SqlOperator op, final List<RelDataType> inputTypes,
      final RelDataType returnType) {
    this.op = op;
    this.inputTypes = inputTypes;
    this.returnType = returnType;
  }

  @Test
  public void testInferReturnType() {
    Assert.assertEquals(returnType, op.inferReturnType(TYPE_FACTORY, inputTypes));
  }

  @Parameterized.Parameters(name = "op={0}, inTypes={1}, expectedReturnType={2}")
  public static Collection<Object[]> generateValidOperatorCalls() throws SemanticException {
    RelDataType varchar19 = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, 19);
    RelDataType bigint = TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);

    List<Object[]> calls = new ArrayList<>();
    calls.add(new Object[] { HiveUnixTimestampSqlOperator.INSTANCE, Collections.emptyList(), bigint });
    calls.add(new Object[] { HiveUnixTimestampSqlOperator.INSTANCE, Collections.singletonList(varchar19), bigint });
    calls.add(new Object[] { HiveUnixTimestampSqlOperator.INSTANCE, Arrays.asList(varchar19, varchar19), bigint });

    calls.add(new Object[] { HiveToUnixTimestampSqlOperator.INSTANCE, Collections.emptyList(), bigint });
    calls.add(new Object[] { HiveToUnixTimestampSqlOperator.INSTANCE, Collections.singletonList(varchar19), bigint });
    calls.add(new Object[] { HiveToUnixTimestampSqlOperator.INSTANCE, Arrays.asList(varchar19, varchar19), bigint });
    return calls;
  }

}
