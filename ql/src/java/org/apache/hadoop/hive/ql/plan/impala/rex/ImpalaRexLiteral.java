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

package org.apache.hadoop.hive.ql.plan.impala.rex;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimestampString;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.impala.expr.ImpalaBoolLiteral;
import org.apache.hadoop.hive.ql.plan.impala.expr.ImpalaDateLiteral;
import org.apache.hadoop.hive.ql.plan.impala.expr.ImpalaFunctionCallExpr;
import org.apache.hadoop.hive.ql.plan.impala.expr.ImpalaNullLiteral;
import org.apache.hadoop.hive.ql.plan.impala.expr.ImpalaStringLiteral;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.DefaultFunctionSignature;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.ImpalaFunctionSignature;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.ImpalaTypeConverter;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.ScalarFunctionDetails;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.ScalarFunctionUtil;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.SqlCastException;
import org.apache.impala.thrift.TPrimitiveType;

import java.math.BigDecimal;
import java.util.List;

/**
 * Static Helper class that returns Exprs for RexLiteral nodes.
 */
public class ImpalaRexLiteral {

  /*
   * Returns Expr object for ImpalaRexLiteral
   */
  public static Expr getExpr(Analyzer analyzer, RexLiteral rexLiteral) throws HiveException {
    try {
      // TODO: CDPD-8266: need to support all getTypeName types.
      switch (rexLiteral.getTypeName()) {
        case NULL:
          SqlTypeName sqlTypeName = rexLiteral.getType().getSqlTypeName();
          TPrimitiveType primitiveType = ImpalaTypeConverter.getTPrimitiveType(sqlTypeName);
          Type type = ImpalaTypeConverter.getImpalaType(primitiveType,
              rexLiteral.getType().getPrecision(), rexLiteral.getType().getScale());
          return new ImpalaNullLiteral(analyzer, type);
        case BOOLEAN:
          return new ImpalaBoolLiteral(analyzer, rexLiteral.getValueAs(Boolean.class));
        case DECIMAL:
        case DOUBLE:
          return new NumericLiteral(rexLiteral.getValueAs(BigDecimal.class),
              ImpalaTypeConverter.getImpalaType(rexLiteral.getType()));
        case CHAR:
          return new ImpalaStringLiteral(analyzer, rexLiteral.getValueAs(String.class));
        case DATE:
          DateString dateStringClass = rexLiteral.getValueAs(DateString.class);
          String dateString = (dateStringClass == null) ? null : dateStringClass.toString();
          return new ImpalaDateLiteral(analyzer, rexLiteral.getValueAs(Integer.class),
              dateString);
        case SYMBOL:
          return new ImpalaStringLiteral(analyzer, rexLiteral.getValue().toString());
        case TIMESTAMP:
          return createCastTimestampExpr(analyzer, rexLiteral);
        default:
          throw new HiveException("Unsupported RexLiteral: " + rexLiteral.getTypeName());
      }
    } catch (SqlCastException e) {
      throw new HiveException("Cast exception for type " + rexLiteral.getTypeName()
          + " for value " + rexLiteral + " :" + e);
    }
  }

  /**
   * Create a cast timestamp expression from a String to a Timestamp.
   * The only way to create a TimestampLiteral directly in Impala is by accessing
   * the backend. This will normally be done earlier in Calcite via constant folding.
   * If constant folding was not allowed, it means we did not have access to the backend
   * and thus need to do a cast in order to support conversion to a Timestamp.
   */
  private static Expr createCastTimestampExpr(Analyzer analyzer, RexLiteral rexLiteral)
      throws HiveException, SqlCastException {
    List<SqlTypeName> typeNames =
        ImmutableList.of(SqlTypeName.VARCHAR);

    String timestamp = rexLiteral.getValueAs(TimestampString.class).toString();
    List<Expr> argList = Lists.newArrayList(new ImpalaStringLiteral(analyzer, timestamp));
    ImpalaFunctionSignature castFuncSig =
        new DefaultFunctionSignature("cast", typeNames, SqlTypeName.TIMESTAMP);
    ScalarFunctionDetails castFuncDetails = ScalarFunctionDetails.get(castFuncSig);
    Function castFunc = ScalarFunctionUtil.create(castFuncDetails);
    return new ImpalaFunctionCallExpr(analyzer, castFunc, argList, null, Type.TIMESTAMP);
  }
}
