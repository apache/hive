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

package org.apache.hadoop.hive.impala.rex;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimestampString;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.impala.expr.ImpalaFunctionCallExpr;
import org.apache.hadoop.hive.impala.expr.ImpalaNullLiteral;
import org.apache.hadoop.hive.impala.funcmapper.ImpalaTypeConverter;
import org.apache.hadoop.hive.impala.funcmapper.ScalarFunctionDetails;
import org.apache.hadoop.hive.impala.funcmapper.ImpalaFunctionUtil;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.BoolLiteral;
import org.apache.impala.analysis.DateLiteral;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.analysis.StringLiteral;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;

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
      if (SqlTypeName.INTERVAL_TYPES.contains(rexLiteral.getTypeName())) {
        return new NumericLiteral(new BigDecimal(rexLiteral.getValueAs(Long.class)), Type.BIGINT);
      }
      // TODO: CDPD-8266: need to support all getTypeName types.
      switch (rexLiteral.getTypeName()) {
        case NULL:
          Type type = ImpalaTypeConverter.createImpalaType(rexLiteral.getType());
          return new ImpalaNullLiteral(null, type);
        case BOOLEAN:
          Expr boolExpr = new BoolLiteral(rexLiteral.getValueAs(Boolean.class));
          boolExpr.analyze(null);
          return boolExpr;
        case BIGINT:
        case DECIMAL:
        case DOUBLE:
          Expr numericExpr = new NumericLiteral(rexLiteral.getValueAs(BigDecimal.class),
              ImpalaTypeConverter.createImpalaType(rexLiteral.getType()));
          numericExpr.analyze(null);
          return numericExpr;
        case CHAR:
        case VARCHAR:
          Expr charExpr = new StringLiteral(rexLiteral.getValueAs(String.class),
              getCharType(rexLiteral), false);
          charExpr.analyze(null);
          return charExpr;
        case DATE:
          DateString dateStringClass = rexLiteral.getValueAs(DateString.class);
          String dateString = (dateStringClass == null) ? null : dateStringClass.toString();
          Expr dateExpr = new DateLiteral(rexLiteral.getValueAs(Integer.class), dateString);
          dateExpr.analyze(null);
          return dateExpr;
        case SYMBOL:
          Expr symbolExpr = new StringLiteral(rexLiteral.getValue().toString(), Type.STRING, false);
          symbolExpr.analyze(null);
          return symbolExpr;
        case TIMESTAMP:
          return createCastTimestampExpr(analyzer, rexLiteral);
        default:
          throw new HiveException("Unsupported RexLiteral: " + rexLiteral.getTypeName());
      }
    } catch (AnalysisException e) {
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
      throws HiveException {
    List<Type> typeNames = ImmutableList.of(Type.STRING);
 
    String timestamp = rexLiteral.getValueAs(TimestampString.class).toString();
    List<Expr> argList =
        Lists.newArrayList(new StringLiteral(timestamp, Type.STRING, false));
    ScalarFunctionDetails castFuncDetails = ScalarFunctionDetails.get("cast", typeNames,
        Type.TIMESTAMP);
    Function castFunc = ImpalaFunctionUtil.create(castFuncDetails);
    return new ImpalaFunctionCallExpr(analyzer, castFunc, argList, null, Type.TIMESTAMP);
  }

  private static Type getCharType(RexLiteral rexLiteral) {
    switch (rexLiteral.getType().getSqlTypeName()) {
      case CHAR:
        return ImpalaTypeConverter.createImpalaType(
            Type.CHAR, rexLiteral.getType().getPrecision(), 0);
      case VARCHAR:
        if (rexLiteral.getType().getPrecision() == Integer.MAX_VALUE) {
          return ImpalaTypeConverter.createImpalaType(Type.STRING, 0, 0);
        }
        return ImpalaTypeConverter.createImpalaType(
            Type.VARCHAR, rexLiteral.getType().getPrecision(), 0);
    }
    throw new RuntimeException("Not a char/varchar/string type: " + rexLiteral.getType());
  }
}
