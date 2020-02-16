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

import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.DateString;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.impala.expr.ImpalaBoolLiteral;
import org.apache.hadoop.hive.ql.plan.impala.expr.ImpalaDateLiteral;
import org.apache.hadoop.hive.ql.plan.impala.expr.ImpalaNullLiteral;
import org.apache.hadoop.hive.ql.plan.impala.expr.ImpalaStringLiteral;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.ImpalaTypeConverter;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.common.SqlCastException;

import java.math.BigDecimal;

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
          return new ImpalaNullLiteral(analyzer);
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
        default:
          throw new HiveException("Unsupported RexLiteral: " + rexLiteral.getTypeName());
      }
    } catch (SqlCastException e) {
      throw new HiveException("Cast exception for type " + rexLiteral.getTypeName()
          + " for value " + rexLiteral + " :" + e);
    }
  }
}
