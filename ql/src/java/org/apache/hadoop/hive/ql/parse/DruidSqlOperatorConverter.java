/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.parse;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import org.apache.calcite.adapter.druid.DirectOperatorConversion;
import org.apache.calcite.adapter.druid.DruidExpressions;
import org.apache.calcite.adapter.druid.DruidQuery;
import org.apache.calcite.adapter.druid.ExtractOperatorConversion;
import org.apache.calcite.adapter.druid.FloorOperatorConversion;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveConcat;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveExtractDate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFloorDate;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/**
 * Contains custom Druid SQL operator converter classes, contains either:
 * Hive specific OperatorConversion logic that can not be part of Calcite
 * Some temporary OperatorConversion that is not release by Calcite yet
 */
public class DruidSqlOperatorConverter {
  private DruidSqlOperatorConverter() {
  }
  private static Map druidOperatorMap = null;

  public static final Map<SqlOperator, org.apache.calcite.adapter.druid.DruidSqlOperatorConverter> getDefaultMap() {
    if (druidOperatorMap == null) {
      druidOperatorMap =
          new HashMap<SqlOperator, org.apache.calcite.adapter.druid.DruidSqlOperatorConverter>();
      DruidQuery.DEFAULT_OPERATORS_LIST.stream()
          .forEach(op -> druidOperatorMap.put(op.calciteOperator(), op));

      //Override Hive specific operators
      druidOperatorMap.putAll(Maps.asMap(HiveFloorDate.ALL_FUNCTIONS,
          (Function<SqlFunction, org.apache.calcite.adapter.druid.DruidSqlOperatorConverter>) input -> new FloorOperatorConversion()
      ));
      druidOperatorMap.putAll(Maps.asMap(HiveExtractDate.ALL_FUNCTIONS,
          (Function<SqlFunction, org.apache.calcite.adapter.druid.DruidSqlOperatorConverter>) input -> new ExtractOperatorConversion()
      ));
      druidOperatorMap
          .put(HiveConcat.INSTANCE, new DirectOperatorConversion(HiveConcat.INSTANCE, "concat"));
      druidOperatorMap.put(SqlStdOperatorTable.SUBSTRING,
          new DruidSqlOperatorConverter.DruidSubstringOperatorConversion()
      );
    }
    return druidOperatorMap;
  }

  //@TODO remove this when it is fixed in calcite https://issues.apache.org/jira/browse/HIVE-18996
  public static class DruidSubstringOperatorConversion extends org.apache.calcite.adapter.druid.SubstringOperatorConversion {
    @Nullable @Override public String toDruidExpression(RexNode rexNode, RelDataType rowType,
        DruidQuery query
    ) {
      final RexCall call = (RexCall) rexNode;
      final String arg = DruidExpressions.toDruidExpression(
          call.getOperands().get(0), rowType, query);
      if (arg == null) {
        return null;
      }

      final String indexStart;
      final String length;
      // SQL is 1-indexed, Druid is 0-indexed.
      if (!call.getOperands().get(1).isA(SqlKind.LITERAL)) {
        final String indexExp = DruidExpressions.toDruidExpression(
            call.getOperands().get(1), rowType, query);
        if (indexExp == null) {
          return null;
        }
        indexStart = DruidQuery.format("(%s - 1)", indexExp);
      } else {
        final int index = RexLiteral.intValue(call.getOperands().get(1)) - 1;
        indexStart = DruidExpressions.numberLiteral(index);
      }

      if (call.getOperands().size() > 2) {
        //case substring from index with length
        if (!call.getOperands().get(2).isA(SqlKind.LITERAL)) {
          length = DruidExpressions.toDruidExpression(
              call.getOperands().get(2), rowType, query);
          if (length == null) {
            return null;
          }
        } else {
          length = DruidExpressions.numberLiteral(RexLiteral.intValue(call.getOperands().get(2)));
        }

      } else {
        //case substring from index to the end
        length = DruidExpressions.numberLiteral(-1);
      }
      return DruidQuery.format("substring(%s, %s, %s)",
          arg,
          indexStart,
          length);
    }
  }
}
