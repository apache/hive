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

package org.apache.hadoop.hive.ql.parse;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.calcite.adapter.druid.DirectOperatorConversion;
import org.apache.calcite.adapter.druid.DruidExpressions;
import org.apache.calcite.adapter.druid.DruidQuery;
import org.apache.calcite.adapter.druid.ExtractOperatorConversion;
import org.apache.calcite.adapter.druid.FloorOperatorConversion;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveConcat;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveDateAddSqlOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveDateSubSqlOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveExtractDate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFloorDate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFromUnixTimeSqlOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveToDateSqlOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTruncSqlOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveToUnixTimestampSqlOperator;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Contains custom Druid SQL operator converter classes, contains either:
 * Hive specific OperatorConversion logic that can not be part of Calcite
 * Some temporary OperatorConversion that is not release by Calcite yet
 */
public class DruidSqlOperatorConverter {

  private static final String YYYY_MM_DD = "yyyy-MM-dd";
  public static final String DEFAULT_TS_FORMAT = "yyyy-MM-dd HH:mm:ss";

  private DruidSqlOperatorConverter() {
  }

  private static Map druidOperatorMap = null;

  public static final Map<SqlOperator, org.apache.calcite.adapter.druid.DruidSqlOperatorConverter> getDefaultMap() {
    if (druidOperatorMap == null) {
      druidOperatorMap = new HashMap<SqlOperator, org.apache.calcite.adapter.druid.DruidSqlOperatorConverter>();
      DruidQuery.DEFAULT_OPERATORS_LIST.stream().forEach(op -> druidOperatorMap.put(op.calciteOperator(), op));

      //Override Hive specific operators
      druidOperatorMap.putAll(Maps.asMap(HiveFloorDate.ALL_FUNCTIONS,
          (Function<SqlFunction, org.apache.calcite.adapter.druid.DruidSqlOperatorConverter>) input -> new
              FloorOperatorConversion()
      ));
      druidOperatorMap.putAll(Maps.asMap(HiveExtractDate.ALL_FUNCTIONS,
          (Function<SqlFunction, org.apache.calcite.adapter.druid.DruidSqlOperatorConverter>) input -> new
              ExtractOperatorConversion()
      ));
      druidOperatorMap.put(HiveConcat.INSTANCE, new DirectOperatorConversion(HiveConcat.INSTANCE, "concat"));
      druidOperatorMap
          .put(SqlStdOperatorTable.SUBSTRING, new DruidSqlOperatorConverter.DruidSubstringOperatorConversion());
      druidOperatorMap
          .put(SqlStdOperatorTable.IS_NULL, new UnaryFunctionOperatorConversion(SqlStdOperatorTable.IS_NULL, "isnull"));
      druidOperatorMap.put(SqlStdOperatorTable.IS_NOT_NULL,
          new UnaryFunctionOperatorConversion(SqlStdOperatorTable.IS_NOT_NULL, "notnull")
      );
      druidOperatorMap.put(HiveTruncSqlOperator.INSTANCE, new DruidDateTruncOperatorConversion());
      druidOperatorMap.put(HiveToDateSqlOperator.INSTANCE, new DruidToDateOperatorConversion());
      druidOperatorMap.put(HiveFromUnixTimeSqlOperator.INSTANCE, new DruidFormUnixTimeOperatorConversion());
      druidOperatorMap.put(HiveToUnixTimestampSqlOperator.INSTANCE, new DruidUnixTimestampOperatorConversion());
      druidOperatorMap.put(HiveDateAddSqlOperator.INSTANCE,
          new DruidDateArithmeticOperatorConversion(1, HiveDateAddSqlOperator.INSTANCE)
      );
      druidOperatorMap.put(HiveDateSubSqlOperator.INSTANCE,
          new DruidDateArithmeticOperatorConversion(-1, HiveDateSubSqlOperator.INSTANCE)
      );
    }
    return druidOperatorMap;
  }

  /**
   * Druid operator converter from Hive Substring to Druid SubString.
   * This is a temporary fix that can be removed once we move to a Calcite version including the following.
   * https://issues.apache.org/jira/browse/CALCITE-2226
   */
  public static class DruidSubstringOperatorConversion
      extends org.apache.calcite.adapter.druid.SubstringOperatorConversion {
    @Nullable @Override public String toDruidExpression(RexNode rexNode, RelDataType rowType, DruidQuery query
    ) {
      final RexCall call = (RexCall) rexNode;
      final String arg = DruidExpressions.toDruidExpression(call.getOperands().get(0), rowType, query);
      if (arg == null) {
        return null;
      }

      final String indexStart;
      final String length;
      // SQL is 1-indexed, Druid is 0-indexed.
      if (!call.getOperands().get(1).isA(SqlKind.LITERAL)) {
        final String indexExp = DruidExpressions.toDruidExpression(call.getOperands().get(1), rowType, query);
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
          length = DruidExpressions.toDruidExpression(call.getOperands().get(2), rowType, query);
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
      return DruidQuery.format("substring(%s, %s, %s)", arg, indexStart, length);
    }
  }

  /**
   * Operator conversion form Hive TRUNC UDF to Druid Date Time UDFs.
   */
  public static class DruidDateTruncOperatorConversion
      implements org.apache.calcite.adapter.druid.DruidSqlOperatorConverter {

    @Override public SqlOperator calciteOperator() {
      return HiveTruncSqlOperator.INSTANCE;
    }

    @Nullable @Override public String toDruidExpression(RexNode rexNode, RelDataType rowType, DruidQuery query
    ) {
      final RexCall call = (RexCall) rexNode;
      //can handle only case trunc date type
      if (call.getOperands().size() < 1) {
        throw new IllegalStateException("trunc() requires at least 1 argument, got " + call.getOperands().size());
      }
      if (call.getOperands().size() == 1) {
        final String arg = DruidExpressions.toDruidExpression(call.getOperands().get(0), rowType, query);
        if (arg == null) {
          return null;
        }
        if (SqlTypeUtil.isDatetime(call.getOperands().get(0).getType())) {
          final TimeZone tz = timezoneId(query, call.getOperands().get(0));
          return applyTimestampFormat(
              DruidExpressions.applyTimestampFloor(arg, Period.days(1).toString(), "", tz),
              YYYY_MM_DD,
              tz);
        }
        return null;
      } else if (call.getOperands().size() == 2) {
        final String arg = DruidExpressions.toDruidExpression(call.getOperands().get(0), rowType, query);
        if (arg == null) {
          return null;
        }
        String granularity = DruidExpressions.toDruidExpression(call.getOperands().get(1), rowType, query);
        if (granularity == null) {
          return null;
        }
        final String unit;
        if ("'MONTH'".equals(granularity) || "'MON'".equals(granularity) || "'MM'".equals(granularity)) {
          unit = Period.months(1).toString();
        } else if ("'YEAR'".equals(granularity) || "'YYYY'".equals(granularity) || "'YY'".equals(granularity)) {
          unit = Period.years(1).toString();
        } else if ("'QUARTER'".equals(granularity) || "'Q'".equals(granularity)) {
          unit = Period.months(3).toString();
        } else {
          unit = null;
        }
        if (unit == null) {
          //bail out can not infer unit
          return null;
        }
        final TimeZone tz = timezoneId(query, call.getOperands().get(0));
        return applyTimestampFormat(
            DruidExpressions.applyTimestampFloor(arg, unit, "", tz),
            YYYY_MM_DD,
            tz);
      }
      return null;
    }
  }

  /**
   * Expression operator conversion form Hive TO_DATE operator to Druid Date cast.
   */
  public static class DruidToDateOperatorConversion
      implements org.apache.calcite.adapter.druid.DruidSqlOperatorConverter {

    @Override public SqlOperator calciteOperator() {
      return HiveToDateSqlOperator.INSTANCE;
    }

    @Nullable @Override public String toDruidExpression(RexNode rexNode, RelDataType rowType, DruidQuery query
    ) {
      final RexCall call = (RexCall) rexNode;
      if (call.getOperands().size() != 1) {
        throw new IllegalStateException("to_date() requires 1 argument, got " + call.getOperands().size());
      }
      final String arg = DruidExpressions.toDruidExpression(call.getOperands().get(0), rowType, query);
      if (arg == null) {
        return null;
      }
      return DruidExpressions.applyTimestampFloor(
          arg,
          Period.days(1).toString(),
          "",
          timezoneId(query, call.getOperands().get(0)));
    }
  }

  public static class DruidUnixTimestampOperatorConversion
      implements org.apache.calcite.adapter.druid.DruidSqlOperatorConverter {

    @Override public SqlOperator calciteOperator() {
      return HiveToUnixTimestampSqlOperator.INSTANCE;
    }

    @Nullable @Override public String toDruidExpression(RexNode rexNode, RelDataType rowType, DruidQuery query
    ) {
      final RexCall call = (RexCall) rexNode;
      final String arg0 = DruidExpressions.toDruidExpression(call.getOperands().get(0), rowType, query);
      if (arg0 == null) {
        return null;
      }
      if (SqlTypeUtil.isDatetime((call.getOperands().get(0).getType()))) {
        // Timestamp is represented as long internally no need to any thing here
        return DruidExpressions.functionCall("div", ImmutableList.of(arg0, DruidExpressions.numberLiteral(1000)));
      }
      // dealing with String type
      final String format = call.getOperands().size() == 2 ? DruidExpressions
          .toDruidExpression(call.getOperands().get(1), rowType, query) : DEFAULT_TS_FORMAT;
      return DruidExpressions
          .functionCall("unix_timestamp", ImmutableList.of(arg0, DruidExpressions.stringLiteral(format)));
    }
  }

  public static class DruidFormUnixTimeOperatorConversion
      implements org.apache.calcite.adapter.druid.DruidSqlOperatorConverter {

    @Override public SqlOperator calciteOperator() {
      return HiveFromUnixTimeSqlOperator.INSTANCE;
    }

    @Nullable @Override public String toDruidExpression(RexNode rexNode, RelDataType rowType, DruidQuery query
    ) {
      final RexCall call = (RexCall) rexNode;
      if (call.getOperands().size() < 1 || call.getOperands().size() > 2) {
        throw new IllegalStateException("form_unixtime() requires 1 or 2 argument, got " + call.getOperands().size());
      }
      final String arg = DruidExpressions.toDruidExpression(call.getOperands().get(0), rowType, query);
      if (arg == null) {
        return null;
      }

      final String numMillis = DruidQuery.format("(%s * '1000')", arg);
      final String format =
          call.getOperands().size() == 1 ? DruidExpressions.stringLiteral(DEFAULT_TS_FORMAT) : DruidExpressions
              .toDruidExpression(call.getOperands().get(1), rowType, query);
      return DruidExpressions.functionCall("timestamp_format",
          ImmutableList.of(numMillis, format, DruidExpressions.stringLiteral(TimeZone.getTimeZone("UTC").getID()))
      );
    }
  }

  /**
   * Base class for Date Add/Sub operator conversion
   */
  public static class DruidDateArithmeticOperatorConversion
      implements org.apache.calcite.adapter.druid.DruidSqlOperatorConverter {

    private final int direction;
    private final SqlOperator operator;

    public DruidDateArithmeticOperatorConversion(int direction, SqlOperator operator) {
      this.direction = direction;
      this.operator = operator;
      Preconditions.checkArgument(direction == 1 || direction == -1);
    }

    @Override public SqlOperator calciteOperator() {
      return operator;
    }

    @Nullable @Override public String toDruidExpression(RexNode rexNode, RelDataType rowType, DruidQuery query
    ) {
      final RexCall call = (RexCall) rexNode;
      if (call.getOperands().size() != 2) {
        throw new IllegalStateException("date_add/date_sub() requires 2 arguments, got " + call.getOperands().size());
      }
      final String arg0 = DruidExpressions.toDruidExpression(call.getOperands().get(0), rowType, query);
      final String arg1 = DruidExpressions.toDruidExpression(call.getOperands().get(1), rowType, query);
      if (arg0 == null || arg1 == null) {
        return null;
      }

      final String steps = direction == -1 ? DruidQuery.format("-( %s )", arg1) : arg1;
      return DruidExpressions.functionCall(
          "timestamp_shift",
          ImmutableList.of(
              arg0,
              DruidExpressions.stringLiteral("P1D"),
              steps,
              DruidExpressions.stringLiteral(timezoneId(query, call.getOperands().get(0)).getID())));
    }
  }

  /**
   * utility function to extract timezone id from Druid query
   * @param query Druid Rel
   * @return time zone
   */
  private static TimeZone timezoneId(final DruidQuery query, final RexNode arg) {
    return arg.getType().getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
        ? TimeZone.getTimeZone(
        query.getTopNode().getCluster().getPlanner().getContext().unwrap(CalciteConnectionConfig.class).timeZone()) :
        TimeZone.getTimeZone("UTC");
  }

  private static String applyTimestampFormat(String arg, String format, TimeZone timeZone) {
    return DruidExpressions.functionCall("timestamp_format",
        ImmutableList.of(arg, DruidExpressions.stringLiteral(format), DruidExpressions.stringLiteral(timeZone.getID()))
    );
  }

  public static class UnaryFunctionOperatorConversion implements org.apache.calcite.adapter.druid.DruidSqlOperatorConverter {

    private final SqlOperator operator;
    private final String druidOperator;

    public UnaryFunctionOperatorConversion(SqlOperator operator, String druidOperator) {
      this.operator = operator;
      this.druidOperator = druidOperator;
    }

    @Override public SqlOperator calciteOperator() {
      return operator;
    }

    @Override public String toDruidExpression(RexNode rexNode, RelDataType rowType,
        DruidQuery druidQuery) {
      final RexCall call = (RexCall) rexNode;

      final List<String> druidExpressions = DruidExpressions.toDruidExpressions(
          druidQuery, rowType,
          call.getOperands());

      if (druidExpressions == null) {
        return null;
      }

      return DruidQuery.format("%s(%s)", druidOperator, Iterables.getOnlyElement(druidExpressions));
    }
  }
}
