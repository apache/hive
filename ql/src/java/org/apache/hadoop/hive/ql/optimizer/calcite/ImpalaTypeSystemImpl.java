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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.ImpalaTypeConverter;
import org.apache.impala.analysis.ArithmeticExpr;
import org.apache.impala.analysis.TypesUtil;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;

public class ImpalaTypeSystemImpl extends RelDataTypeSystemImpl {
  private static final int MAX_DECIMAL_PRECISION     = 38;
  private static final int MAX_DECIMAL_SCALE         = 38;
  private static final int DEFAULT_DECIMAL_PRECISION = 10;
  // STRING type in FENG is represented as VARCHAR with precision Integer.MAX_VALUE.
  // In turn, the max VARCHAR precision should be 65535. However, the value is not
  // used for validation, but rather only internally by the optimizer to know the max
  // precision supported by the system. Thus, no VARCHAR precision should fall between
  // 65535 and Integer.MAX_VALUE; the check for VARCHAR precision is done in Impala.
  private static final int MAX_CHAR_PRECISION        = Integer.MAX_VALUE;
  private static final int DEFAULT_VARCHAR_PRECISION = 65535;
  private static final int DEFAULT_CHAR_PRECISION    = 255;
  private static final int MAX_BINARY_PRECISION      = Integer.MAX_VALUE;
  private static final int MAX_TIMESTAMP_PRECISION   = 9;
  private static final int MAX_TIMESTAMP_WITH_LOCAL_TIME_ZONE_PRECISION = 15; // Up to nanos
  private static final int DEFAULT_TINYINT_PRECISION  = 3;
  private static final int DEFAULT_SMALLINT_PRECISION = 5;
  private static final int DEFAULT_INTEGER_PRECISION  = 10;
  private static final int DEFAULT_BIGINT_PRECISION   = 19;
  private static final int DEFAULT_FLOAT_PRECISION    = 7;
  private static final int DEFAULT_DOUBLE_PRECISION   = 15;


  @Override
  public int getMaxScale(SqlTypeName typeName) {
    switch (typeName) {
    case DECIMAL:
      return getMaxNumericScale();
    case INTERVAL_YEAR:
    case INTERVAL_MONTH:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      return SqlTypeName.MAX_INTERVAL_FRACTIONAL_SECOND_PRECISION;
    default:
      return -1;
    }
  }

  @Override
  public int getDefaultPrecision(SqlTypeName typeName) {
    switch (typeName) {
    // Impala will always require user to specify exact sizes for char, varchar;
    // Binary doesn't need any sizes; Decimal has the default of 10.
    case BINARY:
    case VARBINARY:
      return RelDataType.PRECISION_NOT_SPECIFIED;
    case TIME:
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      return getMaxPrecision(typeName);
    case CHAR:
      return DEFAULT_CHAR_PRECISION;
    case VARCHAR:
      return DEFAULT_VARCHAR_PRECISION;
    case DECIMAL:
      return DEFAULT_DECIMAL_PRECISION;
    case INTERVAL_YEAR:
    case INTERVAL_MONTH:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      return SqlTypeName.DEFAULT_INTERVAL_START_PRECISION;
    case TINYINT:
      return DEFAULT_TINYINT_PRECISION;
    case SMALLINT:
      return DEFAULT_SMALLINT_PRECISION;
    case INTEGER:
      return DEFAULT_INTEGER_PRECISION;
    case BIGINT:
      return DEFAULT_BIGINT_PRECISION;
    case FLOAT:
      return DEFAULT_FLOAT_PRECISION;
    case DOUBLE:
      return DEFAULT_DOUBLE_PRECISION;
    default:
      return -1;
    }
  }

  @Override
  public int getMaxPrecision(SqlTypeName typeName) {
    switch (typeName) {
    case DECIMAL:
      return getMaxNumericPrecision();
    case VARCHAR:
    case CHAR:
      return MAX_CHAR_PRECISION;
    case VARBINARY:
    case BINARY:
      return MAX_BINARY_PRECISION;
    case TIME:
    case TIMESTAMP:
      return MAX_TIMESTAMP_PRECISION;
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      return MAX_TIMESTAMP_WITH_LOCAL_TIME_ZONE_PRECISION;
    case INTERVAL_YEAR:
    case INTERVAL_MONTH:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      return SqlTypeName.MAX_INTERVAL_START_PRECISION;
    default:
      return getDefaultPrecision(typeName);
    }
  }

  @Override
  public int getMaxNumericScale() {
    return MAX_DECIMAL_SCALE;
  }

  @Override
  public int getMaxNumericPrecision() {
    return MAX_DECIMAL_PRECISION;
  }

  @Override
  public boolean isSchemaCaseSensitive() {
    return false;
  }

  @Override
  public RelDataType deriveSumType(RelDataTypeFactory typeFactory,
      RelDataType argumentType) {
    // When adding two decimals, Impala sets the precision to 38, which differs from Hive.
    switch (argumentType.getSqlTypeName()) {
      case DECIMAL:
        return typeFactory.createSqlType(
            SqlTypeName.DECIMAL,
            MAX_DECIMAL_PRECISION,
            argumentType.getScale());
    }
    return argumentType;
  }

  public static RelDataType deriveArithmeticType(RelDataTypeFactory typeFactory,
      RelDataType type1, RelDataType type2, ArithmeticExpr.Operator op) {
    try {
      Type t1 = ImpalaTypeConverter.createImpalaType(type1);
      Type t2 = ImpalaTypeConverter.createImpalaType(type2);
      // Call out to Impala code to get the correct derived precision on arithmetic operations.
      Type retType = TypesUtil.getArithmeticResultType(t1, t2, op, true);
      SqlTypeName sqlTypeName = ImpalaTypeConverter.getRelDataType(retType).getSqlTypeName();
      RelDataType preNullableType =
          (sqlTypeName == SqlTypeName.DECIMAL)
              ? typeFactory.createSqlType(sqlTypeName,
                  retType.getPrecision(), retType.getDecimalDigits())
              : typeFactory.createSqlType(sqlTypeName);
      boolean isNullable = type1.isNullable() || type2.isNullable();
      return typeFactory.createTypeWithNullability(preNullableType, isNullable);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static RelDataType derivePlusType(RelDataTypeFactory typeFactory,
      RelDataType type1, RelDataType type2, ArithmeticExpr.Operator op) {
    if (SqlTypeName.INTERVAL_TYPES.contains(type1.getSqlTypeName())) {
      return type2;
    }
    if (SqlTypeName.INTERVAL_TYPES.contains(type2.getSqlTypeName())) {
      return type1;
    }
    return deriveArithmeticType(typeFactory, type1, type2, op);
  }

  @Override
  public RelDataType deriveDecimalPlusType(RelDataTypeFactory typeFactory,
      RelDataType type1, RelDataType type2) {
    return derivePlusType(typeFactory, type1, type2, ArithmeticExpr.Operator.ADD);
  }

  @Override
  public RelDataType deriveDecimalMultiplyType(RelDataTypeFactory typeFactory,
      RelDataType type1, RelDataType type2) {
    return deriveArithmeticType(typeFactory, type1, type2, ArithmeticExpr.Operator.MULTIPLY);
  }

  @Override
  public RelDataType deriveDecimalDivideType(RelDataTypeFactory typeFactory,
      RelDataType type1, RelDataType type2) {
    return deriveArithmeticType(typeFactory, type1, type2, ArithmeticExpr.Operator.DIVIDE);
  }

  @Override
  public RelDataType deriveAvgAggType(RelDataTypeFactory typeFactory,
      RelDataType argumentType) {
    try {
      // This code is similar to code in Impala's FunctionCallExpr
      // XXX: should we refactor the code within Impala?
      if (argumentType.getSqlTypeName() == SqlTypeName.DECIMAL) {
        ScalarType t1 = (ScalarType) ImpalaTypeConverter.createImpalaType(argumentType);
        int digitsBefore = t1.decimalPrecision() - t1.decimalScale();
        int digitsAfter = t1.decimalScale();
        int resultScale = Math.max(ScalarType.MIN_ADJUSTED_SCALE, digitsAfter);
        int resultPrecision = digitsBefore + resultScale;
        return typeFactory.createSqlType(SqlTypeName.DECIMAL, resultPrecision, resultScale);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return super.deriveAvgAggType(typeFactory, argumentType);
  }

  public static ArithmeticExpr.Operator getImpalaArithOp(SqlKind kind) {
    switch (kind) {
      case PLUS:
        return ArithmeticExpr.Operator.ADD;
      case MINUS:
        return ArithmeticExpr.Operator.SUBTRACT;
      case DIVIDE:
        return ArithmeticExpr.Operator.DIVIDE;
      case TIMES:
        return ArithmeticExpr.Operator.MULTIPLY;
      default:
        throw new RuntimeException("Op not supported for impala: " + kind);
    }
  }
}
