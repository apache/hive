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

package org.apache.hadoop.hive.serde2.typeinfo;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

public class HiveDecimalUtils {

  public static HiveDecimal enforcePrecisionScale(HiveDecimal dec, DecimalTypeInfo typeInfo) {
    return HiveDecimal.enforcePrecisionScale(dec, typeInfo.precision(),
        typeInfo.scale());
  }

  public static HiveDecimalWritable enforcePrecisionScale(HiveDecimalWritable writable,
      DecimalTypeInfo typeInfo) {
    if (writable == null) {
      return null;
    }

    HiveDecimalWritable result = new HiveDecimalWritable(writable);
    result.mutateEnforcePrecisionScale(typeInfo.precision(), typeInfo.scale());
    return (result.isSet() ? result : null);
  }

  public static void validateParameter(int precision, int scale) {
    if (precision < 1 || precision > HiveDecimal.MAX_PRECISION) {
      throw new IllegalArgumentException("Decimal precision out of allowed range [1," +
          HiveDecimal.MAX_PRECISION + "]");
    }

    if (scale < 0 || scale > HiveDecimal.MAX_SCALE) {
      throw new IllegalArgumentException("Decimal scale out of allowed range [0," +
          HiveDecimal.MAX_SCALE + "]");
    }

    if (precision < scale) {
      throw new IllegalArgumentException("Decimal scale must be less than or equal to precision");
    }
  }

  /**
   * Need to keep consistent with JdbcColumn.columnPrecision
   *
   */
  public static int getPrecisionForType(PrimitiveTypeInfo typeInfo) {
    switch (typeInfo.getPrimitiveCategory()) {
    case DECIMAL:
      return ((DecimalTypeInfo)typeInfo).precision();
    case FLOAT:
      return 7;
    case DOUBLE:
      return 15;
    case BYTE:
      return 3;
    case SHORT:
      return 5;
    case INT:
      return 10;
    case LONG:
      return 19;
    case VOID:
      return 1;
    default:
      return HiveDecimal.SYSTEM_DEFAULT_PRECISION;
    }
  }

  /**
   * Need to keep consistent with JdbcColumn.columnScale()
   *
   */
  public static int getScaleForType(PrimitiveTypeInfo typeInfo) {
    switch (typeInfo.getPrimitiveCategory()) {
    case DECIMAL:
      return ((DecimalTypeInfo)typeInfo).scale();
    case FLOAT:
      return 7;
    case DOUBLE:
      return 15;
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case VOID:
      return 0;
    default:
      return HiveDecimal.SYSTEM_DEFAULT_SCALE;
    }
  }

  public static TypeInfo getDecimalTypeForPrimitiveCategories(
      PrimitiveTypeInfo a, PrimitiveTypeInfo b) {
    int prec1 = HiveDecimalUtils.getPrecisionForType(a);
    int prec2 = HiveDecimalUtils.getPrecisionForType(b);
    int scale1 = HiveDecimalUtils.getScaleForType(a);
    int scale2 = HiveDecimalUtils.getScaleForType(b);
    int intPart = Math.max(prec1 - scale1, prec2 - scale2);
    int decPart = Math.max(scale1, scale2);
    int prec =  Math.min(intPart + decPart, HiveDecimal.MAX_PRECISION);
    int scale = Math.min(decPart, HiveDecimal.MAX_PRECISION - intPart);
    return TypeInfoFactory.getDecimalTypeInfo(prec, scale);
  }

  public static DecimalTypeInfo getDecimalTypeForPrimitiveCategory(PrimitiveTypeInfo a) {
    if (a instanceof DecimalTypeInfo) return (DecimalTypeInfo)a;
    int prec = HiveDecimalUtils.getPrecisionForType(a);
    int scale = HiveDecimalUtils.getScaleForType(a);
    prec =  Math.min(prec, HiveDecimal.MAX_PRECISION);
    scale = Math.min(scale, HiveDecimal.MAX_PRECISION - (prec - scale));
    return TypeInfoFactory.getDecimalTypeInfo(prec, scale);
  }
}
