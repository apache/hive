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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.udf.generic.RoundUtils;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * Utility functions for vector operations on decimal64 values.
 */
public class Decimal64Util {

  public static long getDecimal64AbsMaxFromDecimalTypeString(String typeString) {
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeString);
    if (!(typeInfo instanceof DecimalTypeInfo)) {
      throw new RuntimeException(
          "Expected decimal type but found " + typeInfo.toString());
    }
    DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
    final int precision = decimalTypeInfo.precision();
    if (!HiveDecimalWritable.isPrecisionDecimal64(precision)) {
      throw new RuntimeException(
          "Expected decimal type " + typeInfo.toString() +
          " to have a decimal64 precision (i.e. <= " + HiveDecimalWritable.DECIMAL64_DECIMAL_DIGITS + ")");
    }
    return HiveDecimalWritable.getDecimal64AbsMax(precision);
  }
}
