/**
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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongColDivideLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongColDivideLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongScalarDivideLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.*;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * Note that in SQL, the return type of divide is not necessarily the same
 * as the parameters. For example, 3 / 2 = 1.5, not 1. To follow SQL, we always
 * return a decimal for divide.
 */
@Description(name = "/", value = "a _FUNC_ b - Divide a by b", extended = "Example:\n"
    + "  > SELECT 3 _FUNC_ 2 FROM src LIMIT 1;\n" + "  1.5")
@VectorizedExpressions({LongColDivideLongColumn.class, LongColDivideDoubleColumn.class,
  DoubleColDivideLongColumn.class, DoubleColDivideDoubleColumn.class,
  LongColDivideLongScalar.class, LongColDivideDoubleScalar.class,
  DoubleColDivideLongScalar.class, DoubleColDivideDoubleScalar.class,
  LongScalarDivideLongColumn.class, LongScalarDivideDoubleColumn.class,
  DoubleScalarDivideLongColumn.class, DoubleScalarDivideDoubleColumn.class,
  DecimalColDivideDecimalColumn.class, DecimalColDivideDecimalScalar.class,
  DecimalScalarDivideDecimalColumn.class})
public class GenericUDFOPDivide extends GenericUDFBaseNumeric {

  public GenericUDFOPDivide() {
    super();
    this.opDisplayName = "/";
  }

  @Override
  protected PrimitiveTypeInfo deriveResultExactTypeInfo() {
    if (ansiSqlArithmetic) {
      return deriveResultExactTypeInfoAnsiSql();
    }
    return deriveResultExactTypeInfoBackwardsCompat();
  }

  protected PrimitiveTypeInfo deriveResultExactTypeInfoAnsiSql() {
    // No type promotion. Everything goes to decimal.
    return deriveResultDecimalTypeInfo();
  }

  protected PrimitiveTypeInfo deriveResultExactTypeInfoBackwardsCompat() {
    // Preserve existing return type behavior for division:
    // Non-decimal division should return double
    if (leftOI.getPrimitiveCategory() != PrimitiveCategory.DECIMAL
        && rightOI.getPrimitiveCategory() != PrimitiveCategory.DECIMAL) {
      return TypeInfoFactory.doubleTypeInfo;
    }

    return deriveResultDecimalTypeInfo();
  }

  @Override
  protected PrimitiveTypeInfo deriveResultApproxTypeInfo() {
    // Hive 0.12 behavior where double / decimal -> decimal is gone.
    return TypeInfoFactory.doubleTypeInfo;
  }

  @Override
  protected DoubleWritable evaluate(DoubleWritable left, DoubleWritable right) {
    if (right.get() == 0.0) {
      return null;
    }
    doubleWritable.set(left.get() / right.get());
    return doubleWritable;
  }

  @Override
  protected HiveDecimalWritable evaluate(HiveDecimal left, HiveDecimal right) {
    if (right.compareTo(HiveDecimal.ZERO) == 0) {
      return null;
    }

    HiveDecimal dec = left.divide(right);
    if (dec == null) {
      return null;
    }

    decimalWritable.set(dec);
    return decimalWritable;
  }

  @Override
  protected DecimalTypeInfo deriveResultDecimalTypeInfo(int prec1, int scale1, int prec2, int scale2) {
    // From https://msdn.microsoft.com/en-us/library/ms190476.aspx
    // e1 / e2
    // Precision: p1 - s1 + s2 + max(6, s1 + p2 + 1)
    // Scale: max(6, s1 + p2 + 1)
    int intDig = prec1 - scale1 + scale2;
    int scale = Math.max(6, scale1 + prec2 + 1);
    int prec = intDig + scale;
    return adjustPrecScale(prec, scale);
  }

}
