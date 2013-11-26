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
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongColDivideLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongColDivideLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongScalarDivideLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColDivideDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColDivideDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColDivideLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColDivideLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleScalarDivideDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleScalarDivideLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColDivideDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColDivideDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongScalarDivideDoubleColumn;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

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
  DoubleScalarDivideLongColumn.class, DoubleScalarDivideDoubleColumn.class})
public class GenericUDFOPDivide extends GenericUDFBaseNumeric {

  public GenericUDFOPDivide() {
    super();
    this.opDisplayName = "/";
  }

  @Override
  protected PrimitiveTypeInfo deriveResultExactTypeInfo() {
    // No type promotion. Everything goes to decimal.
    return deriveResultDecimalTypeInfo();
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
  protected HiveDecimalWritable evaluate(HiveDecimalWritable left, HiveDecimalWritable right) {
    HiveDecimal hd1 = left.getHiveDecimal();
    HiveDecimal hd2 = right.getHiveDecimal();
    if (hd2.compareTo(HiveDecimal.ZERO) == 0) {
      return null;
    }
    HiveDecimal dec = hd1.divide(hd2);
    decimalWritable.set(dec);
    return decimalWritable;
  }

  @Override
  protected DecimalTypeInfo deriveResultDecimalTypeInfo(int prec1, int scale1, int prec2, int scale2) {
    int scale = Math.min(HiveDecimal.MAX_SCALE, Math.max(6, scale1 + prec2 + 1));
    int prec = Math.min(HiveDecimal.MAX_PRECISION, prec1 - scale1 + scale2 + scale);
    return TypeInfoFactory.getDecimalTypeInfo(prec, scale);
  }

}
