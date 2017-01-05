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
import org.apache.hadoop.hive.ql.exec.vector.expressions.BRoundWithNumDigitsDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FuncBRoundWithNumDigitsDecimalToDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncBRoundDecimalToDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncBRoundDoubleToDouble;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

@Description(name = "bround",
value = "_FUNC_(x[, d]) - round x to d decimal places using HALF_EVEN rounding mode.",
extended = "Banker's rounding. The value is rounded to the nearest even number. Also known as Gaussian rounding.\n"
  + "Example:\n"
  + "  > SELECT _FUNC_(12.25, 1);\n  12.2")
@VectorizedExpressions({ FuncBRoundDoubleToDouble.class, BRoundWithNumDigitsDoubleToDouble.class,
    FuncBRoundWithNumDigitsDecimalToDecimal.class, FuncBRoundDecimalToDecimal.class })
public class GenericUDFBRound extends GenericUDFRound {

  @Override
  protected HiveDecimalWritable round(HiveDecimalWritable inputDecWritable, int scale) {
    HiveDecimalWritable result = new HiveDecimalWritable(inputDecWritable);
    result.mutateSetScale(scale, HiveDecimal.ROUND_HALF_EVEN);
    return result;
  }

  @Override
  protected long round(long input, int scale) {
    return RoundUtils.bround(input, scale);
  }

  @Override
  protected double round(double input, int scale) {
    return RoundUtils.bround(input, scale);
  }

  @Override
  protected DoubleWritable round(DoubleWritable input, int scale) {
    double d = input.get();
    if (Double.isNaN(d) || Double.isInfinite(d)) {
      return new DoubleWritable(d);
    } else {
      return new DoubleWritable(RoundUtils.bround(d, scale));
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("bround", children);
  }
}
