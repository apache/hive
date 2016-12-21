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
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.*;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

@Description(name = "-", value = "a _FUNC_ b - Returns the difference a-b")
public class GenericUDFOPNumericMinus extends GenericUDFBaseNumeric {

  public GenericUDFOPNumericMinus() {
    super();
    this.opDisplayName = "-";
  }

  @Override
  protected ByteWritable evaluate(ByteWritable left, ByteWritable right) {
    byteWritable.set((byte)(left.get() - right.get()));
    return byteWritable;
  }

  @Override
  protected ShortWritable evaluate(ShortWritable left, ShortWritable right) {
    shortWritable.set((short)(left.get() - right.get()));
    return shortWritable;
  }

  @Override
  protected IntWritable evaluate(IntWritable left, IntWritable right) {
    intWritable.set(left.get() - right.get());
    return intWritable;
  }

  @Override
  protected LongWritable evaluate(LongWritable left, LongWritable right) {
    longWritable.set(left.get() - right.get());
    return longWritable;
  }

  @Override
  protected FloatWritable evaluate(FloatWritable left, FloatWritable right) {
    floatWritable.set(left.get() - right.get());
    return floatWritable;
  }

  @Override
  protected DoubleWritable evaluate(DoubleWritable left, DoubleWritable right) {
    doubleWritable.set(left.get() - right.get());
    return doubleWritable;
  }

  @Override
  protected HiveDecimalWritable evaluate(HiveDecimal left, HiveDecimal right) {
    HiveDecimal dec = left.subtract(right);
    if (dec == null) {
      return null;
    }
    decimalWritable.set(dec);
    return decimalWritable;
  }

  @Override
  protected DecimalTypeInfo deriveResultDecimalTypeInfo(int prec1, int scale1, int prec2, int scale2) {
    // From https://msdn.microsoft.com/en-us/library/ms190476.aspx
    // e1 + e2
    // Precision: max(s1, s2) + max(p1-s1, p2-s2) + 1
    // Scale: max(s1, s2)
    int intPart = Math.max(prec1 - scale1, prec2 - scale2);
    int scale = Math.max(scale1, scale2);
    int prec =  intPart + scale + 1;
    return adjustPrecScale(prec, scale);
  }

}
