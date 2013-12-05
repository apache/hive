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
import org.apache.hadoop.hive.ql.exec.vector.expressions.PosModDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.PosModLongToLong;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * class for computing positive modulo. Used for positive_mod command in Cli See
 * {org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPMod} See
 * {org.apache.hadoop.hive.ql.exec.FunctionRegistry}
 */
@Description(name = "pmod", value = "a _FUNC_ b - Compute the positive modulo")
@VectorizedExpressions({PosModLongToLong.class, PosModDoubleToDouble.class})
public class GenericUDFPosMod extends GenericUDFBaseNumeric {

  public GenericUDFPosMod() {
    super();
    this.opDisplayName = "pmod";
  }

  @Override
  protected ByteWritable evaluate(ByteWritable left, ByteWritable right) {
    if (right.get() == 0) {
      return null;
    }
    byteWritable.set((byte) (((left.get() % right.get()) + right.get()) % right.get()));
    return byteWritable;
  }

  @Override
  protected ShortWritable evaluate(ShortWritable left, ShortWritable right) {
    if (right.get() == 0) {
      return null;
    }
    shortWritable.set((short) (((left.get() % right.get()) + right.get()) % right.get()));
    return shortWritable;
  }

  @Override
  protected IntWritable evaluate(IntWritable left, IntWritable right) {
    if (right.get() == 0) {
      return null;
    }
    intWritable.set((((left.get() % right.get()) + right.get()) % right.get()));
    return intWritable;
  }

  @Override
  protected LongWritable evaluate(LongWritable left, LongWritable right) {
    if (right.get() == 0) {
      return null;
    }
    longWritable.set(((left.get() % right.get()) + right.get()) % right.get());
    return longWritable;
  }

  @Override
  protected FloatWritable evaluate(FloatWritable left, FloatWritable right) {
    if (right.get() == 0) {
      return null;
    }
    floatWritable.set(((left.get() % right.get()) + right.get()) % right.get());
    return floatWritable;
  }

  @Override
  protected DoubleWritable evaluate(DoubleWritable left, DoubleWritable right) {
    if (right.get() == 0) {
      return null;
    }
    doubleWritable.set(((left.get() % right.get()) + right.get()) % right.get());
    return doubleWritable;
  }

  @Override
  protected HiveDecimalWritable evaluate(HiveDecimal left, HiveDecimal right) {
    if (right.compareTo(HiveDecimal.ZERO) == 0) {
      return null;
    }

    HiveDecimal dec = left.remainder(right).add(right).remainder(right);
    if (dec == null) {
      return null;
    }

    decimalWritable.set(dec);
    return decimalWritable;
  }

  @Override
  protected DecimalTypeInfo deriveResultDecimalTypeInfo(int prec1, int scale1, int prec2, int scale2) {
    int scale = Math.max(scale1, scale2);
    int prec = Math.min(HiveDecimal.MAX_PRECISION, Math.max(prec1 - scale1, prec2 - scale2) + scale);
    return TypeInfoFactory.getDecimalTypeInfo(prec, scale);
  }

}
