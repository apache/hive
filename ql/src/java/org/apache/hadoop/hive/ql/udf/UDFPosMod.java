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

package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.PosModDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.PosModLongToLong;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * class for computing positive modulo. Used for positive_mod command in Cli See
 * {org.apache.hadoop.hive.ql.udf.UDFOPMod} See
 * {org.apache.hadoop.hive.ql.exec.FunctionRegistry}
 */
@Description(name = "pmod", value = "a _FUNC_ b - Compute the positive modulo")
@VectorizedExpressions({PosModLongToLong.class, PosModDoubleToDouble.class})
public class UDFPosMod extends UDFBaseNumericOp {

  public UDFPosMod() {
  }

  @Override
  public ByteWritable evaluate(ByteWritable a, ByteWritable b) {
    // LOG.info("Get input " + a.getClass() + ":" + a + " " + b.getClass() + ":"
    // + b);
    if (a == null || b == null || b.get() == 0) {
      return null;
    }

    byteWritable.set((byte) (((a.get() % b.get()) + b.get()) % b.get()));
    return byteWritable;
  }

  @Override
  public ShortWritable evaluate(ShortWritable a, ShortWritable b) {
    // LOG.info("Get input " + a.getClass() + ":" + a + " " + b.getClass() + ":"
    // + b);
    if (a == null || b == null || b.get() == 0) {
      return null;
    }

    shortWritable.set((short) (((a.get() % b.get()) + b.get()) % b.get()));
    return shortWritable;
  }

  @Override
  public IntWritable evaluate(IntWritable a, IntWritable b) {
    // LOG.info("Get input " + a.getClass() + ":" + a + " " + b.getClass() + ":"
    // + b);
    if (a == null || b == null || b.get() == 0) {
      return null;
    }

    intWritable.set((((a.get() % b.get()) + b.get()) % b.get()));
    return intWritable;
  }

  @Override
  public LongWritable evaluate(LongWritable a, LongWritable b) {
    // LOG.info("Get input " + a.getClass() + ":" + a + " " + b.getClass() + ":"
    // + b);
    if (a == null || b == null || b.get() == 0L) {
      return null;
    }

    longWritable.set(((a.get() % b.get()) + b.get()) % b.get());
    return longWritable;
  }

  @Override
  public FloatWritable evaluate(FloatWritable a, FloatWritable b) {
    // LOG.info("Get input " + a.getClass() + ":" + a + " " + b.getClass() + ":"
    // + b);
    if (a == null || b == null || b.get() == 0.0f) {
      return null;
    }

    floatWritable.set(((a.get() % b.get()) + b.get()) % b.get());
    return floatWritable;
  }

  @Override
  public DoubleWritable evaluate(DoubleWritable a, DoubleWritable b) {
    // LOG.info("Get input " + a.getClass() + ":" + a + " " + b.getClass() + ":"
    // + b);
    if (a == null || b == null || b.get() == 0.0) {
      return null;
    }

    doubleWritable.set(((a.get() % b.get()) + b.get()) % b.get());
    return doubleWritable;
  }

  @Override
  public HiveDecimalWritable evaluate(HiveDecimalWritable a, HiveDecimalWritable b) {
    if ((a == null) || (b == null)) {
      return null;
    }

    HiveDecimal av = a.getHiveDecimal();
    HiveDecimal bv = b.getHiveDecimal();

    if (bv.compareTo(HiveDecimal.ZERO) == 0) {
      return null;
    }

    HiveDecimal dec = av.remainder(bv).add(bv).remainder(bv);
    if (dec == null) {
      return null;
    }

    decimalWritable.set(dec);
    return decimalWritable;
  }
}
