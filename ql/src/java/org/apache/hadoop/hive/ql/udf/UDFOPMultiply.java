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
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColMultiplyDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColMultiplyDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColMultiplyLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColMultiplyLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleScalarMultiplyDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleScalarMultiplyLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColMultiplyDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColMultiplyDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColMultiplyLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColMultiplyLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongScalarMultiplyDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongScalarMultiplyLongColumn;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * UDFOPMultiply.
 *
 */
@Description(name = "*", value = "a _FUNC_ b - Multiplies a by b")
@VectorizedExpressions({LongColMultiplyLongColumn.class, LongColMultiplyDoubleColumn.class,
    DoubleColMultiplyLongColumn.class, DoubleColMultiplyDoubleColumn.class,
    LongColMultiplyLongScalar.class, LongColMultiplyDoubleScalar.class,
    DoubleColMultiplyLongScalar.class, DoubleColMultiplyDoubleScalar.class,
    LongScalarMultiplyLongColumn.class, LongScalarMultiplyDoubleColumn.class,
    DoubleScalarMultiplyLongColumn.class, DoubleScalarMultiplyDoubleColumn.class})
public class UDFOPMultiply extends UDFBaseNumericOp {

  public UDFOPMultiply() {
  }

  @Override
  public ByteWritable evaluate(ByteWritable a, ByteWritable b) {
    // LOG.info("Get input " + a.getClass() + ":" + a + " " + b.getClass() + ":"
    // + b);
    if ((a == null) || (b == null)) {
      return null;
    }

    byteWritable.set((byte) (a.get() * b.get()));
    return byteWritable;
  }

  @Override
  public ShortWritable evaluate(ShortWritable a, ShortWritable b) {
    // LOG.info("Get input " + a.getClass() + ":" + a + " " + b.getClass() + ":"
    // + b);
    if ((a == null) || (b == null)) {
      return null;
    }

    shortWritable.set((short) (a.get() * b.get()));
    return shortWritable;
  }

  @Override
  public IntWritable evaluate(IntWritable a, IntWritable b) {
    // LOG.info("Get input " + a.getClass() + ":" + a + " " + b.getClass() + ":"
    // + b);
    if ((a == null) || (b == null)) {
      return null;
    }

    intWritable.set((a.get() * b.get()));
    return intWritable;
  }

  @Override
  public LongWritable evaluate(LongWritable a, LongWritable b) {
    // LOG.info("Get input " + a.getClass() + ":" + a + " " + b.getClass() + ":"
    // + b);
    if ((a == null) || (b == null)) {
      return null;
    }

    longWritable.set(a.get() * b.get());
    return longWritable;
  }

  @Override
  public FloatWritable evaluate(FloatWritable a, FloatWritable b) {
    // LOG.info("Get input " + a.getClass() + ":" + a + " " + b.getClass() + ":"
    // + b);
    if ((a == null) || (b == null)) {
      return null;
    }

    floatWritable.set(a.get() * b.get());
    return floatWritable;
  }

  @Override
  public DoubleWritable evaluate(DoubleWritable a, DoubleWritable b) {
    // LOG.info("Get input " + a.getClass() + ":" + a + " " + b.getClass() + ":"
    // + b);
    if ((a == null) || (b == null)) {
      return null;
    }

    doubleWritable.set(a.get() * b.get());
    return doubleWritable;
  }

  @Override
  public HiveDecimalWritable evaluate(HiveDecimalWritable a, HiveDecimalWritable b) {
    if ((a == null) || (b == null)) {
      return null;
    }

    HiveDecimal dec = a.getHiveDecimal().multiply(b.getHiveDecimal());
    if (dec == null) {
      return null;
    }

    decimalWritable.set(dec);
    return decimalWritable;
  }
}
