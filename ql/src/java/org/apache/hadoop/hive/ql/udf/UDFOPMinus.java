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
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColSubtractDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColSubtractDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColSubtractLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColSubtractLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleScalarSubtractDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleScalarSubtractLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColSubtractDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColSubtractDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColSubtractLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColSubtractLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongScalarSubtractDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongScalarSubtractLongColumn;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * UDFOPMinus.
 *
 */
@Description(name = "-", value = "a _FUNC_ b - Returns the difference a-b")
@VectorizedExpressions({LongColSubtractLongColumn.class, LongColSubtractDoubleColumn.class,
  DoubleColSubtractLongColumn.class, DoubleColSubtractDoubleColumn.class,
  LongColSubtractLongScalar.class, LongColSubtractDoubleScalar.class,
  DoubleColSubtractLongScalar.class, DoubleColSubtractDoubleScalar.class,
  LongScalarSubtractLongColumn.class, LongScalarSubtractDoubleColumn.class,
  DoubleScalarSubtractLongColumn.class, DoubleScalarSubtractDoubleColumn.class})
public class UDFOPMinus extends UDFBaseNumericOp {

  public UDFOPMinus() {
  }

  @Override
  public ByteWritable evaluate(ByteWritable a, ByteWritable b) {
    // LOG.info("Get input " + a.getClass() + ":" + a + " " + b.getClass() + ":"
    // + b);
    if ((a == null) || (b == null)) {
      return null;
    }

    byteWritable.set((byte) (a.get() - b.get()));
    return byteWritable;
  }

  @Override
  public ShortWritable evaluate(ShortWritable a, ShortWritable b) {
    // LOG.info("Get input " + a.getClass() + ":" + a + " " + b.getClass() + ":"
    // + b);
    if ((a == null) || (b == null)) {
      return null;
    }

    shortWritable.set((short) (a.get() - b.get()));
    return shortWritable;
  }

  @Override
  public IntWritable evaluate(IntWritable a, IntWritable b) {
    // LOG.info("Get input " + a.getClass() + ":" + a + " " + b.getClass() + ":"
    // + b);
    if ((a == null) || (b == null)) {
      return null;
    }

    intWritable.set((a.get() - b.get()));
    return intWritable;
  }

  @Override
  public LongWritable evaluate(LongWritable a, LongWritable b) {
    // LOG.info("Get input " + a.getClass() + ":" + a + " " + b.getClass() + ":"
    // + b);
    if ((a == null) || (b == null)) {
      return null;
    }

    longWritable.set(a.get() - b.get());
    return longWritable;
  }

  @Override
  public FloatWritable evaluate(FloatWritable a, FloatWritable b) {
    // LOG.info("Get input " + a.getClass() + ":" + a + " " + b.getClass() + ":"
    // + b);
    if ((a == null) || (b == null)) {
      return null;
    }

    floatWritable.set(a.get() - b.get());
    return floatWritable;
  }

  @Override
  public DoubleWritable evaluate(DoubleWritable a, DoubleWritable b) {
    // LOG.info("Get input " + a.getClass() + ":" + a + " " + b.getClass() + ":"
    // + b);
    if ((a == null) || (b == null)) {
      return null;
    }

    doubleWritable.set(a.get() - b.get());
    return doubleWritable;
  }

  @Override
  public HiveDecimalWritable evaluate(HiveDecimalWritable a, HiveDecimalWritable b) {
    if ((a == null) || (b == null)) {
      return null;
    }

    HiveDecimal dec = a.getHiveDecimal().subtract(b.getHiveDecimal());
    if (dec == null) {
      return null;
    }

    decimalWritable.set(dec);
    return decimalWritable;
  }
}
