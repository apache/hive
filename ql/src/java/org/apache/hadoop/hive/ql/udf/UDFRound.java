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

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * UDFRound.
 *
 */
@Description(name = "round",
    value = "_FUNC_(x[, d]) - round x to d decimal places",
    extended = "Example:\n"
    + "  > SELECT _FUNC_(12.3456, 1) FROM src LIMIT 1;\n" + "  12.3'")
public class UDFRound extends UDF {
  private final HiveDecimalWritable decimalWritable = new HiveDecimalWritable();
  private final DoubleWritable doubleWritable = new DoubleWritable();
  private final LongWritable longWritable = new LongWritable();
  private final IntWritable intWritable = new IntWritable();
  private final ShortWritable shortWritable = new ShortWritable();
  private final ByteWritable byteWritable = new ByteWritable();

  public UDFRound() {
  }

  private DoubleWritable evaluate(DoubleWritable n, int i) {
    double d = n.get();
    if (Double.isNaN(d) || Double.isInfinite(d)) {
      doubleWritable.set(d);
    } else {
      doubleWritable.set(BigDecimal.valueOf(d).setScale(i,
              RoundingMode.HALF_UP).doubleValue());
    }
    return doubleWritable;
  }

  public DoubleWritable evaluate(DoubleWritable n) {
    if (n == null) {
      return null;
    }
    return evaluate(n, 0);
  }

  public DoubleWritable evaluate(DoubleWritable n, IntWritable i) {
    if ((n == null) || (i == null)) {
      return null;
    }
    return evaluate(n, i.get());
  }

  private HiveDecimalWritable evaluate(HiveDecimalWritable n, int i) {
    if (n == null) {
      return null;
    }
    HiveDecimal bd = n.getHiveDecimal();
    try {
      bd = n.getHiveDecimal().setScale(i, HiveDecimal.ROUND_HALF_UP);
    } catch (NumberFormatException e) {
      return null;
    }
    decimalWritable.set(bd);
    return decimalWritable;
  }

  public HiveDecimalWritable evaluate(HiveDecimalWritable n) {
    return evaluate(n, 0);
  }

  public HiveDecimalWritable evaluate(HiveDecimalWritable n, IntWritable i) {
    if (i == null) {
      return null;
    }
    return evaluate(n, i.get());
  }

  public LongWritable evaluate(LongWritable n) {
    if (n == null) {
      return null;
    }
    longWritable.set(BigDecimal.valueOf(n.get()).setScale(0,
            RoundingMode.HALF_UP).longValue());
    return longWritable;
  }

  public IntWritable evaluate(IntWritable n) {
    if (n == null) {
      return null;
    }
    intWritable.set(BigDecimal.valueOf(n.get()).setScale(0,
            RoundingMode.HALF_UP).intValue());
    return intWritable;
  }

  public ShortWritable evaluate(ShortWritable n) {
    if (n == null) {
      return null;
    }
    shortWritable.set(BigDecimal.valueOf(n.get()).setScale(0,
            RoundingMode.HALF_UP).shortValue());
    return shortWritable;
  }

  public ByteWritable evaluate(ByteWritable n) {
    if (n == null) {
      return null;
    }
    byteWritable.set(BigDecimal.valueOf(n.get()).setScale(0,
            RoundingMode.HALF_UP).byteValue());
    return byteWritable;
  }

}

