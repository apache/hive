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

package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDecimalToLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastStringToLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.CastDoubleToLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastTimestampToLong;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFToLong.
 *
 */
@VectorizedExpressions({CastTimestampToLong.class, CastDoubleToLong.class,
    CastDecimalToLong.class, CastStringToLong.class})
public class UDFToLong extends UDF {
  private final LongWritable longWritable = new LongWritable();

  public UDFToLong() {
  }

  /**
   * Convert from void to a long. This is called for CAST(... AS BIGINT)
   *
   * @param i
   *          The void value to convert
   * @return LongWritable
   */
  public LongWritable evaluate(NullWritable i) {
    return null;
  }

  /**
   * Convert from boolean to a long. This is called for CAST(... AS BIGINT)
   *
   * @param i
   *          The boolean value to convert
   * @return LongWritable
   */
  public LongWritable evaluate(BooleanWritable i) {
    if (i == null) {
      return null;
    } else {
      longWritable.set(i.get() ? (long) 1 : (long) 0);
      return longWritable;
    }
  }

  /**
   * Convert from byte to a long. This is called for CAST(... AS BIGINT)
   *
   * @param i
   *          The byte value to convert
   * @return LongWritable
   */
  public LongWritable evaluate(ByteWritable i) {
    if (i == null) {
      return null;
    } else {
      longWritable.set(i.get());
      return longWritable;
    }
  }

  /**
   * Convert from short to a long. This is called for CAST(... AS BIGINT)
   *
   * @param i
   *          The short value to convert
   * @return LongWritable
   */
  public LongWritable evaluate(ShortWritable i) {
    if (i == null) {
      return null;
    } else {
      longWritable.set(i.get());
      return longWritable;
    }
  }

  /**
   * Convert from integer to a long. This is called for CAST(... AS BIGINT)
   *
   * @param i
   *          The integer value to convert
   * @return LongWritable
   */
  public LongWritable evaluate(IntWritable i) {
    if (i == null) {
      return null;
    } else {
      longWritable.set(i.get());
      return longWritable;
    }
  }

  /**
   * Convert from long to a long. This is called for CAST(... AS BIGINT)
   *
   * @param i
   *          The long value to convert
   * @return LongWritable
   */
  public LongWritable evaluate(LongWritable i) {
    return i;
  }

  /**
   * Convert from float to a long. This is called for CAST(... AS BIGINT)
   *
   * @param i
   *          The float value to convert
   * @return LongWritable
   */
  public LongWritable evaluate(FloatWritable i) {
    if (i == null) {
      return null;
    } else {
      longWritable.set((long) i.get());
      return longWritable;
    }
  }

  /**
   * Convert from double to a long. This is called for CAST(... AS BIGINT)
   *
   * @param i
   *          The double value to convert
   * @return LongWritable
   */
  public LongWritable evaluate(DoubleWritable i) {
    if (i == null) {
      return null;
    } else {
      longWritable.set((long) i.get());
      return longWritable;
    }
  }

  /**
   * Convert from string to a long. This is called for CAST(... AS BIGINT)
   *
   * @param i
   *          The string value to convert
   * @return LongWritable
   */
  public LongWritable evaluate(Text i) {
    if (i == null) {
      return null;
    } else {
      if (!LazyUtils.isNumberMaybe(i.getBytes(), 0, i.getLength())) {
        return null;
      }
      try {
        longWritable
            .set(LazyLong.parseLong(i.getBytes(), 0, i.getLength(), 10, true));
        return longWritable;
      } catch (NumberFormatException e) {
        // MySQL returns 0 if the string is not a well-formed numeric value.
        // return LongWritable.valueOf(0);
        // But we decided to return NULL instead, which is more conservative.
        return null;
      }
    }
  }

  public LongWritable evaluate(TimestampWritable i) {
    if (i == null) {
      return null;
    } else {
      longWritable.set(i.getSeconds());
      return longWritable;
    }
  }

  public LongWritable evaluate(HiveDecimalWritable i) {
    if (i == null || !i.isSet() || !i.isLong()) {
      return null;
    } else {
      longWritable.set(i.longValue());
      return longWritable;
    }
  }

}
