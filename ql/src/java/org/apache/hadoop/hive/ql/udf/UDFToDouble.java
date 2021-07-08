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

import org.apache.hadoop.hive.common.type.TimestampTZUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFMethodResolver;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDecimalToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastStringToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.CastLongToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastTimestampToDouble;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
/**
 * UDFToDouble.
 *
 */
@VectorizedExpressions({CastTimestampToDouble.class, CastLongToDouble.class,
    CastDecimalToDouble.class, CastStringToDouble.class})
@Description(
        name = "double",
        value = "_FUNC_(x) - converts it's parameter to _FUNC_",
        extended =
                "- x is NULL -> NULL\n" +
                "- byte, short, integer, long, float, double, decimal, timestamp:\n" +
                "  x fits into the type _FUNC_ -> x\n" +
                "  undefined otherwise\n" +
                "- boolean:\n" +
                "  true  -> 1.0\n" +
                "  false -> 0.0\n" +
                "- string:\n" +
                "  x is a valid _FUNC_ -> x\n" +
                "  NULL otherwise\n" +
                "Example:\n "
                + "  > SELECT _FUNC_(true);\n"
                + "  1")
public class UDFToDouble extends UDF {
  private final DoubleWritable doubleWritable = new DoubleWritable();

  public UDFToDouble() {
  }

  @Override
  public UDFMethodResolver getResolver() {
    return new TimestampCastRestrictorResolver(super.getResolver());
  }

  /**
   * Convert from void to a double. This is called for CAST(... AS DOUBLE)
   *
   * @param i
   *          The void value to convert
   * @return DoubleWritable
   */
  public DoubleWritable evaluate(NullWritable i) {
    return null;
  }

  /**
   * Convert from boolean to a double. This is called for CAST(... AS DOUBLE)
   *
   * @param i
   *          The boolean value to convert
   * @return DoubleWritable
   */
  public DoubleWritable evaluate(BooleanWritable i) {
    if (i == null) {
      return null;
    } else {
      doubleWritable.set(i.get() ? 1.0 : 0.0);
      return doubleWritable;
    }
  }

  /**
   * Convert from boolean to a double. This is called for CAST(... AS DOUBLE)
   *
   * @param i
   *          The byte value to convert
   * @return DoubleWritable
   */
  public DoubleWritable evaluate(ByteWritable i) {
    if (i == null) {
      return null;
    } else {
      doubleWritable.set(i.get());
      return doubleWritable;
    }
  }

  /**
   * Convert from short to a double. This is called for CAST(... AS DOUBLE)
   *
   * @param i
   *          The short value to convert
   * @return DoubleWritable
   */
  public DoubleWritable evaluate(ShortWritable i) {
    if (i == null) {
      return null;
    } else {
      doubleWritable.set(i.get());
      return doubleWritable;
    }
  }

  /**
   * Convert from integer to a double. This is called for CAST(... AS DOUBLE)
   *
   * @param i
   *          The integer value to convert
   * @return DoubleWritable
   */
  public DoubleWritable evaluate(IntWritable i) {
    if (i == null) {
      return null;
    } else {
      doubleWritable.set(i.get());
      return doubleWritable;
    }
  }

  /**
   * Convert from long to a double. This is called for CAST(... AS DOUBLE)
   *
   * @param i
   *          The long value to convert
   * @return DoubleWritable
   */
  public DoubleWritable evaluate(LongWritable i) {
    if (i == null) {
      return null;
    } else {
      doubleWritable.set(i.get());
      return doubleWritable;
    }
  }

  /**
   * Convert from float to a double. This is called for CAST(... AS DOUBLE)
   *
   * @param i
   *          The float value to convert
   * @return DoubleWritable
   */
  public DoubleWritable evaluate(FloatWritable i) {
    if (i == null) {
      return null;
    } else {
      doubleWritable.set(i.get());
      return doubleWritable;
    }
  }

  /**
   * Convert from string to a double. This is called for CAST(... AS DOUBLE)
   *
   * @param i
   *          The string value to convert
   * @return DoubleWritable
   */
  public DoubleWritable evaluate(Text i) {
    if (i == null) {
      return null;
    } else {
      if (!LazyUtils.isNumberMaybe(i.getBytes(), 0, i.getLength())) {
        return null;
      }
      try {
        doubleWritable.set(Double.parseDouble(i.toString()));
        return doubleWritable;
      } catch (NumberFormatException e) {
        // MySQL returns 0 if the string is not a well-formed double value.
        // But we decided to return NULL instead, which is more conservative.
        return null;
      }
    }
  }

  public DoubleWritable evaluate(TimestampWritableV2 i) {
    if (i == null) {
      return null;
    } else {
      try {
        doubleWritable.set(TimestampTZUtil.convertTimestampTZToDouble(
          UDFUtils.getTimestampTZFromTimestamp(i.getTimestamp())
        ));
        return doubleWritable;
      } catch (NumberFormatException e) {
        // MySQL returns 0 if the string is not a well-formed numeric value.
        // But we decided to return NULL instead, which is more conservative.
        return null;
      }
    }
  }

  public DoubleWritable evaluate(HiveDecimalWritable i) {
    if (i == null || !i.isSet()) {
      return null;
    } else {
      doubleWritable.set(i.doubleValue());
      return doubleWritable;
    }
  }
}
