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
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDecimalToFloat;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastStringToFloat;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.CastLongToFloatViaLongToDouble;
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
 * UDFToFloat.
 *
 */
@VectorizedExpressions({CastTimestampToDouble.class, CastLongToFloatViaLongToDouble.class,
    CastDecimalToFloat.class, CastStringToFloat.class})
@Description(
        name = "float",
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
                + "  1.0")
public class UDFToFloat extends UDF {
  private final FloatWritable floatWritable = new FloatWritable();

  public UDFToFloat() {
  }

  @Override
  public UDFMethodResolver getResolver() {
    return new TimestampCastRestrictorResolver(super.getResolver());
  }
  /**
   * Convert from void to a float. This is called for CAST(... AS FLOAT)
   *
   * @param i
   *          The void value to convert
   * @return FloatWritable
   */
  public FloatWritable evaluate(NullWritable i) {
    return null;
  }

  /**
   * Convert from boolean to a float. This is called for CAST(... AS FLOAT)
   *
   * @param i
   *          The boolean value to convert
   * @return FloatWritable
   */
  public FloatWritable evaluate(BooleanWritable i) {
    if (i == null) {
      return null;
    } else {
      floatWritable.set(i.get() ? (float) 1.0 : (float) 0.0);
      return floatWritable;
    }
  }

  /**
   * Convert from byte to a float. This is called for CAST(... AS FLOAT)
   *
   * @param i
   *          The byte value to convert
   * @return FloatWritable
   */
  public FloatWritable evaluate(ByteWritable i) {
    if (i == null) {
      return null;
    } else {
      floatWritable.set(i.get());
      return floatWritable;
    }
  }

  /**
   * Convert from short to a float. This is called for CAST(... AS FLOAT)
   *
   * @param i
   *          The short value to convert
   * @return FloatWritable
   */
  public FloatWritable evaluate(ShortWritable i) {
    if (i == null) {
      return null;
    } else {
      floatWritable.set(i.get());
      return floatWritable;
    }
  }

  /**
   * Convert from integer to a float. This is called for CAST(... AS FLOAT)
   *
   * @param i
   *          The integer value to convert
   * @return FloatWritable
   */
  public FloatWritable evaluate(IntWritable i) {
    if (i == null) {
      return null;
    } else {
      floatWritable.set(i.get());
      return floatWritable;
    }
  }

  /**
   * Convert from long to a float. This is called for CAST(... AS FLOAT)
   *
   * @param i
   *          The long value to convert
   * @return FloatWritable
   */
  public FloatWritable evaluate(LongWritable i) {
    if (i == null) {
      return null;
    } else {
      floatWritable.set(i.get());
      return floatWritable;
    }
  }

  /**
   * Convert from double to a float. This is called for CAST(... AS FLOAT)
   *
   * @param i
   *          The double value to convert
   * @return FloatWritable
   */
  public FloatWritable evaluate(DoubleWritable i) {
    if (i == null) {
      return null;
    } else {
      floatWritable.set((float) i.get());
      return floatWritable;
    }
  }

  /**
   * Convert from string to a float. This is called for CAST(... AS FLOAT)
   *
   * @param i
   *          The string value to convert
   * @return FloatWritable
   */
  public FloatWritable evaluate(Text i) {
    if (i == null) {
      return null;
    } else {
      if (!LazyUtils.isNumberMaybe(i.getBytes(), 0, i.getLength())) {
        return null;
      }
      try {
        floatWritable.set(Float.parseFloat(i.toString()));
        return floatWritable;
      } catch (NumberFormatException e) {
        // MySQL returns 0 if the string is not a well-formed numeric value.
        // But we decided to return NULL instead, which is more conservative.
        return null;
      }
    }
  }

  public FloatWritable evaluate(TimestampWritableV2 i) {
    if (i == null) {
      return null;
    } else {
      try {
        floatWritable.set((float) TimestampTZUtil.convertTimestampTZToDouble(
          UDFUtils.getTimestampTZFromTimestamp(i.getTimestamp())
        ));
        return floatWritable;
      } catch (NumberFormatException e) {
        // MySQL returns 0 if the string is not a well-formed numeric value.
        // But we decided to return NULL instead, which is more conservative.
        return null;
      }
    }
  }

  public FloatWritable evaluate(HiveDecimalWritable i) {
    if (i == null || !i.isSet()) {
      return null;
    } else {
      floatWritable.set(i.floatValue());
      return floatWritable;
    }
  }

}
