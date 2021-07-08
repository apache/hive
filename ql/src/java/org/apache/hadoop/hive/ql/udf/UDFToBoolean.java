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


import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFMethodResolver;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDecimalToBoolean;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastStringToBoolean;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastStringToLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.CastDoubleToBooleanViaDoubleToLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.CastLongToBooleanViaLongToLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDateToBoolean;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastTimestampToBoolean;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFToBoolean.
 *
 */
@VectorizedExpressions({CastLongToBooleanViaLongToLong.class,
    CastDateToBoolean.class, CastTimestampToBoolean.class, CastStringToBoolean.class,
  CastDoubleToBooleanViaDoubleToLong.class, CastDecimalToBoolean.class, CastStringToLong.class})
@Description(
        name = "boolean",
        value = "_FUNC_(x) - converts it's parameter to _FUNC_",
        extended =
                "- x is NULL -> NULL\n" +
                "- byte, short, integer, long, float, double, decimal:\n" +
                "  x == 0 -> false\n" +
                "  x != 0 -> true\n" +
                "- string:\n" +
                "  x is '', 'false', 'no', 'zero', 'off' -> false\n" +
                "  true otherwise\n" +
                "- date: always NULL\n" +
                "- timestamp\n" +
                "  seconds or nanos are 0 -> false\n" +
                "  true otherwise\n" +
                "Example:\n "
                + "  > SELECT _FUNC_(0);\n"
                + "  false")
public class UDFToBoolean extends UDF {
  private final BooleanWritable booleanWritable = new BooleanWritable();

  public UDFToBoolean() {
  }

  @Override
  public UDFMethodResolver getResolver() {
    return new TimestampCastRestrictorResolver(super.getResolver());
  }

  /**
   * Convert a void to boolean. This is called for CAST(... AS BOOLEAN)
   *
   * @param i
   *          The value of a void type
   * @return BooleanWritable
   */
  public BooleanWritable evaluate(NullWritable i) {
    return null;
  }

  /**
   * Convert from a byte to boolean. This is called for CAST(... AS BOOLEAN)
   *
   * @param i
   *          The byte value to convert
   * @return BooleanWritable
   */
  public BooleanWritable evaluate(ByteWritable i) {
    if (i == null) {
      return null;
    } else {
      booleanWritable.set(i.get() != 0);
      return booleanWritable;
    }
  }

  /**
   * Convert from a short to boolean. This is called for CAST(... AS BOOLEAN)
   *
   * @param i
   *          The short value to convert
   * @return BooleanWritable
   */
  public BooleanWritable evaluate(ShortWritable i) {
    if (i == null) {
      return null;
    } else {
      booleanWritable.set(i.get() != 0);
      return booleanWritable;
    }
  }

  /**
   * Convert from a integer to boolean. This is called for CAST(... AS BOOLEAN)
   *
   * @param i
   *          The integer value to convert
   * @return BooleanWritable
   */
  public BooleanWritable evaluate(IntWritable i) {
    if (i == null) {
      return null;
    } else {
      booleanWritable.set(i.get() != 0);
      return booleanWritable;
    }
  }

  /**
   * Convert from a long to boolean. This is called for CAST(... AS BOOLEAN)
   *
   * @param i
   *          The long value to convert
   * @return BooleanWritable
   */
  public BooleanWritable evaluate(LongWritable i) {
    if (i == null) {
      return null;
    } else {
      booleanWritable.set(i.get() != 0);
      return booleanWritable;
    }
  }

  /**
   * Convert from a float to boolean. This is called for CAST(... AS BOOLEAN)
   *
   * @param i
   *          The float value to convert
   * @return BooleanWritable
   */
  public BooleanWritable evaluate(FloatWritable i) {
    if (i == null) {
      return null;
    } else {
      booleanWritable.set(i.get() != 0);
      return booleanWritable;
    }
  }

  /**
   * Convert from a double to boolean. This is called for CAST(... AS BOOLEAN)
   *
   * @param i
   *          The double value to convert
   * @return BooleanWritable
   */
  public BooleanWritable evaluate(DoubleWritable i) {
    if (i == null) {
      return null;
    } else {
      booleanWritable.set(i.get() != 0);
      return booleanWritable;
    }
  }

  /**
   * Convert from a string to boolean. This is called for CAST(... AS BOOLEAN)
   *
   * @param i
   *          The string value to convert
   * @return BooleanWritable
   */
  public BooleanWritable evaluate(Text i) {
    if (i == null) {
      return null;
    }
    boolean b = PrimitiveObjectInspectorUtils.parseBoolean(i.getBytes(), 0, i.getLength());
    booleanWritable.set(b);
    return booleanWritable;
  }

  public BooleanWritable evaluate(DateWritableV2 d) {
    // date value to boolean doesn't make any sense.
    return null;
  }

  public BooleanWritable evaluate(TimestampWritableV2 i) {
    if (i == null) {
      return null;
    } else {
      TimestampTZ timestamp = UDFUtils.getTimestampTZFromTimestamp(i.getTimestamp());
      booleanWritable.set((timestamp.getEpochSecond() != 0) || (timestamp.getNanos() != 0));
      return booleanWritable;
    }
  }

  public BooleanWritable evaluate(HiveDecimalWritable i) {
    if (i == null || !i.isSet()) {
      return null;
    } else {
      booleanWritable.set(i.compareTo(HiveDecimal.ZERO) != 0);
      return booleanWritable;
    }
  }

}
