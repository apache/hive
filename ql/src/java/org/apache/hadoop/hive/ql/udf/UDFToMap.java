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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFMethodResolver;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * UDFToInteger.
 *
 */
//@VectorizedExpressions({CastTimestampToLong.class, CastDoubleToLong.class,
//    CastDecimalToLong.class, CastStringToLong.class})
@Description(
        name = "int",
        value = "_FUNC_(x) - converts it's parameter to _FUNC_",
        extended =
                "- x is NULL -> NULL\n" +
                "- byte, short, integer, long, timestamp:\n" +
                "  x fits into the type _FUNC_ -> integer part of x\n" +
                "  undefined otherwise\n" +
                "- boolean:\n" +
                "  true  -> 1\n" +
                "  false -> 0\n" +
                "- string:\n" +
                "  x is a valid integer -> x\n" +
                "  NULL otherwise\n" +
                "Example:\n "
                + "  > SELECT _FUNC_(true);\n"
                + "  1")
public class UDFToMap extends UDF {
  private final List<IntWritable> intWritable = new ArrayList<>();

  public UDFToMap() {
  }

  @Override
  public UDFMethodResolver getResolver() {
    return new TimestampCastRestrictorResolver(super.getResolver());
  }

  /**
   * Convert from void to an integer. This is called for CAST(... AS INT)
   *
   * @param i
   *          The void value to convert
   * @return Integer
   */
  public Map<Object, Object> evaluate(NullWritable i) {
    return null;
  }

  /**
   * Convert from boolean to an integer. This is called for CAST(... AS INT)
   *
   * @param i
   *          The boolean value to convert
   * @return IntWritable
   */
  public Map<Object, Object> evaluate(BooleanWritable i) {
    if (i == null) {
      return null;
    } else {
//      intWritable.set(i.get() ? 1 : 0);
//      return intWritable;
      return null;
    }
  }

  /**
   * Convert from byte to an integer. This is called for CAST(... AS INT)
   *
   * @param i
   *          The byte value to convert
   * @return IntWritable
   */
  public Map<Object, Object> evaluate(ByteWritable i) {
    if (i == null) {
      return null;
    } else {
//      intWritable.set(i.get());
//      return intWritable;
      return null;
    }
  }

  /**
   * Convert from short to an integer. This is called for CAST(... AS INT)
   *
   * @param i
   *          The short value to convert
   * @return IntWritable
   */
  public Map<Object, Object> evaluate(ShortWritable i) {
    if (i == null) {
      return null;
    } else {
//      intWritable.set(i.get());
//      return intWritable;
      return null;
    }
  }

  /**
   * Convert from long to an integer. This is called for CAST(... AS INT)
   *
   * @param i
   *          The long value to convert
   * @return IntWritable
   */
  public Map<Object, Object> evaluate(LongWritable i) {
    if (i == null) {
      return null;
    } else {
//      intWritable.set((int) i.get());
//      return intWritable;
      return null;
    }
  }

  /**
   * Convert from float to an integer. This is called for CAST(... AS INT)
   *
   * @param i
   *          The float value to convert
   * @return IntWritable
   */
  public Map<Object, Object> evaluate(FloatWritable i) {
    if (i == null) {
      return null;
    } else {
//      intWritable.set((int) i.get());
//      return intWritable;
      return null;
    }
  }

  /**
   * Convert from double to an integer. This is called for CAST(... AS INT)
   *
   * @param i
   *          The double value to convert
   * @return IntWritable
   */
  public Map<Object, Object> evaluate(DoubleWritable i) {
    if (i == null) {
      return null;
    } else {
//      intWritable.set((int) i.get());
//      return intWritable;
      return null;
    }
  }

  /**
   * Convert from string to an integer. This is called for CAST(... AS INT)
   *
   * @param i
   *          The string value to convert
   * @return IntWritable
   */
  public Map<Object, Object> evaluate(Text i) {
    if (i == null) {
      return null;
    } else {
      return null;
//      if (!LazyUtils.isNumberMaybe(i.getBytes(), 0, i.getLength())) {
//        return null;
//      }
//      try {
//        intWritable.set(LazyInteger
//            .parseInt(i.getBytes(), 0, i.getLength(), 10, true));
//        return intWritable;
//      } catch (NumberFormatException e) {
//        // MySQL returns 0 if the string is not a well-formed numeric value.
//        // return IntWritable.valueOf(0);
//        // But we decided to return NULL instead, which is more conservative.
//        return null;
//      }
    }
  }

  /**
   * Convert from Timestamp to an integer. This is called for CAST(... AS INT)
   *
   * @param i
   *          The Timestamp value to convert
   * @return IntWritable
   */
  public Map<Object, Object> evaluate(TimestampWritableV2 i) {
    if (i == null) {
      return null;
    } else {
      return null;
//      final long longValue = UDFUtils.getTimestampTZFromTimestamp(i.getTimestamp()).getEpochSecond();
//      final int intValue = (int) longValue;
//      if (intValue != longValue) {
//        return null;
//      }
//      intWritable.set(intValue);
//      return intWritable;
    }
  }

  public Map<Object, Object> evaluate(HiveDecimalWritable i) {
    if (i == null || !i.isSet() || !i.isInt()) {
      return null;
    } else {
      return null;
//      intWritable.set(i.intValue());
//      return intWritable;
    }
  }

  /**
   * Convert a RecordIdentifier.  This is done so that we can use the RecordIdentifier in place
   * of the bucketing column.
   * @param i RecordIdentifier to convert
   * @return value of the bucket identifier
   */
  public Map<Object, Object> evaluate(RecordIdentifier i) {
    return null;
//    if (i == null) {
//      return null;
//    } else {
//      BucketCodec decoder =
//        BucketCodec.determineVersion(i.getBucketProperty());
//      intWritable.set(decoder.decodeWriterId(i.getBucketProperty()));
//      return intWritable;
//    }
  }

}
