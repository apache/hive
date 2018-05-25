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
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.CastDoubleToLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastStringToLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastTimestampToLong;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFToInteger.
 *
 */
@VectorizedExpressions({CastTimestampToLong.class, CastDoubleToLong.class,
    CastDecimalToLong.class, CastStringToLong.class})
public class UDFToInteger extends UDF {
  private final IntWritable intWritable = new IntWritable();

  public UDFToInteger() {
  }

  /**
   * Convert from void to an integer. This is called for CAST(... AS INT)
   *
   * @param i
   *          The void value to convert
   * @return Integer
   */
  public IntWritable evaluate(NullWritable i) {
    return null;
  }

  /**
   * Convert from boolean to an integer. This is called for CAST(... AS INT)
   *
   * @param i
   *          The boolean value to convert
   * @return IntWritable
   */
  public IntWritable evaluate(BooleanWritable i) {
    if (i == null) {
      return null;
    } else {
      intWritable.set(i.get() ? 1 : 0);
      return intWritable;
    }
  }

  /**
   * Convert from byte to an integer. This is called for CAST(... AS INT)
   *
   * @param i
   *          The byte value to convert
   * @return IntWritable
   */
  public IntWritable evaluate(ByteWritable i) {
    if (i == null) {
      return null;
    } else {
      intWritable.set(i.get());
      return intWritable;
    }
  }

  /**
   * Convert from short to an integer. This is called for CAST(... AS INT)
   *
   * @param i
   *          The short value to convert
   * @return IntWritable
   */
  public IntWritable evaluate(ShortWritable i) {
    if (i == null) {
      return null;
    } else {
      intWritable.set(i.get());
      return intWritable;
    }
  }

  /**
   * Convert from long to an integer. This is called for CAST(... AS INT)
   *
   * @param i
   *          The long value to convert
   * @return IntWritable
   */
  public IntWritable evaluate(LongWritable i) {
    if (i == null) {
      return null;
    } else {
      intWritable.set((int) i.get());
      return intWritable;
    }
  }

  /**
   * Convert from float to an integer. This is called for CAST(... AS INT)
   *
   * @param i
   *          The float value to convert
   * @return IntWritable
   */
  public IntWritable evaluate(FloatWritable i) {
    if (i == null) {
      return null;
    } else {
      intWritable.set((int) i.get());
      return intWritable;
    }
  }

  /**
   * Convert from double to an integer. This is called for CAST(... AS INT)
   *
   * @param i
   *          The double value to convert
   * @return IntWritable
   */
  public IntWritable evaluate(DoubleWritable i) {
    if (i == null) {
      return null;
    } else {
      intWritable.set((int) i.get());
      return intWritable;
    }
  }

  /**
   * Convert from string to an integer. This is called for CAST(... AS INT)
   *
   * @param i
   *          The string value to convert
   * @return IntWritable
   */
  public IntWritable evaluate(Text i) {
    if (i == null) {
      return null;
    } else {
      if (!LazyUtils.isNumberMaybe(i.getBytes(), 0, i.getLength())) {
        return null;
      }
      try {
        intWritable.set(LazyInteger
            .parseInt(i.getBytes(), 0, i.getLength(), 10, true));
        return intWritable;
      } catch (NumberFormatException e) {
        // MySQL returns 0 if the string is not a well-formed numeric value.
        // return IntWritable.valueOf(0);
        // But we decided to return NULL instead, which is more conservative.
        return null;
      }
    }
  }

  /**
   * Convert from Timestamp to an integer. This is called for CAST(... AS INT)
   *
   * @param i
   *          The Timestamp value to convert
   * @return IntWritable
   */
  public IntWritable evaluate(TimestampWritable i) {
    if (i == null) {
      return null;
    } else {
      intWritable.set((int) i.getSeconds());
      return intWritable;
    }
  }

  public IntWritable evaluate(HiveDecimalWritable i) {
    if (i == null || !i.isSet() || !i.isInt()) {
      return null;
    } else {
      intWritable.set(i.intValue());
      return intWritable;
    }
  }

  /**
   * Convert a RecordIdentifier.  This is done so that we can use the RecordIdentifier in place
   * of the bucketing column.
   * @param i RecordIdentifier to convert
   * @return value of the bucket identifier
   */
  public IntWritable evaluate(RecordIdentifier i) {
    if (i == null) {
      return null;
    } else {
      BucketCodec decoder =
        BucketCodec.determineVersion(i.getBucketProperty());
      intWritable.set(decoder.decodeWriterId(i.getBucketProperty()));
      return intWritable;
    }
  }

}
