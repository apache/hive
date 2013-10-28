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

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.CastLongToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.CastTimestampToDoubleViaLongToDouble;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
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
@VectorizedExpressions({CastTimestampToDoubleViaLongToDouble.class, CastLongToDouble.class})
public class UDFToDouble extends UDF {
  private final DoubleWritable doubleWritable = new DoubleWritable();

  public UDFToDouble() {
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
      try {
        doubleWritable.set(Double.valueOf(i.toString()));
        return doubleWritable;
      } catch (NumberFormatException e) {
        // MySQL returns 0 if the string is not a well-formed double value.
        // But we decided to return NULL instead, which is more conservative.
        return null;
      }
    }
  }

  public DoubleWritable evaluate(TimestampWritable i) {
    if (i == null) {
      return null;
    } else {
      try {
        doubleWritable.set(i.getDouble());
        return doubleWritable;
      } catch (NumberFormatException e) {
        // MySQL returns 0 if the string is not a well-formed numeric value.
        // But we decided to return NULL instead, which is more conservative.
        return null;
      }
    }
  }

  public DoubleWritable evaluate(HiveDecimalWritable i) {
    if (i == null) {
      return null;
    } else {
      doubleWritable.set(i.getHiveDecimal().doubleValue());
      return doubleWritable;
    }
  }
}
