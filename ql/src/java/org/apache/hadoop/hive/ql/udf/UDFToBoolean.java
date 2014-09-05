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
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDecimalToBoolean;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.CastDoubleToBooleanViaDoubleToLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.CastLongToBooleanViaLongToLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.CastDateToBooleanViaLongToLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.CastTimestampToBooleanViaLongToLong;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
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
 * UDFToBoolean.
 *
 */
@VectorizedExpressions({CastLongToBooleanViaLongToLong.class,
  CastDateToBooleanViaLongToLong.class, CastTimestampToBooleanViaLongToLong.class,
  CastDoubleToBooleanViaDoubleToLong.class, CastDecimalToBoolean.class})
public class UDFToBoolean extends UDF {
  private final BooleanWritable booleanWritable = new BooleanWritable();

  public UDFToBoolean() {
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
    } else {
      booleanWritable.set(i.getLength() != 0);
      return booleanWritable;
    }
  }

  public BooleanWritable evaluate(DateWritable d) {
    // date value to boolean doesn't make any sense.
    return null;
  }

  public BooleanWritable evaluate(TimestampWritable i) {
    if (i == null) {
      return null;
    } else {
      booleanWritable.set(i.getSeconds() != 0 || i.getNanos() != 0);
      return booleanWritable;
    }
  }

  public BooleanWritable evaluate(HiveDecimalWritable i) {
    if (i == null) {
      return null;
    } else {
      booleanWritable.set(HiveDecimal.ZERO.compareTo(i.getHiveDecimal()) != 0);
      return booleanWritable;
    }
  }

}
