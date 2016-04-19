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
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDecimalToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDecimalToLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.CastDoubleToLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastTimestampToLong;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyShort;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFToShort.
 *
 */
@VectorizedExpressions({CastTimestampToLong.class, CastDoubleToLong.class,
    CastDecimalToLong.class})
public class UDFToShort extends UDF {
  ShortWritable shortWritable = new ShortWritable();

  public UDFToShort() {
  }

  /**
   * Convert from void to a short. This is called for CAST(... AS SMALLINT)
   *
   * @param i
   *          The void value to convert
   * @return ShortWritable
   */
  public ShortWritable evaluate(NullWritable i) {
    return null;
  }

  /**
   * Convert from boolean to a short. This is called for CAST(... AS SMALLINT)
   *
   * @param i
   *          The boolean value to convert
   * @return ShortWritable
   */
  public ShortWritable evaluate(BooleanWritable i) {
    if (i == null) {
      return null;
    } else {
      shortWritable.set(i.get() ? (short) 1 : (short) 0);
      return shortWritable;
    }
  }

  /**
   * Convert from byte to a short. This is called for CAST(... AS SMALLINT)
   *
   * @param i
   *          The byte value to convert
   * @return ShortWritable
   */
  public ShortWritable evaluate(ByteWritable i) {
    if (i == null) {
      return null;
    } else {
      shortWritable.set(i.get());
      return shortWritable;
    }
  }

  /**
   * Convert from integer to a short. This is called for CAST(... AS SMALLINT)
   *
   * @param i
   *          The integer value to convert
   * @return ShortWritable
   */
  public ShortWritable evaluate(IntWritable i) {
    if (i == null) {
      return null;
    } else {
      shortWritable.set((short) i.get());
      return shortWritable;
    }
  }

  /**
   * Convert from long to a short. This is called for CAST(... AS SMALLINT)
   *
   * @param i
   *          The long value to convert
   * @return ShortWritable
   */
  public ShortWritable evaluate(LongWritable i) {
    if (i == null) {
      return null;
    } else {
      shortWritable.set((short) i.get());
      return shortWritable;
    }
  }

  /**
   * Convert from float to a short. This is called for CAST(... AS SMALLINT)
   *
   * @param i
   *          The float value to convert
   * @return ShortWritable
   */
  public ShortWritable evaluate(FloatWritable i) {
    if (i == null) {
      return null;
    } else {
      shortWritable.set((short) i.get());
      return shortWritable;
    }
  }

  /**
   * Convert from double to a short. This is called for CAST(... AS SMALLINT)
   *
   * @param i
   *          The double value to convert
   * @return ShortWritable
   */
  public ShortWritable evaluate(DoubleWritable i) {
    if (i == null) {
      return null;
    } else {
      shortWritable.set((short) i.get());
      return shortWritable;
    }
  }

  /**
   * Convert from string to a short. This is called for CAST(... AS SMALLINT)
   *
   * @param i
   *          The string value to convert
   * @return ShortWritable
   */
  public ShortWritable evaluate(Text i) {
    if (i == null) {
      return null;
    } else {
      try {
        shortWritable.set(LazyShort.parseShort(i.getBytes(), 0, i.getLength(),
            10));
        return shortWritable;
      } catch (NumberFormatException e) {
        // MySQL returns 0 if the string is not a well-formed numeric value.
        // return Byte.valueOf(0);
        // But we decided to return NULL instead, which is more conservative.
        return null;
      }
    }
  }

  public ShortWritable evaluate(TimestampWritable i) {
    if (i == null) {
      return null;
    } else {
      shortWritable.set((short) i.getSeconds());
      return shortWritable;
    }
  }

  public ShortWritable evaluate(HiveDecimalWritable i) {
    if (i == null) {
      return null;
    } else {
      shortWritable.set(i.getHiveDecimal().shortValue());
      return shortWritable;
    }
  }

}
