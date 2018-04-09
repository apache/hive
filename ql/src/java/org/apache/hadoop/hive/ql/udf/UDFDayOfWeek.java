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

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFDayOfWeekDate;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFDayOfWeekString;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFDayOfWeekTimestamp;
import org.apache.hadoop.hive.ql.udf.generic.NDV;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFDayOfWeek.
 *
 */
@Description(name = "dayofweek",
    value = "_FUNC_(param) - Returns the day of the week of date/timestamp "
    + "(1 = Sunday, 2 = Monday, ..., 7 = Saturday)",
    extended = "param can be one of:\n"
    + "1. A string in the format of 'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd'.\n"
    + "2. A date value\n"
    + "3. A timestamp value"
    + "Example:\n "
    + "  > SELECT _FUNC_('2009-07-30') FROM src LIMIT 1;\n" + "  5")
@VectorizedExpressions({VectorUDFDayOfWeekDate.class, VectorUDFDayOfWeekString.class, VectorUDFDayOfWeekTimestamp.class})
@NDV(maxNdv = 7)
public class UDFDayOfWeek extends UDF {

  private final IntWritable result = new IntWritable();

  public UDFDayOfWeek() {
  }

  /**
   * Get the day of week from a date string.
   *
   * @param dateString
   *          the dateString in the format of "yyyy-MM-dd HH:mm:ss" or
   *          "yyyy-MM-dd".
   * @return an int from 1 to 7. null if the dateString is not a valid date
   *         string.
   */
  public IntWritable evaluate(Text dateString) {
    if (dateString == null) {
      return null;
    }
    try {
      Date date = Date.valueOf(dateString.toString());
      result.set(date.getDayOfWeek());
      return result;
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  public IntWritable evaluate(DateWritableV2 d) {
    if (d == null) {
      return null;
    }

    result.set(d.get().getDayOfWeek());
    return result;
  }

  public IntWritable evaluate(TimestampWritableV2 t) {
    if (t == null) {
      return null;
    }

    result.set(t.getTimestamp().getDayOfWeek());
    return result;
  }

}
