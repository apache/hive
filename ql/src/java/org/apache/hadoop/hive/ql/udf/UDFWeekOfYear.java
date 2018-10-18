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

import java.util.Calendar;
import java.util.TimeZone;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFWeekOfYearDate;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFWeekOfYearString;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFWeekOfYearTimestamp;
import org.apache.hadoop.hive.ql.udf.generic.NDV;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFWeekOfYear.
 *
 */
@Description(name = "yearweek",
    value = "_FUNC_(date) - Returns the week of the year of the given date. A week "
    + "is considered to start on a Monday and week 1 is the first week with >3 days.",
    extended = "Examples:\n"
    + "  > SELECT _FUNC_('2008-02-20') FROM src LIMIT 1;\n"
    + "  8\n"
    + "  > SELECT _FUNC_('1980-12-31 12:59:59') FROM src LIMIT 1;\n" + "  1")
@VectorizedExpressions({VectorUDFWeekOfYearDate.class, VectorUDFWeekOfYearString.class, VectorUDFWeekOfYearTimestamp.class})
@NDV(maxNdv = 52)
public class UDFWeekOfYear extends UDF {

  private final IntWritable result = new IntWritable();

  private final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));


  public UDFWeekOfYear() {
    calendar.setFirstDayOfWeek(Calendar.MONDAY);
    calendar.setMinimalDaysInFirstWeek(4);
  }

  /**
   * Get the week of the year from a date string.
   *
   * @param dateString
   *          the dateString in the format of "yyyy-MM-dd HH:mm:ss" or
   *          "yyyy-MM-dd".
   * @return an int from 1 to 53. null if the dateString is not a valid date
   *         string.
   */
  public IntWritable evaluate(Text dateString) {
    if (dateString == null) {
      return null;
    }
    try {
      Date date = Date.valueOf(dateString.toString());
      calendar.setTimeInMillis(date.toEpochMilli());
      result.set(calendar.get(Calendar.WEEK_OF_YEAR));
      return result;
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  public IntWritable evaluate(DateWritableV2 d) {
    if (d == null) {
      return null;
    }
    Date date = d.get();
    calendar.setTimeInMillis(date.toEpochMilli());
    result.set(calendar.get(Calendar.WEEK_OF_YEAR));
    return result;
  }

  public IntWritable evaluate(TimestampWritableV2 t) {
    if (t == null) {
      return null;
    }

    Timestamp ts = t.getTimestamp();
    calendar.setTimeInMillis(ts.toEpochMilli());
    result.set(calendar.get(Calendar.WEEK_OF_YEAR));
    return result;
  }

}
