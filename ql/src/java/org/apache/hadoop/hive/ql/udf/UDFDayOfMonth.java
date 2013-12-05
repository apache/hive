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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFDayOfMonthLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFDayOfMonthString;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFDayOfMonth.
 *
 */
@Description(name = "day,dayofmonth",
    value = "_FUNC_(date) - Returns the date of the month of date",
    extended = "date is a string in the format of 'yyyy-MM-dd HH:mm:ss' or "
    + "'yyyy-MM-dd'.\n"
    + "Example:\n "
    + "  > SELECT _FUNC_('2009-30-07', 1) FROM src LIMIT 1;\n" + "  30")
@VectorizedExpressions({VectorUDFDayOfMonthLong.class, VectorUDFDayOfMonthString.class})
public class UDFDayOfMonth extends UDF {
  private final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
  private final Calendar calendar = Calendar.getInstance();

  private final IntWritable result = new IntWritable();

  public UDFDayOfMonth() {
  }

  /**
   * Get the day of month from a date string.
   *
   * @param dateString
   *          the dateString in the format of "yyyy-MM-dd HH:mm:ss" or
   *          "yyyy-MM-dd".
   * @return an int from 1 to 31. null if the dateString is not a valid date
   *         string.
   */
  public IntWritable evaluate(Text dateString) {

    if (dateString == null) {
      return null;
    }

    try {
      Date date = formatter.parse(dateString.toString());
      calendar.setTime(date);
      result.set(calendar.get(Calendar.DAY_OF_MONTH));
      return result;
    } catch (ParseException e) {
      return null;
    }
  }

  public IntWritable evaluate(DateWritable d) {
    if (d == null) {
      return null;
    }

    calendar.setTime(d.get());
    result.set(calendar.get(Calendar.DAY_OF_MONTH));
    return result;
  }

  public IntWritable evaluate(TimestampWritable t) {
    if (t == null) {
      return null;
    }

    calendar.setTime(t.getTimestamp());
    result.set(calendar.get(Calendar.DAY_OF_MONTH));
    return result;
  }

}
