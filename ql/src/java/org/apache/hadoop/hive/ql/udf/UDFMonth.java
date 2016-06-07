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
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFMonthLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFMonthString;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFMonth.
 *
 */
@Description(name = "month",
    value = "_FUNC_(param) - Returns the month component of the date/timestamp/interval",
    extended = "param can be one of:\n"
    + "1. A string in the format of 'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd'.\n"
    + "2. A date value\n"
    + "3. A timestamp value\n"
    + "4. A year-month interval value"
    + "Example:\n"
    + "  > SELECT _FUNC_('2009-07-30') FROM src LIMIT 1;\n" + "  7")
@VectorizedExpressions({VectorUDFMonthLong.class, VectorUDFMonthString.class})
public class UDFMonth extends UDF {
  private final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
  private final Calendar calendar = Calendar.getInstance();

  private IntWritable result = new IntWritable();

  public UDFMonth() {
  }

  /**
   * Get the month from a date string.
   *
   * @param dateString
   *          the dateString in the format of "yyyy-MM-dd HH:mm:ss" or
   *          "yyyy-MM-dd".
   * @return an int from 1 to 12. null if the dateString is not a valid date
   *         string.
   */
  public IntWritable evaluate(Text dateString) {
    if (dateString == null) {
      return null;
    }
    try {
      Date date = formatter.parse(dateString.toString());
      calendar.setTime(date);
      result.set(1 + calendar.get(Calendar.MONTH));
      return result;
    } catch (ParseException e) {
      return null;
    }
  }

  public IntWritable evaluate(DateWritable d) {
    if (d == null) {
      return null;
    }

    calendar.setTime(d.get(false));  // Time doesn't matter.
    result.set(1 + calendar.get(Calendar.MONTH));
    return result;
  }

  public IntWritable evaluate(TimestampWritable t) {
    if (t == null) {
      return null;
    }

    calendar.setTime(t.getTimestamp());
    result.set(1 + calendar.get(Calendar.MONTH));
    return result;
  }

  public IntWritable evaluate(HiveIntervalYearMonthWritable i) {
    if (i == null) {
      return null;
    }

    result.set(i.getHiveIntervalYearMonth().getMonths());
    return result;
  }
}
