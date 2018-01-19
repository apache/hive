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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFMinuteDate;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFMinuteString;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFMinuteTimestamp;
import org.apache.hadoop.hive.ql.udf.generic.NDV;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFMinute.
 *
 */
@Description(name = "minute",
    value = "_FUNC_(param) - Returns the minute component of the string/timestamp/interval",
    extended = "param can be one of:\n"
    + "1. A string in the format of 'yyyy-MM-dd HH:mm:ss' or 'HH:mm:ss'.\n"
    + "2. A timestamp value\n"
    + "3. A day-time interval value"
    + "Example:\n "
    + "  > SELECT _FUNC_('2009-07-30 12:58:59') FROM src LIMIT 1;\n"
    + "  58\n"
    + "  > SELECT _FUNC_('12:58:59') FROM src LIMIT 1;\n" + "  58")
@VectorizedExpressions({VectorUDFMinuteDate.class, VectorUDFMinuteString.class, VectorUDFMinuteTimestamp.class})
@NDV(maxNdv = 60)
public class UDFMinute extends UDF {
  private final SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private final SimpleDateFormat formatter2 = new SimpleDateFormat("HH:mm:ss");
  private final Calendar calendar = Calendar.getInstance();

  private final IntWritable result = new IntWritable();

  public UDFMinute() {
  }

  /**
   * Get the minute from a date string.
   *
   * @param dateString
   *          the dateString in the format of "yyyy-MM-dd HH:mm:ss" or
   *          "yyyy-MM-dd".
   * @return an int from 0 to 59. null if the dateString is not a valid date
   *         string.
   */
  public IntWritable evaluate(Text dateString) {

    if (dateString == null) {
      return null;
    }

    try {
      Date date = null;
      try {
        date = formatter1.parse(dateString.toString());
      } catch (ParseException e) {
        date = formatter2.parse(dateString.toString());
      }
      calendar.setTime(date);
      result.set(calendar.get(Calendar.MINUTE));
      return result;
    } catch (ParseException e) {
      return null;
    }
  }

  public IntWritable evaluate(TimestampWritable t) {
    if (t == null) {
      return null;
    }

    calendar.setTime(t.getTimestamp());
    result.set(calendar.get(Calendar.MINUTE));
    return result;
  }

  public IntWritable evaluate(HiveIntervalDayTimeWritable i) {
    if (i == null) {
      return null;
    }

    result.set(i.getHiveIntervalDayTime().getMinutes());
    return result;
  }
}
