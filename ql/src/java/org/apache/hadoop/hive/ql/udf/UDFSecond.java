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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

@Description(name = "second", value = "_FUNC_(date) - Returns the second of date", extended = "date is a string in the format of 'yyyy-MM-dd HH:mm:ss' or "
    + "'HH:mm:ss'.\n"
    + "Example:\n "
    + "  > SELECT _FUNC_('2009-07-30 12:58:59') FROM src LIMIT 1;\n"
    + "  59\n"
    + "  > SELECT _FUNC_('12:58:59') FROM src LIMIT 1;\n" + "  59")
public class UDFSecond extends UDF {

  private static Log LOG = LogFactory.getLog(UDFSecond.class.getName());

  private final SimpleDateFormat formatter1 = new SimpleDateFormat(
      "yyyy-MM-dd HH:mm:ss");
  private final SimpleDateFormat formatter2 = new SimpleDateFormat("HH:mm:ss");
  private final Calendar calendar = Calendar.getInstance();

  IntWritable result = new IntWritable();

  public UDFSecond() {
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
      result.set(calendar.get(Calendar.SECOND));
      return result;
    } catch (ParseException e) {
      return null;
    }
  }

}
