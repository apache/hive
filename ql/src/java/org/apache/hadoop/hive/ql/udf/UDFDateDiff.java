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
import java.util.TimeZone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

@description(name = "datediff", value = "_FUNC_(date1, date2) - Returns the number of days between date1 "
    + "and date2", extended = "date1 and date2 are strings in the format "
    + "'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd'. The time parts are ignored."
    + "If date1 is earlier than date2, the result is negative.\n"
    + "Example:\n "
    + "  > SELECT _FUNC_('2009-30-07', '2009-31-07') FROM src LIMIT 1;\n"
    + "  1")
public class UDFDateDiff extends UDF {

  private static Log LOG = LogFactory.getLog(UDFDateDiff.class.getName());

  private final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

  IntWritable result = new IntWritable();

  public UDFDateDiff() {
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  /**
   * Calculate the difference in the number of days. The time part of the string
   * will be ignored. If dateString1 is earlier than dateString2, then the
   * result can be negative.
   * 
   * @param dateString1
   *          the date string in the format of "yyyy-MM-dd HH:mm:ss" or
   *          "yyyy-MM-dd".
   * @param dateString2
   *          the date string in the format of "yyyy-MM-dd HH:mm:ss" or
   *          "yyyy-MM-dd".
   * @return the difference in days.
   */
  public IntWritable evaluate(Text dateString1, Text dateString2) {

    if (dateString1 == null || dateString2 == null) {
      return null;
    }

    try {
      // NOTE: This implementation avoids the extra-second problem
      // by comparing with UTC epoch and integer division.
      long diffInMilliSeconds = (formatter.parse(dateString1.toString())
          .getTime() - formatter.parse(dateString2.toString()).getTime());
      // 86400 is the number of seconds in a day
      result.set((int) (diffInMilliSeconds / (86400 * 1000)));
      return result;
    } catch (ParseException e) {
      return null;
    }
  }

}
