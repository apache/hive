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

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFFromUnixTime.
 *
 */
@Description(name = "from_unixtime",
    value = "_FUNC_(unix_time, format) - returns unix_time in the specified format",
    extended = "Example:\n"
    + "  > SELECT _FUNC_(0, 'yyyy-MM-dd HH:mm:ss') FROM src LIMIT 1;\n"
    + "  '1970-01-01 00:00:00'")
public class UDFFromUnixTime extends UDF {
  private SimpleDateFormat formatter;

  private Text result = new Text();
  private Text lastFormat = new Text();

  public UDFFromUnixTime() {
  }

  private Text defaultFormat = new Text("yyyy-MM-dd HH:mm:ss");

  public Text evaluate(IntWritable unixtime) {
    return evaluate(unixtime, defaultFormat);
  }

  /**
   * Convert UnixTime to a string format.
   * 
   * @param unixtime
   *          The number of seconds from 1970-01-01 00:00:00
   * @param format
   *          See
   *          http://java.sun.com/j2se/1.4.2/docs/api/java/text/SimpleDateFormat
   *          .html
   * @return a String in the format specified.
   */
  public Text evaluate(LongWritable unixtime, Text format) {
    if (unixtime == null || format == null) {
      return null;
    }

    return eval(unixtime.get(), format);
  }
  
  /**
   * Convert UnixTime to a string format.
   * 
   * @param unixtime
   *          The number of seconds from 1970-01-01 00:00:00
   * @return a String in default format specified.
   */
  public Text evaluate(LongWritable unixtime) {
    if (unixtime == null) {
      return null;
    }

    return eval(unixtime.get(), defaultFormat);
  }

  /**
   * Convert UnixTime to a string format.
   * 
   * @param unixtime
   *          The number of seconds from 1970-01-01 00:00:00
   * @param format
   *          See
   *          http://java.sun.com/j2se/1.4.2/docs/api/java/text/SimpleDateFormat
   *          .html
   * @return a String in the format specified.
   */
  public Text evaluate(IntWritable unixtime, Text format) {
    if (unixtime == null || format == null) {
      return null;
    }

    return eval(unixtime.get(), format);
  }

  /**
   * Internal evaluation function given the seconds from 1970-01-01 00:00:00 and
   * the output text format.
   * 
   * @param unixtime
   *          seconds of type long from 1970-01-01 00:00:00
   * @param format
   *          display format. See
   *          http://java.sun.com/j2se/1.4.2/docs/api/java/text
   *          /SimpleDateFormat.html
   * @return elapsed time in the given format.
   */
  private Text eval(long unixtime, Text format) {
    if (!format.equals(lastFormat)) {
      formatter = new SimpleDateFormat(format.toString());
      lastFormat.set(format);
    }

    // convert seconds to milliseconds
    Date date = new Date(unixtime * 1000L);
    result.set(formatter.format(date));
    return result;
  }
}
