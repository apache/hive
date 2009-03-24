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

import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.TimeZone;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;


@UDFType(deterministic=false)
public class UDFUnixTimeStamp extends UDF {

  private static Log LOG = LogFactory.getLog(UDFUnixTimeStamp.class.getName());

  //  For now, we just use the default time zone.
  private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public UDFUnixTimeStamp() {
  }

  /**
   * Return current UnixTime.
   * @return long Number of seconds from 1970-01-01 00:00:00
   */
  public long evaluate()  {
    Date date = new Date();
    return date.getTime() / 1000;
  }

  /**
   * Convert time string to UnixTime.
   * @param datestring Time string in format yyyy-MM-dd HH:mm:ss
   * @return long Number of seconds from 1970-01-01 00:00:00
   */
  public long evaluate(String datestring)  {
    if (datestring == null) {
      Date date = new Date();
      return date.getTime() / 1000;
    }

    try {
      Date date = (Date)formatter.parse(datestring);
      return date.getTime() / 1000;
    } catch (ParseException e) {
      return 0;
    }
  }

  /**
   * Convert time string to UnixTime with user defined pattern.
   * @param datestring Time string in format patternstring
   * @param patternstring Time patterns string supported by SimpleDateFormat
   * @return long Number of seconds from 1970-01-01 00:00:00
   */
  public long evaluate(String datestring, String patternstring)  {
    try {
      formatter.applyPattern(patternstring);
    } catch (Exception e) {
      return 0;
    }

    return evaluate(datestring);
  }
}
