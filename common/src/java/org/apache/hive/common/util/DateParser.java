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
package org.apache.hive.common.util;

import java.sql.Date;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;

/**
 * Date parser class for Hive.
 */
public class DateParser {
  private final SimpleDateFormat formatter;
  private final ParsePosition pos;
  public DateParser() {
    formatter = new SimpleDateFormat("yyyy-MM-dd");
    // TODO: ideally, we should set formatter.setLenient(false);
    pos = new ParsePosition(0);
  }

  public Date parseDate(String strValue) {
    Date result = new Date(0);
    if (parseDate(strValue, result)) {
      return result;
    }
    return null;
  }

  public boolean parseDate(String strValue, Date result) {
    pos.setIndex(0);
    java.util.Date parsedVal = formatter.parse(strValue, pos);
    if (parsedVal == null) {
      return false;
    }
    result.setTime(parsedVal.getTime());
    return true;
  }
}
