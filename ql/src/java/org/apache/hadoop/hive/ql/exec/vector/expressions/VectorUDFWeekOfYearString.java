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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.io.Text;

import java.nio.charset.CharacterCodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Expression to get week of year.
 * Extends {@link VectorUDFTimestampFieldString}
 */
public final class VectorUDFWeekOfYearString extends VectorUDFTimestampFieldString {

  private static final long serialVersionUID = 1L;

  private transient final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

  public VectorUDFWeekOfYearString(int colNum, int outputColumnNum) {
    super(colNum, outputColumnNum, -1, -1);
  }

  public VectorUDFWeekOfYearString() {
    super();
  }

  @Override
  public void initCalendar() {

    // code copied over from UDFWeekOfYear implementation
    calendar.setFirstDayOfWeek(Calendar.MONDAY);
    calendar.setMinimalDaysInFirstWeek(4);
  }

  @Override
  protected long doGetField(byte[] bytes, int start, int length) throws ParseException {
    Date date = null;
    try {
      String decoded = Text.decode(bytes, start, length);
      date = format.parse(decoded);
    } catch (CharacterCodingException e) {
      throw new ParseException(e.getMessage(), 0);
    }
    calendar.setTime(date);
    return calendar.get(Calendar.WEEK_OF_YEAR);
  }
}
