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

import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Vectorized version of GenericUDFDatetimeLegacyHybridCalendar (datetime_legacy_hybrid_calendar).
 * Converts a date/timestamp to legacy hybrid Julian-Gregorian calendar assuming that its internal
 * days/milliseconds since epoch is calculated using the proleptic Gregorian calendar.
 * Extends {@link FuncTimestampToTimestamp}
 */

public class VectorUDFDatetimeLegacyHybridCalendarTimestamp extends FuncTimestampToTimestamp {
  private static final long serialVersionUID = 1L;

  // SimpleDateFormat doesn't serialize well; it's also not thread-safe
  private static final ThreadLocal<SimpleDateFormat> SIMPLE_DATE_FORMAT_THREAD_LOCAL =
      ThreadLocal.withInitial(() -> {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        formatter.setLenient(false);
        return formatter;
      });

  public VectorUDFDatetimeLegacyHybridCalendarTimestamp() {
    super();
  }

  public VectorUDFDatetimeLegacyHybridCalendarTimestamp(int inputColumn, int outputColumnNum) {
    super(inputColumn, outputColumnNum);
  }

  protected void func(TimestampColumnVector outputColVector, TimestampColumnVector inputColVector,
      int i) {
    String adjustedTimestampString = SIMPLE_DATE_FORMAT_THREAD_LOCAL.get()
        .format(new java.sql.Timestamp(inputColVector.time[i]));
    Timestamp adjustedTimestamp = Timestamp.valueOf(adjustedTimestampString);
    outputColVector.time[i] = adjustedTimestamp.toEpochMilli();
    // Nanos don't change
    outputColVector.nanos[i] = inputColVector.nanos[i];
  }
}
