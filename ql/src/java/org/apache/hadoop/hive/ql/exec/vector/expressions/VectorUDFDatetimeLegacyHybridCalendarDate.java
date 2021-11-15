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

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Vectorized version of GenericUDFDatetimeLegacyHybridCalendar (datetime_legacy_hybrid_calendar).
 * Converts a date/timestamp to legacy hybrid Julian-Gregorian calendar assuming that its internal
 * days/milliseconds since epoch is calculated using the proleptic Gregorian calendar.
 * Extends {@link FuncDateToDate}
 */

public class VectorUDFDatetimeLegacyHybridCalendarDate extends FuncDateToDate {
  private static final long serialVersionUID = 1L;

  // SimpleDateFormat doesn't serialize well; it's also not thread-safe
  private static final ThreadLocal<SimpleDateFormat> SIMPLE_DATE_FORMAT_THREAD_LOCAL =
      ThreadLocal.withInitial(() -> {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        formatter.setLenient(false);
        return formatter;
      });

  public VectorUDFDatetimeLegacyHybridCalendarDate() {
    super();
  }

  public VectorUDFDatetimeLegacyHybridCalendarDate(int inputColumn, int outputColumnNum) {
    super(inputColumn, outputColumnNum);
  }

  protected void func(LongColumnVector outputColVector, LongColumnVector inputColVector, int i) {
    // get number of milliseconds from number of days
    Date inputDate = Date.ofEpochDay((int) inputColVector.vector[i]);
    java.sql.Date oldDate = new java.sql.Date(inputDate.toEpochMilli());
    Date adjustedDate = Date.valueOf(SIMPLE_DATE_FORMAT_THREAD_LOCAL.get().format(oldDate));
    outputColVector.vector[i] = adjustedDate.toEpochDay();
  }
}
