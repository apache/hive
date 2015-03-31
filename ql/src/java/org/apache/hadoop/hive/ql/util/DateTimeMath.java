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
package org.apache.hadoop.hive.ql.util;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hive.common.util.DateUtils;


public class DateTimeMath {

  private static class NanosResult {
    public int seconds;
    public int nanos;

    public void addNanos(int leftNanos, int rightNanos) {
      seconds = 0;
      nanos = leftNanos + rightNanos;
      if (nanos < 0) {
        seconds = -1;
        nanos += DateUtils.NANOS_PER_SEC;
      } else if (nanos >= DateUtils.NANOS_PER_SEC) {
        seconds = 1;
        nanos -= DateUtils.NANOS_PER_SEC;
      }
    }
  }

  protected Calendar calUtc = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
  protected Calendar calLocal = Calendar.getInstance();
  protected NanosResult nanosResult = new NanosResult();

  //
  // Operations involving/returning year-month intervals
  //

  /**
   * Perform month arithmetic to millis value using UTC time zone.
   * @param millis
   * @param months
   * @return
   */
  public long addMonthsToMillisUtc(long millis, int months) {
    calUtc.setTimeInMillis(millis);
    calUtc.add(Calendar.MONTH, months);
    return calUtc.getTimeInMillis();
  }

  /**
   * Perform month arithmetic to millis value using local time zone.
   * @param millis
   * @param months
   * @return
   */
  public long addMonthsToMillisLocal(long millis, int months) {
    calLocal.setTimeInMillis(millis);
    calLocal.add(Calendar.MONTH, months);
    return calLocal.getTimeInMillis();
  }

  public long addMonthsToNanosUtc(long nanos, int months) {
    long result = addMonthsToMillisUtc(nanos / 1000000, months) * 1000000 + (nanos % 1000000);
    return result;
  }

  public long addMonthsToNanosLocal(long nanos, int months) {
    long result = addMonthsToMillisLocal(nanos / 1000000, months) * 1000000 + (nanos % 1000000);
    return result;
  }

  public long addMonthsToDays(long days, int months) {
    long millis = DateWritable.daysToMillis((int) days);
    millis = addMonthsToMillisLocal(millis, months);
    // Convert millis result back to days
    return DateWritable.millisToDays(millis);
  }

  public Timestamp add(Timestamp ts, HiveIntervalYearMonth interval) {
    if (ts == null || interval == null) {
      return null;
    }

    // Attempt to match Oracle semantics for timestamp arithmetic,
    // where timestamp arithmetic is done in UTC, then converted back to local timezone
    long resultMillis = addMonthsToMillisUtc(ts.getTime(), interval.getTotalMonths());
    Timestamp tsResult = new Timestamp(resultMillis);
    tsResult.setNanos(ts.getNanos());

    return tsResult;
  }

  public Date add(Date dt, HiveIntervalYearMonth interval) {
    if (dt == null || interval == null) {
      return null;
    }

    // Since Date millis value is in local timezone representation, do date arithmetic
    // using local timezone so the time remains at the start of the day.
    long resultMillis = addMonthsToMillisLocal(dt.getTime(), interval.getTotalMonths());
    return new Date(resultMillis);
  }

  public HiveIntervalYearMonth add(HiveIntervalYearMonth left, HiveIntervalYearMonth right) {
    HiveIntervalYearMonth result = null;
    if (left == null || right == null) {
      return null;
    }

    result = new HiveIntervalYearMonth(left.getTotalMonths() + right.getTotalMonths());
    return result;
  }

  public Timestamp subtract(Timestamp left, HiveIntervalYearMonth right) {
    if (left == null || right == null) {
      return null;
    }
    return add(left, right.negate());
  }

  public Date subtract(Date left, HiveIntervalYearMonth right) {
    if (left == null || right == null) {
      return null;
    }
    return add(left, right.negate());
  }

  public HiveIntervalYearMonth subtract(HiveIntervalYearMonth left, HiveIntervalYearMonth right) {
    if (left == null || right == null) {
      return null;
    }
    return add(left, right.negate());
  }

  //
  // Operations involving/returning day-time intervals
  //

  public Timestamp add(Timestamp ts, HiveIntervalDayTime interval) {
    if (ts == null || interval == null) {
      return null;
    }

    nanosResult.addNanos(ts.getNanos(), interval.getNanos());

    long newMillis = ts.getTime()
        + TimeUnit.SECONDS.toMillis(interval.getTotalSeconds() + nanosResult.seconds);
    Timestamp tsResult = new Timestamp(newMillis);
    tsResult.setNanos(nanosResult.nanos);
    return tsResult;
  }

  public HiveIntervalDayTime add(HiveIntervalDayTime left, HiveIntervalDayTime right) {
    HiveIntervalDayTime result = null;
    if (left == null || right == null) {
      return null;
    }

    nanosResult.addNanos(left.getNanos(), right.getNanos());

    long totalSeconds = left.getTotalSeconds() + right.getTotalSeconds() + nanosResult.seconds;
    result = new HiveIntervalDayTime(totalSeconds, nanosResult.nanos);
    return result;
  }

  public Timestamp subtract(Timestamp left, HiveIntervalDayTime right) {
    if (left == null || right == null) {
      return null;
    }
    return add(left, right.negate());
  }

  public HiveIntervalDayTime subtract(HiveIntervalDayTime left, HiveIntervalDayTime right) {
    if (left == null || right == null) {
      return null;
    }
    return add(left, right.negate());
  }

  public HiveIntervalDayTime subtract(Timestamp left, Timestamp right) {
    HiveIntervalDayTime result = null;
    if (left == null || right == null) {
      return null;
    }

    nanosResult.addNanos(left.getNanos(), -(right.getNanos()));

    long totalSeconds = TimeUnit.MILLISECONDS.toSeconds(left.getTime())
        - TimeUnit.MILLISECONDS.toSeconds(right.getTime()) + nanosResult.seconds;
    result = new HiveIntervalDayTime(totalSeconds, nanosResult.nanos);
    return result;
  }
}
