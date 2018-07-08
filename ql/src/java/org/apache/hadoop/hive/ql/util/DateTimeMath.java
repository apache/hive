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
package org.apache.hadoop.hive.ql.util;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hive.common.util.DateUtils;

import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;


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
  public long addMonthsToMillis(long millis, int months) {
    calUtc.setTimeInMillis(millis);
    calUtc.add(Calendar.MONTH, months);
    return calUtc.getTimeInMillis();
  }

  public long addMonthsToNanos(long nanos, int months) {
    long result = addMonthsToMillis(nanos / 1000000, months) * 1000000 + (nanos % 1000000);
    return result;
  }

  public long addMonthsToDays(long days, int months) {
    long millis = DateWritableV2.daysToMillis((int) days);
    millis = addMonthsToMillis(millis, months);
    // Convert millis result back to days
    return DateWritableV2.millisToDays(millis);
  }

  public Timestamp add(Timestamp ts, HiveIntervalYearMonth interval) {
    if (ts == null || interval == null) {
      return null;
    }

    Timestamp tsResult = new Timestamp();
    add(ts, interval, tsResult);

    return tsResult;
  }

  @Deprecated
  public java.sql.Timestamp add(java.sql.Timestamp ts, HiveIntervalYearMonth interval) {
    if (ts == null || interval == null) {
      return null;
    }

    java.sql.Timestamp tsResult = new java.sql.Timestamp(0);
    add(ts, interval, tsResult);

    return tsResult;
  }

  public boolean add(Timestamp ts, HiveIntervalYearMonth interval, Timestamp result) {
    if (ts == null || interval == null) {
      return false;
    }

    long resultMillis = addMonthsToMillis(ts.toEpochMilli(), interval.getTotalMonths());
    result.setTimeInMillis(resultMillis, ts.getNanos());

    return true;
  }

  @Deprecated
  public boolean add(java.sql.Timestamp ts, HiveIntervalYearMonth interval, java.sql.Timestamp result) {
    if (ts == null || interval == null) {
      return false;
    }

    // Attempt to match Oracle semantics for timestamp arithmetic,
    // where timestamp arithmetic is done in UTC, then converted back to local timezone
    long resultMillis = addMonthsToMillis(ts.getTime(), interval.getTotalMonths());
    result.setTime(resultMillis);
    result.setNanos(ts.getNanos());

    return true;
  }

  public Timestamp add(HiveIntervalYearMonth interval, Timestamp ts) {
    if (ts == null || interval == null) {
      return null;
    }

    Timestamp tsResult = new Timestamp();
    add(interval, ts, tsResult);

    return tsResult;
  }

  @Deprecated
  public java.sql.Timestamp add(HiveIntervalYearMonth interval, java.sql.Timestamp ts) {
    if (ts == null || interval == null) {
      return null;
    }

    java.sql.Timestamp tsResult = new java.sql.Timestamp(0);
    add(interval, ts, tsResult);

    return tsResult;
  }

  public boolean add(HiveIntervalYearMonth interval, Timestamp ts, Timestamp result) {
    if (ts == null || interval == null) {
      return false;
    }

    long resultMillis = addMonthsToMillis(ts.toEpochMilli(), interval.getTotalMonths());
    result.setTimeInMillis(resultMillis, ts.getNanos());

    return true;
  }

  @Deprecated
  public boolean add(HiveIntervalYearMonth interval, java.sql.Timestamp ts, java.sql.Timestamp result) {
    if (ts == null || interval == null) {
      return false;
    }

    long resultMillis = addMonthsToMillis(ts.getTime(), interval.getTotalMonths());
    result.setTime(resultMillis);
    result.setNanos(ts.getNanos());

    return true;
  }

  public Date add(Date dt, HiveIntervalYearMonth interval) {
    if (dt == null || interval == null) {
      return null;
    }

    Date dtResult = new Date();
    add(dt, interval, dtResult);

    return dtResult;
  }

  @Deprecated
  public java.sql.Date add(java.sql.Date dt, HiveIntervalYearMonth interval) {
    if (dt == null || interval == null) {
      return null;
    }

    java.sql.Date dtResult = new java.sql.Date(0);
    add(dt, interval, dtResult);

    return dtResult;
  }

  public boolean add(Date dt, HiveIntervalYearMonth interval, Date result) {
    if (dt == null || interval == null) {
      return false;
    }

    long resultMillis = addMonthsToMillis(dt.toEpochMilli(), interval.getTotalMonths());
    result.setTimeInMillis(resultMillis);
    return true;
  }

  @Deprecated
  public boolean add(java.sql.Date dt, HiveIntervalYearMonth interval, java.sql.Date result) {
    if (dt == null || interval == null) {
      return false;
    }

    long resultMillis = addMonthsToMillis(dt.getTime(), interval.getTotalMonths());
    result.setTime(resultMillis);
    return true;
  }

  public Date add(HiveIntervalYearMonth interval, Date dt) {
    if (dt == null || interval == null) {
      return null;
    }

    Date dtResult = new Date();
    add(interval, dt, dtResult);

    return dtResult;
  }

  @Deprecated
  public java.sql.Date add(HiveIntervalYearMonth interval, java.sql.Date dt) {
    if (dt == null || interval == null) {
      return null;
    }

    java.sql.Date dtResult = new java.sql.Date(0);
    add(interval, dt, dtResult);

    return dtResult;
  }

  public boolean add(HiveIntervalYearMonth interval, Date dt, Date result) {
    if (dt == null || interval == null) {
      return false;
    }

    long resultMillis = addMonthsToMillis(dt.toEpochMilli(), interval.getTotalMonths());
    result.setTimeInMillis(resultMillis);
    return true;
  }

  @Deprecated
  public boolean add(HiveIntervalYearMonth interval, java.sql.Date dt, java.sql.Date result) {
    if (dt == null || interval == null) {
      return false;
    }

    long resultMillis = addMonthsToMillis(dt.getTime(), interval.getTotalMonths());
    result.setTime(resultMillis);
    return true;
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

    Timestamp tsResult = new Timestamp();
    subtract(left, right, tsResult);

    return tsResult;
  }

  @Deprecated
  public java.sql.Timestamp subtract(java.sql.Timestamp left, HiveIntervalYearMonth right) {
    if (left == null || right == null) {
      return null;
    }

    java.sql.Timestamp tsResult = new java.sql.Timestamp(0);
    subtract(left, right, tsResult);

    return tsResult;
  }

  public boolean subtract(Timestamp left, HiveIntervalYearMonth right, Timestamp result) {
    if (left == null || right == null) {
      return false;
    }
    return add(left, right.negate(), result);
  }

  @Deprecated
  public boolean subtract(java.sql.Timestamp left, HiveIntervalYearMonth right, java.sql.Timestamp result) {
    if (left == null || right == null) {
      return false;
    }
    return add(left, right.negate(), result);
  }

  public Date subtract(Date left, HiveIntervalYearMonth right) {
    if (left == null || right == null) {
      return null;
    }

    Date dtResult = new Date();
    subtract(left, right, dtResult);

    return dtResult;
  }

  @Deprecated
  public java.sql.Date subtract(java.sql.Date left, HiveIntervalYearMonth right) {
    if (left == null || right == null) {
      return null;
    }

    java.sql.Date dtResult = new java.sql.Date(0);
    subtract(left, right, dtResult);

    return dtResult;
  }

  public boolean subtract(Date left, HiveIntervalYearMonth right, Date result) {
    if (left == null || right == null) {
      return false;
    }
    return add(left, right.negate(), result);
  }

  @Deprecated
  public boolean subtract(java.sql.Date left, HiveIntervalYearMonth right, java.sql.Date result) {
    if (left == null || right == null) {
      return false;
    }
    return add(left, right.negate(), result);
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

    Timestamp tsResult = new Timestamp();
    add(ts, interval, tsResult);

    return tsResult;
  }

  @Deprecated
  public java.sql.Timestamp add(java.sql.Timestamp ts, HiveIntervalDayTime interval) {
    if (ts == null || interval == null) {
      return null;
    }

    java.sql.Timestamp tsResult = new java.sql.Timestamp(0);
    add(ts, interval, tsResult);

    return tsResult;
  }

  public boolean add(Timestamp ts, HiveIntervalDayTime interval,
      Timestamp result) {
    if (ts == null || interval == null) {
      return false;
    }

    nanosResult.addNanos(ts.getNanos(), interval.getNanos());

    long newMillis = ts.toEpochMilli()
        + TimeUnit.SECONDS.toMillis(interval.getTotalSeconds() + nanosResult.seconds);
    result.setTimeInMillis(newMillis, nanosResult.nanos);
    return true;
  }

  @Deprecated
  public boolean add(java.sql.Timestamp ts, HiveIntervalDayTime interval,
      java.sql.Timestamp result) {
    if (ts == null || interval == null) {
      return false;
    }

    nanosResult.addNanos(ts.getNanos(), interval.getNanos());

    long newMillis = ts.getTime()
        + TimeUnit.SECONDS.toMillis(interval.getTotalSeconds() + nanosResult.seconds);
    result.setTime(newMillis);
    result.setNanos(nanosResult.nanos);
    return true;
  }

  public Timestamp add(HiveIntervalDayTime interval, Timestamp ts) {
    if (ts == null || interval == null) {
      return null;
    }

    Timestamp tsResult = new Timestamp();
    add(interval, ts, tsResult);
    return tsResult;
  }

  @Deprecated
  public java.sql.Timestamp add(HiveIntervalDayTime interval, java.sql.Timestamp ts) {
    if (ts == null || interval == null) {
      return null;
    }

    java.sql.Timestamp tsResult = new java.sql.Timestamp(0);
    add(interval, ts, tsResult);
    return tsResult;
  }

  public boolean add(HiveIntervalDayTime interval, Timestamp ts,
      Timestamp result) {
    if (ts == null || interval == null) {
      return false;
    }

    nanosResult.addNanos(ts.getNanos(), interval.getNanos());

    long newMillis = ts.toEpochMilli()
        + TimeUnit.SECONDS.toMillis(interval.getTotalSeconds() + nanosResult.seconds);
    result.setTimeInMillis(newMillis, nanosResult.nanos);
    return true;
  }

  @Deprecated
  public boolean add(HiveIntervalDayTime interval, java.sql.Timestamp ts,
      java.sql.Timestamp result) {
    if (ts == null || interval == null) {
      return false;
    }

    nanosResult.addNanos(ts.getNanos(), interval.getNanos());

    long newMillis = ts.getTime()
        + TimeUnit.SECONDS.toMillis(interval.getTotalSeconds() + nanosResult.seconds);
    result.setTime(newMillis);
    result.setNanos(nanosResult.nanos);
    return true;
  }

  public HiveIntervalDayTime add(HiveIntervalDayTime left, HiveIntervalDayTime right) {
    if (left == null || right == null) {
      return null;
    }

    HiveIntervalDayTime result = new HiveIntervalDayTime();
    add(left, right, result);
 
    return result;
  }

  public boolean add(HiveIntervalDayTime left, HiveIntervalDayTime right,
      HiveIntervalDayTime result) {
    if (left == null || right == null) {
      return false;
    }

    nanosResult.addNanos(left.getNanos(), right.getNanos());

    long totalSeconds = left.getTotalSeconds() + right.getTotalSeconds() + nanosResult.seconds;
    result.set(totalSeconds, nanosResult.nanos);
    return true;
  }

  public Timestamp subtract(Timestamp left, HiveIntervalDayTime right) {
    if (left == null || right == null) {
      return null;
    }
    return add(left, right.negate());
  }

  @Deprecated
  public java.sql.Timestamp subtract(java.sql.Timestamp left, HiveIntervalDayTime right) {
    if (left == null || right == null) {
      return null;
    }
    return add(left, right.negate());
  }

  public boolean subtract(Timestamp left, HiveIntervalDayTime right, Timestamp result) {
    if (left == null || right == null) {
      return false;
    }
    return add(left, right.negate(), result);
  }

  @Deprecated
  public boolean subtract(java.sql.Timestamp left, HiveIntervalDayTime right, java.sql.Timestamp result) {
    if (left == null || right == null) {
      return false;
    }
    return add(left, right.negate(), result);
  }

  public HiveIntervalDayTime subtract(HiveIntervalDayTime left, HiveIntervalDayTime right) {
    if (left == null || right == null) {
      return null;
    }
    return add(left, right.negate());
  }

  public boolean subtract(HiveIntervalDayTime left, HiveIntervalDayTime right,
      HiveIntervalDayTime result) {
    if (left == null || right == null) {
      return false;
    }
    return add(left, right.negate(), result);
  }

  public HiveIntervalDayTime subtract(Timestamp left, Timestamp right) {
    if (left == null || right == null) {
      return null;
    }

    HiveIntervalDayTime result = new HiveIntervalDayTime();
    subtract(left, right, result);

    return result;
  }

  @Deprecated
  public HiveIntervalDayTime subtract(java.sql.Timestamp left, java.sql.Timestamp right) {
    if (left == null || right == null) {
      return null;
    }

    HiveIntervalDayTime result = new HiveIntervalDayTime();
    subtract(left, right, result);

    return result;
  }

  public boolean subtract(Timestamp left, Timestamp right,
      HiveIntervalDayTime result) {
    if (left == null || right == null) {
      return false;
    }

    nanosResult.addNanos(left.getNanos(), -(right.getNanos()));

    long totalSeconds = TimeUnit.MILLISECONDS.toSeconds(left.toEpochMilli())
        - TimeUnit.MILLISECONDS.toSeconds(right.toEpochMilli()) + nanosResult.seconds;
    result.set(totalSeconds, nanosResult.nanos);
    return true;
  }

  @Deprecated
  public boolean subtract(java.sql.Timestamp left, java.sql.Timestamp right,
      HiveIntervalDayTime result) {
    if (left == null || right == null) {
      return false;
    }

    nanosResult.addNanos(left.getNanos(), -(right.getNanos()));

    long totalSeconds = TimeUnit.MILLISECONDS.toSeconds(left.getTime())
        - TimeUnit.MILLISECONDS.toSeconds(right.getTime()) + nanosResult.seconds;
    result.set(totalSeconds, nanosResult.nanos);
    return true;
  }
}
