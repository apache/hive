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
package org.apache.hadoop.hive.common.type;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hive.common.util.IntervalDayTimeUtils;


/**
 * Day-time interval type representing an offset in days/hours/minutes/seconds,
 * with nanosecond precision.
 * 1 day = 24 hours = 1440 minutes = 86400 seconds
 */
public class HiveIntervalDayTime implements Comparable<HiveIntervalDayTime> {

  // days/hours/minutes/seconds all represented as seconds
  protected long totalSeconds;
  protected int nanos;

  public HiveIntervalDayTime() {
  }

  public HiveIntervalDayTime(int days, int hours, int minutes, int seconds, int nanos) {
    set(days, hours, minutes, seconds, nanos);
  }

  public HiveIntervalDayTime(long seconds, int nanos) {
    set(seconds, nanos);
  }

  public HiveIntervalDayTime(BigDecimal seconds) {
    set(seconds);
  }

  public HiveIntervalDayTime(HiveIntervalDayTime other) {
    set(other.totalSeconds, other.nanos);
  }

  public int getDays() {
    return (int) TimeUnit.SECONDS.toDays(totalSeconds);
  }

  public int getHours() {
    return (int) (TimeUnit.SECONDS.toHours(totalSeconds) % TimeUnit.DAYS.toHours(1));
  }

  public int getMinutes() {
    return (int) (TimeUnit.SECONDS.toMinutes(totalSeconds) % TimeUnit.HOURS.toMinutes(1));
  }

  public int getSeconds() {
    return (int) (totalSeconds % TimeUnit.MINUTES.toSeconds(1));
  }

  public int getNanos() {
    return nanos;
  }

  /**
   * Returns days/hours/minutes all converted into seconds.
   * Nanos still need to be retrieved using getNanos()
   * @return
   */
  public long getTotalSeconds() {
    return totalSeconds;
  }

  /**
   *
   * @return double representation of the interval day time, accurate to nanoseconds
   */
  public double getDouble() {
    return totalSeconds + nanos / 1000000000;
  }

  /**
   * Ensures that the seconds and nanoseconds fields have consistent sign
   */
  protected void normalizeSecondsAndNanos() {
    if (totalSeconds > 0 && nanos < 0) {
      --totalSeconds;
      nanos += IntervalDayTimeUtils.NANOS_PER_SEC;
    } else if (totalSeconds < 0 && nanos > 0) {
      ++totalSeconds;
      nanos -= IntervalDayTimeUtils.NANOS_PER_SEC;
    }
  }

  public void set(int days, int hours, int minutes, int seconds, int nanos) {
    long totalSeconds = seconds;
    totalSeconds += TimeUnit.DAYS.toSeconds(days);
    totalSeconds += TimeUnit.HOURS.toSeconds(hours);
    totalSeconds += TimeUnit.MINUTES.toSeconds(minutes);
    totalSeconds += TimeUnit.NANOSECONDS.toSeconds(nanos);
    nanos = nanos % IntervalDayTimeUtils.NANOS_PER_SEC;

    this.totalSeconds = totalSeconds;
    this.nanos = nanos;

    normalizeSecondsAndNanos();
  }

  public void set(long seconds, int nanos) {
    this.totalSeconds = seconds;
    this.nanos = nanos;
    normalizeSecondsAndNanos();
  }

  public void set(BigDecimal totalSecondsBd) {
    long totalSeconds = totalSecondsBd.longValue();
    BigDecimal fractionalSecs = totalSecondsBd.remainder(BigDecimal.ONE);
    int nanos = fractionalSecs.multiply(IntervalDayTimeUtils.NANOS_PER_SEC_BD).intValue();
    set(totalSeconds, nanos);
  }

  public void set(HiveIntervalDayTime other) {
    set(other.getTotalSeconds(), other.getNanos());
  }

  public HiveIntervalDayTime negate() {
    return new HiveIntervalDayTime(-getTotalSeconds(), -getNanos());
  }

  @Override
  public int compareTo(HiveIntervalDayTime other) {
    long cmp = this.totalSeconds - other.totalSeconds;
    if (cmp == 0) {
      cmp = this.nanos - other.nanos;
    }
    if (cmp != 0) {
      cmp = cmp > 0 ? 1 : -1;
    }
    return (int) cmp;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof HiveIntervalDayTime)) {
      return false;
    }
    return 0 == compareTo((HiveIntervalDayTime) obj);
  }

  /**
   * Return a copy of this object.
   */
  @Override
  public Object clone() {
      return new HiveIntervalDayTime(totalSeconds, nanos);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(totalSeconds).append(nanos).toHashCode();
  }

  @Override
  public String toString() {
    // If normalize() was used, then day-hour-minute-second-nanos should have the same sign.
    // This is currently working with that assumption.
    boolean isNegative = (totalSeconds < 0 || nanos < 0);
    String daySecondSignStr = isNegative ? "-" : "";

    return String.format("%s%d %02d:%02d:%02d.%09d",
        daySecondSignStr, Math.abs(getDays()),
        Math.abs(getHours()), Math.abs(getMinutes()),
        Math.abs(getSeconds()), Math.abs(getNanos()));
  }

  public static HiveIntervalDayTime valueOf(String strVal) {
    HiveIntervalDayTime result = null;
    if (strVal == null) {
      throw new IllegalArgumentException("Interval day-time string was null");
    }
    Matcher patternMatcher = PATTERN_MATCHER.get();
    patternMatcher.reset(strVal);
    if (patternMatcher.matches()) {
      // Parse out the individual parts
      try {
        // Sign - whether interval is positive or negative
        int sign = 1;
        String field = patternMatcher.group(1);
        if (field != null && field.equals("-")) {
          sign = -1;
        }
        int days = sign *
            IntervalDayTimeUtils.parseNumericValueWithRange("day", patternMatcher.group(2),
                0, Integer.MAX_VALUE);
        byte hours = (byte) (sign *
            IntervalDayTimeUtils.parseNumericValueWithRange("hour", patternMatcher.group(3), 0, 23));
        byte minutes = (byte) (sign *
            IntervalDayTimeUtils.parseNumericValueWithRange("minute", patternMatcher.group(4), 0, 59));
        int seconds = 0;
        int nanos = 0;
        field = patternMatcher.group(5);
        if (field != null) {
          BigDecimal bdSeconds = new BigDecimal(field);
          if (bdSeconds.compareTo(IntervalDayTimeUtils.MAX_INT_BD) > 0) {
            throw new IllegalArgumentException("seconds value of " + bdSeconds + " too large");
          }
          seconds = sign * bdSeconds.intValue();
          nanos = sign * bdSeconds.subtract(new BigDecimal(bdSeconds.toBigInteger()))
              .multiply(IntervalDayTimeUtils.NANOS_PER_SEC_BD).intValue();
        }

        result = new HiveIntervalDayTime(days, hours, minutes, seconds, nanos);
      } catch (Exception err) {
        throw new IllegalArgumentException("Error parsing interval day-time string: " + strVal, err);
      }
    } else {
      throw new IllegalArgumentException(
          "Interval string does not match day-time format of 'd h:m:s.n': " + strVal);
    }

    return result;
  }

  // Simple pattern: D H:M:S.nnnnnnnnn
  private final static String PARSE_PATTERN =
      "([+|-])?(\\d+) (\\d+):(\\d+):((\\d+)(\\.(\\d+))?)";

  private static final ThreadLocal<Matcher> PATTERN_MATCHER = new ThreadLocal<Matcher>() {
      @Override
      protected Matcher initialValue() {
        return Pattern.compile(PARSE_PATTERN).matcher("");
      }
  };
}
