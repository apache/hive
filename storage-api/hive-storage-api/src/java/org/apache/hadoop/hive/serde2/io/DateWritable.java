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
package org.apache.hadoop.hive.serde2.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Date;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


/**
 * DateWritable
 * Writable equivalent of java.sql.Date.
 *
 * Dates are of the format
 *    YYYY-MM-DD
 *
 */
public class DateWritable implements WritableComparable<DateWritable> {

  private static final long MILLIS_PER_DAY = TimeUnit.DAYS.toMillis(1);

  // Local time zone. Store separately because Calendar would clone it.
  // Java TimeZone has no mention of thread safety. Use thread local instance to be safe.
  private static final ThreadLocal<TimeZone> LOCAL_TIMEZONE = new ThreadLocal<TimeZone>() {
    @Override
    protected TimeZone initialValue() {
      return Calendar.getInstance().getTimeZone();
    }
  };

  private static final ThreadLocal<Calendar> UTC_CALENDAR = new ThreadLocal<Calendar>() {
    @Override
    protected Calendar initialValue() {
      return new GregorianCalendar(TimeZone.getTimeZone("UTC"));
    }
  };
  private static final ThreadLocal<Calendar> LOCAL_CALENDAR = new ThreadLocal<Calendar>() {
    @Override
    protected Calendar initialValue() {
      return Calendar.getInstance();
    }
  };

  // Internal representation is an integer representing day offset from our epoch value 1970-01-01
  private int daysSinceEpoch = 0;

  /* Constructors */
  public DateWritable() {
  }

  public DateWritable(DateWritable d) {
    set(d);
  }

  public DateWritable(Date d) {
    set(d);
  }

  public DateWritable(int d) {
    set(d);
  }

  /**
   * Set the DateWritable based on the days since epoch date.
   * @param d integer value representing days since epoch date
   */
  public void set(int d) {
    daysSinceEpoch = d;
  }

  /**
   * Set the DateWritable based on the year/month/day of the date in the local timezone.
   * @param d Date value
   */
  public void set(Date d) {
    if (d == null) {
      daysSinceEpoch = 0;
      return;
    }

    set(dateToDays(d));
  }

  public void set(DateWritable d) {
    set(d.daysSinceEpoch);
  }

  /**
   * @return Date value corresponding to the date in the local time zone
   */
  public Date get() {
    return get(true);
  }

  // TODO: we should call this more often. In theory, for DATE type, time should never matter, but
  //       it's hard to tell w/some code paths like UDFs/OIs etc. that are used in many places.
  public Date get(boolean doesTimeMatter) {
    return new Date(daysToMillis(daysSinceEpoch, doesTimeMatter));
  }

  public int getDays() {
    return daysSinceEpoch;
  }

  /**
   *
   * @return time in seconds corresponding to this DateWritable
   */
  public long getTimeInSeconds() {
    return get().getTime() / 1000;
  }

  public static Date timeToDate(long l) {
    return new Date(l * 1000);
  }

  public static long daysToMillis(int d) {
    return daysToMillis(d, true);
  }

  public static long daysToMillis(int d, boolean doesTimeMatter) {
    // What we are trying to get is the equivalent of new Date(ymd).getTime() in the local tz,
    // where ymd is whatever d represents. How it "works" is this.
    // First we get the UTC midnight for that day (which always exists, a small island of sanity).
    long utcMidnight = d * MILLIS_PER_DAY;
    // Now we take a local TZ offset at midnight UTC. Say we are in -4; that means (surprise
    // surprise) that at midnight UTC it was 20:00 in local. So far we are on firm ground.
    long utcMidnightOffset = LOCAL_TIMEZONE.get().getOffset(utcMidnight);
    // And now we wander straight into the swamp, when instead of adding, we subtract it from UTC
    // midnight to supposedly get local midnight (in the above case, 4:00 UTC). Of course, given
    // all the insane DST variations, where we actually end up is anyone's guess.
    long hopefullyMidnight = utcMidnight - utcMidnightOffset;
    // Then we determine the local TZ offset at that magical time.
    long offsetAtHM = LOCAL_TIMEZONE.get().getOffset(hopefullyMidnight);
    // If the offsets are the same, we assume our initial jump did not cross any DST boundaries,
    // and is thus valid. Both times flowed at the same pace. We congratulate ourselves and bail.
    if (utcMidnightOffset == offsetAtHM) return hopefullyMidnight;
    // Alas, we crossed some DST boundary. If the time of day doesn't matter to the caller, we'll
    // simply get the next day and go back half a day. This is not ideal but seems to work.
    if (!doesTimeMatter) return daysToMillis(d + 1) - (MILLIS_PER_DAY >> 1);
    // Now, we could get previous and next day, figure our how many hours were inserted or removed,
    // and from which of the days, etc. But at this point our gun is pointing straight at our foot,
    // so let's just go the safe, expensive way.
    Calendar utc = UTC_CALENDAR.get(), local = LOCAL_CALENDAR.get();
    utc.setTimeInMillis(utcMidnight);
    local.set(utc.get(Calendar.YEAR), utc.get(Calendar.MONTH), utc.get(Calendar.DAY_OF_MONTH));
    return local.getTimeInMillis();
  }

  public static int millisToDays(long millisLocal) {
    // We assume millisLocal is midnight of some date. What we are basically trying to do
    // here is go from local-midnight to UTC-midnight (or whatever time that happens to be).
    long millisUtc = millisLocal + LOCAL_TIMEZONE.get().getOffset(millisLocal);
    int days;
    if (millisUtc >= 0L) {
      days = (int) (millisUtc / MILLIS_PER_DAY);
    } else {
      days = (int) ((millisUtc - 86399999 /*(MILLIS_PER_DAY - 1)*/) / MILLIS_PER_DAY);
    }
    return days;
  }

  public static int dateToDays(Date d) {
    // convert to equivalent time in UTC, then get day offset
    long millisLocal = d.getTime();
    return millisToDays(millisLocal);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    daysSinceEpoch = WritableUtils.readVInt(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, daysSinceEpoch);
  }

  @Override
  public int compareTo(DateWritable d) {
    return daysSinceEpoch - d.daysSinceEpoch;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof DateWritable)) {
      return false;
    }
    return compareTo((DateWritable) o) == 0;
  }

  @Override
  public String toString() {
    // For toString, the time does not matter
    return get(false).toString();
  }

  @Override
  public int hashCode() {
    return daysSinceEpoch;
  }
}
