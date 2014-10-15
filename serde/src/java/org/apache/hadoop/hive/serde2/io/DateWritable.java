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
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.ByteStream.RandomAccessOutput;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils.VInt;
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
  private static final Log LOG = LogFactory.getLog(DateWritable.class);

  private static final long MILLIS_PER_DAY = TimeUnit.DAYS.toMillis(1);

  // Local time zone.
  // Java TimeZone has no mention of thread safety. Use thread local instance to be safe.
  private static final ThreadLocal<TimeZone> LOCAL_TIMEZONE = new ThreadLocal<TimeZone>() {
    @Override
    protected TimeZone initialValue() {
      return Calendar.getInstance().getTimeZone();
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
   *
   * @return Date value corresponding to the date in the local time zone
   */
  public Date get() {
    return new Date(daysToMillis(daysSinceEpoch));
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
    // Convert from day offset to ms in UTC, then apply local timezone offset.
    long millisUtc = d * MILLIS_PER_DAY;
    long tmp =  millisUtc - LOCAL_TIMEZONE.get().getOffset(millisUtc);
    // Between millisUtc and tmp, the time zone offset may have changed due to DST.
    // Look up the offset again.
    return millisUtc - LOCAL_TIMEZONE.get().getOffset(tmp);
  }

  public static int dateToDays(Date d) {
    // convert to equivalent time in UTC, then get day offset
    long millisLocal = d.getTime();
    long millisUtc = millisLocal + LOCAL_TIMEZONE.get().getOffset(millisLocal);
    return (int)(millisUtc / MILLIS_PER_DAY);
  }

  public void setFromBytes(byte[] bytes, int offset, int length, VInt vInt) {
    LazyBinaryUtils.readVInt(bytes, offset, vInt);
    assert (length == vInt.length);
    set(vInt.value);
  }

  public void writeToByteStream(RandomAccessOutput byteStream) {
    LazyBinaryUtils.writeVInt(byteStream, getDays());
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
    return get().toString();
  }

  @Override
  public int hashCode() {
    return daysSinceEpoch;
  }
}
