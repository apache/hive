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
package org.apache.hadoop.hive.serde2.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


/**
 * DateWritableV2
 * Writable equivalent of java.sql.Date.
 *
 * Dates are of the format
 *    YYYY-MM-DD
 *
 */
public class DateWritableV2 implements WritableComparable<DateWritableV2> {

  private Date date = new Date();

  /* Constructors */
  public DateWritableV2() {
  }

  public DateWritableV2(DateWritableV2 d) {
    set(d);
  }

  public DateWritableV2(Date d) {
    set(d);
  }

  public DateWritableV2(int d) {
    set(d);
  }

  /**
   * Set the DateWritableV2 based on the days since epoch date.
   * @param d integer value representing days since epoch date
   */
  public void set(int d) {
    date = Date.ofEpochDay(d);
  }

  /**
   * Set the DateWritableV2 based on the year/month/day of the date in the local timezone.
   * @param d Date value
   */
  public void set(Date d) {
    if (d == null) {
      date = new Date();
      return;
    }

    set(d.toEpochDay());
  }

  public void set(DateWritableV2 d) {
    set(d.getDays());
  }

  /**
   * @return Date value corresponding to the date in the local time zone
   */
  public Date get() {
    return date;
  }

  public int getDays() {
    return date.toEpochDay();
  }

  /**
   *
   * @return time in seconds corresponding to this DateWritableV2
   */
  public long getTimeInSeconds() {
    return date.toEpochSecond();
  }

  public static Date timeToDate(long seconds) {
    return Date.ofEpochMilli(seconds * 1000);
  }

  public static long daysToMillis(int days) {
    return Date.ofEpochDay(days).toEpochMilli();
  }

  public static int millisToDays(long millis) {
    return Date.ofEpochMilli(millis).toEpochDay();
  }

  public static int dateToDays(Date d) {
    return d.toEpochDay();
  }

  @Deprecated
  public static int dateToDays(java.sql.Date d) {
    return Date.ofEpochMilli(d.getTime()).toEpochDay();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    date.setTimeInDays(WritableUtils.readVInt(in));
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, (int) date.toEpochDay());
  }

  @Override
  public int compareTo(DateWritableV2 d) {
    return date.compareTo(d.date);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof DateWritableV2)) {
      return false;
    }
    return compareTo((DateWritableV2) o) == 0;
  }

  @Override
  public String toString() {
    return date.toString();
  }

  @Override
  public int hashCode() {
    return date.toEpochDay();
  }
}
