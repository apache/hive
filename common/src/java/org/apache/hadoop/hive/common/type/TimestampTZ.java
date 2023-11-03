/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.common.type;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * This is the internal type for Timestamp with time zone.
 * The full qualified input format of Timestamp with time zone is
 * "yyyy-MM-dd HH:mm:ss[.SSS...] zoneid/zoneoffset", where the time and zone parts are optional.
 * If time part is absent, a default '00:00:00.0' will be used.
 * If zone part is absent, the system time zone will be used.
 */
public class TimestampTZ implements Comparable<TimestampTZ> {

  private static final ZonedDateTime EPOCH = ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);

  private ZonedDateTime zonedDateTime;

  public TimestampTZ() {
    this(EPOCH);
  }

  public TimestampTZ(ZonedDateTime zonedDateTime) {
    setZonedDateTime(zonedDateTime);
  }

  public TimestampTZ(long seconds, int nanos, ZoneId timeZone) {
    set(seconds, nanos, timeZone);
  }

  /**
   * Obtains an instance of Instant using seconds from the epoch of 1970-01-01T00:00:00Z and
   * nanosecond fraction of second. Then, it creates a zoned date-time with the same instant
   * as that specified but in the given time-zone.
   */
  public void set(long seconds, int nanos, ZoneId timeZone) {
    Instant instant = Instant.ofEpochSecond(seconds, nanos);
    setZonedDateTime(ZonedDateTime.ofInstant(instant, timeZone));
  }

  public ZonedDateTime getZonedDateTime() {
    return zonedDateTime;
  }

  public void setZonedDateTime(ZonedDateTime zonedDateTime) {
    this.zonedDateTime = zonedDateTime != null ? zonedDateTime : EPOCH;
  }

  @Override
  public String toString() {
    return zonedDateTime.format(TimestampTZUtil.FORMATTER);
  }

  @Override
  public int hashCode() {
    return zonedDateTime.toInstant().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof TimestampTZ) {
      return compareTo((TimestampTZ) other) == 0;
    }
    return false;
  }

  @Override
  public int compareTo(TimestampTZ o) {
    return zonedDateTime.toInstant().compareTo(o.zonedDateTime.toInstant());
  }

  public long getEpochSecond() {
    return zonedDateTime.toInstant().getEpochSecond();
  }

  public long toEpochMilli() {
    return zonedDateTime.toInstant().toEpochMilli();
  }

  public int getNanos() {
    return zonedDateTime.toInstant().getNano();
  }

  public Instant toInstant() {
    return zonedDateTime.toInstant();
  }
}
