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
package org.apache.hadoop.hive.common.type;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

/**
 * Pisa project is named after the famous Leonardo of Pisa, or better known as Fibanacci.
 *
 * A Pisa timestamp is a timestamp without a time-zone (i.e. local) in the ISO-8601 calendar system,
 * such as 2007-12-03 10:15:30.0123456789, with accuracy to the nanosecond (1 billionth of a
 * second).
 *
 * Pisa timestamps use the same starting point as a java.sql.Timestamp -- the number of nanoseconds
 * since the epoch (1970-01-01, or the day Unix roared awake) where negative numbers represent
 * earlier days.
 *
 * However, we use the PisaTimestamp class which has different design requirements than
 * java.sql.Timestamp.  It is designed to be mutable and NOT thread-safe to avoid high memory
 * allocation / garbage collection costs.  And, provides for ease of use by our vectorization
 * code to avoid the high CPU data cache miss cost for small objects, too.  We do this by allowing
 * the epoch day and nano of day to be stored externally (i.e. vector arrays).
 *
 * And, importantly, PisaTimestamp is a light-weight class similar to the epochDay/NanoOfDay of
 * the newer Java 8 LocalDateTime class, except the timestamp is *indifferent* to timezone.
 *
 * A common usage would be to treat it as UTC.
 *
 * You can work with days, seconds, milliseconds, nanoseconds, etc.  But to work with months you
 * will need to convert to an external timestamp object and use calendars, etc.
 * *
 * The storage for a PisaTimestamp is:
 *
 *        long epochDay
 *            // The number of days since 1970-01-01 (==> similar to Java 8 LocalDate).
 *        long nanoOfDay
 *            // The number of nanoseconds within the day, with the range of
 *            //  0 to 24 * 60 * 60 * 1,000,000,000 - 1 (==> similar to Java 8 LocalTime).
 *
 * Both epochDay and nanoOfDay are signed.
 *
 * We when both epochDay and nanoOfDay are non-zero, we will maintain them so they have the
 * same sign.
 *
 */

public class PisaTimestamp {

  private static final long serialVersionUID = 1L;

  private long epochDay;
  private long nanoOfDay;

  private Timestamp scratchTimestamp;

  public static final long NANOSECONDS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);
  public static final long NANOSECONDS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);
  public static final long NANOSECONDS_PER_DAY = TimeUnit.DAYS.toNanos(1);

  public static final long MILLISECONDS_PER_SECOND = TimeUnit.SECONDS.toMillis(1);
  public static final long MILLISECONDS_PER_DAY = TimeUnit.DAYS.toMillis(1);

  public static final long SECONDS_PER_DAY = TimeUnit.DAYS.toSeconds(1);

  public static final long MIN_NANO_OF_DAY = -NANOSECONDS_PER_DAY;
  public static final long MAX_NANO_OF_DAY = NANOSECONDS_PER_DAY;

  public static final BigDecimal BIG_NANOSECONDS_PER_SECOND = new BigDecimal(NANOSECONDS_PER_SECOND);

  public long getEpochDay() {
    return epochDay;
  }

  public long getNanoOfDay() {
    return nanoOfDay;
  }

  public PisaTimestamp() {
    epochDay = 0;
    nanoOfDay = 0;
    scratchTimestamp = new Timestamp(0);
  }

  public PisaTimestamp(long epochDay, long nanoOfDay) {

    Preconditions.checkState(validateIntegrity(epochDay, nanoOfDay),
        "epochDay " + epochDay + ", nanoOfDay " + nanoOfDay + " not valid");

    this.epochDay = epochDay;
    this.nanoOfDay = nanoOfDay;
    scratchTimestamp = new Timestamp(0);
  }

  public PisaTimestamp(Timestamp timestamp) {
    super();
    updateFromTimestamp(timestamp);
  }

  public void reset() {
    epochDay = 0;
    nanoOfDay = 0;
  }

  /**
   * NOTE: This method validates the integrity rules between epoch day and nano of day,
   * but not overflow/underflow of epoch day.  Since epoch day overflow/underflow can result
   * from to client data input, that must be checked manually with <undone> as this
   * class do not throw data range exceptions as a rule.  It leaves that choice to the caller.
   * @param epochDay
   * @param nanoOfDay
   * @return true if epoch day and nano of day have integrity.
   */
  public static boolean validateIntegrity(long epochDay, long nanoOfDay) {

    // Range check nano per day as invariant.
    if (nanoOfDay >= NANOSECONDS_PER_DAY || nanoOfDay <= -NANOSECONDS_PER_DAY) {
      return false;
    }

    // Signs of epoch day and nano of day must match.
    if (!(epochDay >= 0 && nanoOfDay >= 0 ||
          epochDay <= 0 && nanoOfDay <= 0)) {
      return false;
    }

    return true;
  }

  /**
   * Set this PisaTimestamp from another PisaTimestamp.
   * @param source
   * @return this
   */
  public PisaTimestamp update(PisaTimestamp source) {
    this.epochDay = source.epochDay;
    this.nanoOfDay = source.nanoOfDay;
    return this;
  }

  /**
   * Set this PisaTimestamp from a epoch day and nano of day.
   * @param epochDay
   * @param nanoOfDay
   * @return this
   */
  public PisaTimestamp update(long epochDay, long nanoOfDay) {

    Preconditions.checkState(validateIntegrity(epochDay, nanoOfDay),
        "epochDay " + epochDay + ", nanoOfDay " + nanoOfDay + " not valid");

    this.epochDay = epochDay;
    this.nanoOfDay = nanoOfDay;
    return this;
  }

  /**
   * Set the PisaTimestamp from a Timestamp object.
   * @param timestamp
   * @return this
   */
  public PisaTimestamp updateFromTimestamp(Timestamp timestamp) {

    long timestampTime = timestamp.getTime();
    int nanos = timestamp.getNanos();

    /**
     * Since the Timestamp class always stores nanos as a positive quantity (0 .. 999,999,999),
     * we have to adjust back the time (subtract) by 1,000,000,000 to get right quantity for
     * our calculations below.  One thing it ensures is nanoOfDay will be negative.
     */
    if (timestampTime < 0 && nanos > 0) {
      timestampTime -= MILLISECONDS_PER_SECOND;
    }

    // The Timestamp class does not use the milliseconds part (always 0).  It is covered by nanos.
    long epochSeconds = timestampTime / MILLISECONDS_PER_SECOND;

    nanoOfDay = (epochSeconds % SECONDS_PER_DAY) * NANOSECONDS_PER_SECOND + nanos;
    epochDay = epochSeconds / SECONDS_PER_DAY + (nanoOfDay / NANOSECONDS_PER_DAY);

    Preconditions.checkState(validateIntegrity(epochDay, nanoOfDay));
    return this;
  }

  /**
   * Set this PisaTimestamp from a timestamp milliseconds.
   * @param epochMilliseconds
   * @return this
   */
  public PisaTimestamp updateFromTimestampMilliseconds(long timestampMilliseconds) {
    /**
     * The Timestamp class setTime sets both the time (seconds stored as milliseconds) and
     * the nanos.
     */
    scratchTimestamp.setTime(timestampMilliseconds);
    updateFromTimestamp(scratchTimestamp);
    return this;
  }

  /**
   * Set this PisaTimestamp from a timestamp seconds.
   * @param epochMilliseconds
   * @return this
   */
  public PisaTimestamp updateFromTimestampSeconds(long timestampSeconds) {
    return updateFromTimestampMilliseconds(timestampSeconds * MILLISECONDS_PER_SECOND);
  }

  /**
   * Set this PisaTimestamp from a timestamp seconds.
   * @param epochMilliseconds
   * @return this
   */
  public PisaTimestamp updateFromTimestampSecondsWithFractionalNanoseconds(
      double timestampSecondsWithFractionalNanoseconds) {

    // Otherwise, BigDecimal throws an exception.  (Support vector operations that sometimes
    // do work on double Not-a-Number NaN values).
    if (Double.isNaN(timestampSecondsWithFractionalNanoseconds)) {
      timestampSecondsWithFractionalNanoseconds = 0;
    }
    // Algorithm used by TimestampWritable.doubleToTimestamp method.
    // Allocates a BigDecimal object!

    long seconds = (long) timestampSecondsWithFractionalNanoseconds;

    // We must ensure the exactness of the double's fractional portion.
    // 0.6 as the fraction part will be converted to 0.59999... and
    // significantly reduce the savings from binary serialization.
    BigDecimal bd;

    bd = new BigDecimal(String.valueOf(timestampSecondsWithFractionalNanoseconds));
    bd = bd.subtract(new BigDecimal(seconds));       // Get the nanos fraction.
    bd = bd.multiply(BIG_NANOSECONDS_PER_SECOND);    // Make nanos an integer.

    int nanos = bd.intValue();

    // Convert to millis
    long millis = seconds * 1000;
    if (nanos < 0) {
      millis -= 1000;
      nanos += 1000000000;
    }

    scratchTimestamp.setTime(millis);
    scratchTimestamp.setNanos(nanos);
    updateFromTimestamp(scratchTimestamp);
    return this;
  }

  /**
   * Set this PisaTimestamp from a epoch seconds and signed nanos (-999999999 to 999999999).
   * @param epochSeconds
   * @param signedNanos
   * @return this
   */
  public PisaTimestamp updateFromEpochSecondsAndSignedNanos(long epochSeconds, int signedNanos) {

    long nanoOfDay = (epochSeconds % SECONDS_PER_DAY) * NANOSECONDS_PER_SECOND + signedNanos;
    long epochDay = epochSeconds / SECONDS_PER_DAY + nanoOfDay / NANOSECONDS_PER_DAY;

    Preconditions.checkState(validateIntegrity(epochDay, nanoOfDay));

    this.epochDay = epochDay;
    this.nanoOfDay = nanoOfDay;
    return this;
  }

  /**
   * Set a scratch PisaTimestamp with this PisaTimestamp's values and return the scratch object.
   * @param epochDay
   * @param nanoOfDay
   */
  public PisaTimestamp scratchCopy(PisaTimestamp scratch) {

    scratch.epochDay = epochDay;
    scratch.nanoOfDay = nanoOfDay;
    return scratch;
  }

  /**
   * Set a Timestamp object from this PisaTimestamp.
   * @param timestamp
   */
  public void timestampUpdate(Timestamp timestamp) {

    /*
     * java.sql.Timestamp consists of a long variable to store milliseconds and an integer variable for nanoseconds.
     * The long variable is used to store only the full seconds converted to millis. For example for 1234 milliseconds,
     * 1000 is stored in the long variable, and 234000000 (234 converted to nanoseconds) is stored as nanoseconds.
     * The negative timestamps are also supported, but nanoseconds must be positive therefore millisecond part is
     * reduced by one second.
     */

    long epochSeconds = epochDay * SECONDS_PER_DAY + nanoOfDay / NANOSECONDS_PER_SECOND;
    long integralSecInMillis;
    int nanos = (int) (nanoOfDay % NANOSECONDS_PER_SECOND); // The nanoseconds.
    if (nanos < 0) {
      nanos = (int) NANOSECONDS_PER_SECOND + nanos; // The positive nano-part that will be added to milliseconds.
      integralSecInMillis = (epochSeconds - 1) * MILLISECONDS_PER_SECOND; // Reduce by one second.
    } else {
      integralSecInMillis = epochSeconds * MILLISECONDS_PER_SECOND; // Full seconds converted to millis.
    }

    timestamp.setTime(integralSecInMillis);
    timestamp.setNanos(nanos);
  }

  /**
   * Return the scratch timestamp with values from Pisa timestamp.
   * @return
   */
  public Timestamp asScratchTimestamp() {
    timestampUpdate(scratchTimestamp);
    return scratchTimestamp;
  }

  /**
   * Return the scratch timestamp for use by the caller.
   * @return
   */
  public Timestamp useScratchTimestamp() {
    return scratchTimestamp;
  }

  public int compareTo(PisaTimestamp another) {

    if (epochDay == another.epochDay) {
      if (nanoOfDay == another.nanoOfDay){
        return 0;
      } else {
        return (nanoOfDay < another.nanoOfDay ? -1 : 1);
      }
    } else {
      return (epochDay < another.epochDay ? -1: 1);
    }
  }

  public static int compareTo(long epochDay1, long nanoOfDay1, PisaTimestamp another) {

    if (epochDay1 == another.epochDay) {
      if (nanoOfDay1 == another.nanoOfDay){
        return 0;
      } else {
        return (nanoOfDay1 < another.nanoOfDay ? -1 : 1);
      }
    } else {
      return (epochDay1 < another.epochDay ? -1: 1);
    }
  }

  public static int compareTo(PisaTimestamp pisaTimestamp1, long epochDay2, long nanoOfDay2) {

    if (pisaTimestamp1.epochDay == epochDay2) {
      if (pisaTimestamp1.nanoOfDay == nanoOfDay2){
        return 0;
      } else {
        return (pisaTimestamp1.nanoOfDay < nanoOfDay2 ? -1 : 1);
      }
    } else {
      return (pisaTimestamp1.epochDay < epochDay2 ? -1: 1);
    }
  }

  public static int compareTo(long epochDay1, long nanoOfDay1, long epochDay2, long nanoOfDay2) {

    if (epochDay1 == epochDay2) {
      if (nanoOfDay1 == nanoOfDay2){
        return 0;
      } else {
        return (nanoOfDay1 < nanoOfDay2 ? -1 : 1);
      }
    } else {
      return (epochDay1 < epochDay2 ? -1: 1);
    }
  }


  /**
   * Standard equals method override.
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == null || obj.getClass() != getClass()) {
      return false;
    }
    return equals((PisaTimestamp) obj);
  }

  public boolean equals(PisaTimestamp other) {

    if (epochDay == other.epochDay) {
      if  (nanoOfDay == other.nanoOfDay) {
        return true;
      } else {
        return false;
      }
    } else {
        return false;
    }
  }

  public static void add(PisaTimestamp pisaTimestamp1, PisaTimestamp pisaTimestamp2,
      PisaTimestamp result) {
    add(pisaTimestamp1.epochDay, pisaTimestamp1.nanoOfDay,
        pisaTimestamp2.epochDay, pisaTimestamp2.nanoOfDay,
        result);
  }

  public static void add(long epochDay1, long nanoOfDay1,
      long epochDay2, long nanoOfDay2,
      PisaTimestamp result) {

    // Validate integrity rules between epoch day and nano of day.
    Preconditions.checkState(PisaTimestamp.validateIntegrity(epochDay1, nanoOfDay1));
    Preconditions.checkState(PisaTimestamp.validateIntegrity(epochDay2, nanoOfDay2));

    long intermediateEpochDay = epochDay1 + epochDay2;
    long intermediateNanoOfDay = nanoOfDay1 + nanoOfDay2;

    // Normalize so both are positive or both are negative.
    long normalizedEpochDay;
    long normalizedNanoOfDay;
    if (intermediateEpochDay > 0 && intermediateNanoOfDay < 0) {
      normalizedEpochDay = intermediateEpochDay - 1;
      normalizedNanoOfDay = intermediateNanoOfDay + NANOSECONDS_PER_DAY;
    } else if (intermediateEpochDay < 0 && intermediateNanoOfDay > 0) {
      normalizedEpochDay = intermediateEpochDay + 1;
      normalizedNanoOfDay = intermediateNanoOfDay - NANOSECONDS_PER_DAY;
    } else {
      normalizedEpochDay = intermediateEpochDay;
      normalizedNanoOfDay = intermediateNanoOfDay;
    }

    long resultEpochDay;
    long resultNanoOfDay;
    if (normalizedNanoOfDay >= NANOSECONDS_PER_DAY || normalizedNanoOfDay <= -NANOSECONDS_PER_DAY) {
      // Adjust for carry or overflow...

      resultEpochDay = normalizedEpochDay + normalizedNanoOfDay / NANOSECONDS_PER_DAY;
      resultNanoOfDay = normalizedNanoOfDay % NANOSECONDS_PER_DAY;

    } else {
      resultEpochDay = normalizedEpochDay;
      resultNanoOfDay = normalizedNanoOfDay;
    }

    // The update method will validate integrity rules between epoch day and nano of day,
    // but not overflow/underflow of epoch day.
    result.update(resultEpochDay, resultNanoOfDay);
  }

  public static void addSeconds(PisaTimestamp timestamp1, long epochSeconds, PisaTimestamp result) {
    long epochDay = epochSeconds / SECONDS_PER_DAY;
    long nanoOfDay = (epochSeconds % SECONDS_PER_DAY) * NANOSECONDS_PER_SECOND;
    add(timestamp1.epochDay, timestamp1.nanoOfDay, epochDay, nanoOfDay, result);
  }

  public static void subtract(PisaTimestamp timestamp1, PisaTimestamp timestamp2,
      PisaTimestamp result) {

    add(timestamp1.epochDay, timestamp1.nanoOfDay, -timestamp2.epochDay, -timestamp2.nanoOfDay,
        result);
  }

  public static void subtract(long epochDay1, long nanoOfDay1,
      long epochDay2, long nanoOfDay2,
      PisaTimestamp result) {

    add(epochDay1, nanoOfDay1, -epochDay2, -nanoOfDay2, result);
  }

  public static void subtractSeconds(PisaTimestamp timestamp1, long epochSeconds,
      PisaTimestamp result) {
    long epochDay = epochSeconds / SECONDS_PER_DAY;
    long nanoOfDay = (epochSeconds % SECONDS_PER_DAY) * NANOSECONDS_PER_SECOND;
    add(timestamp1.epochDay, timestamp1.nanoOfDay, -epochDay, -nanoOfDay, result);
  }

  /**
   * Rounds the number of milliseconds relative to the epoch down to the nearest whole number of
   * seconds. 500 would round to 0, -500 would round to -1.
   */
  public static long timestampMillisToSeconds(long millis) {
    if (millis >= 0) {
      return millis / 1000;
    } else {
      return (millis - 999) / 1000;
    }
  }

  /**
   * Return a double with the integer part as the seconds and the fractional part as
   * the nanoseconds the way the Timestamp class does it.
   * @return seconds.nanoseconds
   */
  public double getTimestampSecondsWithFractionalNanos() {
    // Algorithm must be the same as TimestampWritable.getDouble method.
    timestampUpdate(scratchTimestamp);
    double seconds = timestampMillisToSeconds(scratchTimestamp.getTime());
    double nanos = scratchTimestamp.getNanos();
    BigDecimal bigSeconds = new BigDecimal(seconds);
    BigDecimal bigNanos = new BigDecimal(nanos).divide(BIG_NANOSECONDS_PER_SECOND);
    return bigSeconds.add(bigNanos).doubleValue();
  }

  /**
   * Return an integer as the seconds the way the Timestamp class does it.
   * @return seconds.nanoseconds
   */
  public long getTimestampSeconds() {
    // Algorithm must be the same as TimestampWritable.getSeconds method.
    timestampUpdate(scratchTimestamp);
    return timestampMillisToSeconds(scratchTimestamp.getTime());
  }

  /**
   * Return an integer as the milliseconds the way the Timestamp class does it.
   * @return seconds.nanoseconds
   */
  public long getTimestampMilliseconds() {
    timestampUpdate(scratchTimestamp);
    return scratchTimestamp.getTime();
  }

  /**
   * Return the epoch seconds.
   * @return
   */
  public long getEpochSeconds() {
    return epochDay * SECONDS_PER_DAY + nanoOfDay / NANOSECONDS_PER_SECOND;
  }

  /**
   * Return the epoch seconds, given the epoch day and nano of day.
   * @param epochDay
   * @param nanoOfDay
   * @return
   */
  public static long getEpochSecondsFromEpochDayAndNanoOfDay(long epochDay, long nanoOfDay) {
    return epochDay * SECONDS_PER_DAY + nanoOfDay / NANOSECONDS_PER_SECOND;
  }

  /**
   * Return the signed nanos (-999999999 to 999999999).
   * NOTE: Not the same as Timestamp class nanos (which are always positive).
   */
  public int getSignedNanos() {
    return (int) (nanoOfDay % NANOSECONDS_PER_SECOND);
  }

  /**
   * Return the signed nanos (-999999999 to 999999999).
   * NOTE: Not the same as Timestamp class nanos (which are always positive).
   */
  public static int getSignedNanos(long nanoOfDay) {
    return (int) (nanoOfDay % NANOSECONDS_PER_SECOND);
  }

  /**
   * Return the epoch milliseconds.
   * @return
   */
  public long getEpochMilliseconds() {
    return epochDay * MILLISECONDS_PER_DAY + nanoOfDay / NANOSECONDS_PER_MILLISECOND;
  }

  /**
   * Return the epoch seconds, given the epoch day and nano of day.
   * @param epochDay
   * @param nanoOfDay
   * @return
   */
  public static long getEpochMillisecondsFromEpochDayAndNanoOfDay(long epochDay, long nanoOfDay) {
    return epochDay * MILLISECONDS_PER_DAY + nanoOfDay / NANOSECONDS_PER_MILLISECOND;
  }

  @Override
  public int hashCode() {
    // UNDONE: We don't want to box the longs just to get the hash codes...
    return new Long(epochDay).hashCode() ^ new Long(nanoOfDay).hashCode();
  }

  @Override
  public String toString() {
    timestampUpdate(scratchTimestamp);
    return scratchTimestamp.toString();
  }
}