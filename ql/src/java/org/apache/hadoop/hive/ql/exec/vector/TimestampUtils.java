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

package org.apache.hadoop.hive.ql.exec.vector;

import java.sql.Timestamp;

import org.apache.hadoop.hive.serde2.io.DateWritable;

public final class TimestampUtils {

  /**
   * Store the given timestamp in nanoseconds into the timestamp object.
   * @param timeInNanoSec Given timestamp in nanoseconds
   * @param t             The timestamp object
   */
  public static void assignTimeInNanoSec(long timeInNanoSec, Timestamp t) {
    /*
     * java.sql.Timestamp consists of a long variable to store milliseconds and an integer variable for nanoseconds.
     * The long variable is used to store only the full seconds converted to millis. For example for 1234 milliseconds,
     * 1000 is stored in the long variable, and 234000000 (234 converted to nanoseconds) is stored as nanoseconds.
     * The negative timestamps are also supported, but nanoseconds must be positive therefore millisecond part is
     * reduced by one second.
     */
    long integralSecInMillis = (timeInNanoSec / 1000000000) * 1000; // Full seconds converted to millis.
    long nanos = timeInNanoSec % 1000000000; // The nanoseconds.
    if (nanos < 0) {
      nanos = 1000000000 + nanos; // The positive nano-part that will be added to milliseconds.
      integralSecInMillis = ((timeInNanoSec / 1000000000) - 1) * 1000; // Reduce by one second.
    }
    t.setTime(integralSecInMillis);
    t.setNanos((int) nanos);
  }

  public static long getTimeNanoSec(Timestamp t) {
    long time = t.getTime();
    int nanos = t.getNanos();
    return (time * 1000000) + (nanos % 1000000);
  }

  public static long secondsToNanoseconds(long seconds) {
    return seconds * 1000000000;
  }

  public static long doubleToNanoseconds(double d) {
    return (long) (d * 1000000000);
  }

  public static long daysToNanoseconds(long daysSinceEpoch) {
    return DateWritable.daysToMillis((int) daysSinceEpoch) * 1000000;
  }
}
