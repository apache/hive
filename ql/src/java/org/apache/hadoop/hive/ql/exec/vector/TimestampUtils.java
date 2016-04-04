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

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;

public final class TimestampUtils {

  static final long MILLISECONDS_PER_SECOND = TimeUnit.SECONDS.toMillis(1);
  static final long NANOSECONDS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);

  public static long daysToNanoseconds(long daysSinceEpoch) {
    return DateWritable.daysToMillis((int) daysSinceEpoch) * NANOSECONDS_PER_MILLISECOND;
  }

  public static TimestampWritable timestampColumnVectorWritable(
      TimestampColumnVector timestampColVector, int elementNum,
      TimestampWritable timestampWritable) {
    timestampWritable.set(timestampColVector.asScratchTimestamp(elementNum));
    return timestampWritable;
  }

  public static HiveIntervalDayTimeWritable intervalDayTimeColumnVectorWritable(
      IntervalDayTimeColumnVector intervalDayTimeColVector, int elementNum,
      HiveIntervalDayTimeWritable intervalDayTimeWritable) {
    intervalDayTimeWritable.set(intervalDayTimeColVector.asScratchIntervalDayTime(elementNum));
    return intervalDayTimeWritable;
  }
}
