/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet.timestamp;

import java.time.ZoneId;
import java.time.ZoneOffset;

import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;

public class ParquetTimestampUtils {
  private static final long MILLI = 1000;
  private static final long MICRO = 1_000_000;
  private static final long NANO = 1_000_000_000;

  public static Timestamp getTimestamp(long value, TimeUnit timeUnit, boolean isAdjustedToUTC) {

    ZoneId zone = ZoneOffset.UTC;
    if (isAdjustedToUTC) {
      zone = ZoneId.systemDefault();
    }
    long seconds = 0L;
    long nanoseconds = 0L;

    switch (timeUnit) {
    case MILLIS:
      seconds = value / MILLI;
      nanoseconds = (value % MILLI) * MICRO;
      break;

    case MICROS:
      seconds = value / MICRO;
      nanoseconds = (value % MICRO) * MILLI;
      break;

    case NANOS:
      seconds = value / NANO;
      nanoseconds = (value % NANO);
      break;
    default:
      break;
    }
    return Timestamp.ofEpochSecond(seconds, nanoseconds, zone);
  }
}