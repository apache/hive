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

package org.apache.hadoop.hive.ql.exec.vector;

import java.time.ZoneId;
import java.time.ZoneOffset;

import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;

public final class TimestampUtils {

  public static TimestampWritableV2 timestampColumnVectorWritable(
      TimestampColumnVector timestampColVector, int elementNum,
      TimestampWritableV2 timestampWritable) {
    java.sql.Timestamp ts = timestampColVector.asScratchTimestamp(elementNum);
    if (ts == null) {
      timestampWritable.set((Timestamp) null);
      return timestampWritable;
    }
    Timestamp newTS = Timestamp.ofEpochMilli(ts.getTime(), ts.getNanos());
    timestampWritable.set(newTS);
    return timestampWritable;
  }

  public static HiveIntervalDayTimeWritable intervalDayTimeColumnVectorWritable(
      IntervalDayTimeColumnVector intervalDayTimeColVector, int elementNum,
      HiveIntervalDayTimeWritable intervalDayTimeWritable) {
    intervalDayTimeWritable.set(intervalDayTimeColVector.asScratchIntervalDayTime(elementNum));
    return intervalDayTimeWritable;
  }

  public static String timestampScalarTypeToString(Object o) {
    if (o instanceof java.sql.Timestamp) {
      // Special handling for timestamp
      java.sql.Timestamp ts = (java.sql.Timestamp) o;
      return org.apache.hadoop.hive.common.type.Timestamp.ofEpochMilli(
          ts.getTime(), ts.getNanos()).toString();
    }
    return o.toString();
  }

  public static Timestamp fromColumnVector(TimestampColumnVector tcv, int row) {
    return Timestamp.ofEpochSecond(Math.floorDiv(tcv.time[row], 1000L), tcv.nanos[row],
        tcv.isUTC() ? ZoneOffset.UTC : ZoneId.systemDefault());
  }
}
