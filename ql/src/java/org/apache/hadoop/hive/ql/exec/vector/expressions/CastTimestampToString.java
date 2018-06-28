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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

public class CastTimestampToString extends TimestampToStringUnaryUDF {
  private static final long serialVersionUID = 1L;
  protected transient Timestamp dt = new Timestamp(0);
  private static final DateTimeFormatter PRINT_FORMATTER;

  static {
    DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
    // Date and time parts
    builder.append(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    // Fractional part
    builder.optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd();
    PRINT_FORMATTER = builder.toFormatter();
  }

  public CastTimestampToString() {
    super();
  }

  public CastTimestampToString(int inputColumn, int outputColumnNum) {
    super(inputColumn, outputColumnNum);
  }

  // The assign method will be overridden for CHAR and VARCHAR.
  protected void assign(BytesColumnVector outV, int i, byte[] bytes, int length) {
    outV.setVal(i, bytes, 0, length);
  }

  @Override
  protected void func(BytesColumnVector outV, TimestampColumnVector inV, int i) {
    dt.setTime(inV.time[i]);
    dt.setNanos(inV.nanos[i]);
    byte[] temp = LocalDateTime.ofInstant(Instant.ofEpochMilli(inV.time[i]), ZoneOffset.UTC)
        .withNano(inV.nanos[i])
        .format(PRINT_FORMATTER).getBytes();
    assign(outV, i, temp, temp.length);
  }
}
