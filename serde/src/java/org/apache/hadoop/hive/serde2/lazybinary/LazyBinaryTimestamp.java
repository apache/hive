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
package org.apache.hadoop.hive.serde2.lazybinary;

import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableTimestampObjectInspector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * LazyBinaryTimestamp
 * A LazyBinaryObject that encodes a java.sql.Timestamp 4 to 9 bytes.
 *
 */
public class LazyBinaryTimestamp extends
    LazyBinaryPrimitive<WritableTimestampObjectInspector, TimestampWritableV2> {
  static final Logger LOG = LoggerFactory.getLogger(LazyBinaryTimestamp.class);

  private final boolean legacyConversionEnabled;

  LazyBinaryTimestamp(WritableTimestampObjectInspector oi, boolean legacyConversionEnabled) {
    super(oi);
    data = new TimestampWritableV2();
    this.legacyConversionEnabled = legacyConversionEnabled;
  }

  /**
   * Initializes LazyBinaryTimestamp object
   * @param bytes
   * @param start
   * @param length
   *    If length is 4, no decimal bytes follow, otherwise read following bytes
   *    as VInt and reverse its value
   */
  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    if (!legacyConversionEnabled) {
      // Hive 3.x default: interpret as UTC
      data.set(bytes.getData(), start);
    } else {
      // Legacy Hive 2.x behavior: interpret as local time
      data.set(bytes.getData(), start);

      // Convert to java.sql.Timestamp
      Timestamp ts = data.getTimestamp();
      java.sql.Timestamp javaTs = ts.toSqlTimestamp();
      Instant utcInstant = javaTs.toInstant();

      // Shift from UTC to local time
      LocalDateTime localDateTime = LocalDateTime.ofInstant(utcInstant, ZoneId.systemDefault());

      // Format in SQL-style string for Hive Timestamp parser
      String formatted = localDateTime.toString().replace('T', ' ');

      // Construct Hive Timestamp using string-based factory
      Timestamp legacyTs = Timestamp.valueOf(formatted);

      data.set(legacyTs);
    }
  }
}
