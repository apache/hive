/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.common.type.TimestampTZUtil;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;

import java.util.function.Function;

public class ParquetTypeUtils {

  private static <T> T parseString(byte[] bytes, Function<String, T> parser) {
    if (bytes == null || bytes.length < 8) {
      return null;
    }
    try {
      String s = new String(bytes, StandardCharsets.UTF_8);
      return parser.apply(s);
    } catch (Exception e) {
      return null;
    }
  }

  public static Date parseDate(byte[] bytes) {
    return parseString(bytes, Date::valueOf);
  }

  public static Timestamp parseTimestamp(byte[] bytes) {
    return parseString(bytes, Timestamp::valueOf);
  }

  public static TimestampTZ parseTimestampTZ(byte[] bytes, ZoneId defaultTimeZone) {
    return parseString(bytes, s -> TimestampTZUtil.parse(s, defaultTimeZone));
  }
}
