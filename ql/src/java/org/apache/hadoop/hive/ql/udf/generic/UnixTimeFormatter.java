/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.TimestampTZUtil;
import org.apache.hadoop.hive.conf.HiveConf;

import java.time.ZoneId;

/**
 * Formatter for parsing and printing unixtime objects (long numbers representing seconds since epoch).
 * <p>
 * This interface provides the main entry point for print and parsing and provides factories for the 
 * available implementations of {@code UnixTimeFormatter}.
 * </p>
 * <p>
 * The patterns that are supported and their behavior depend on the underlying implementation of the interface.
 * </p>
 * <p>
 * Implementations of the interface are not meant to be thread safe.
 * </p>
 */
public interface UnixTimeFormatter {

  /**
   * Types for the built-in formatter implementations.
   */
  enum Type {
    /**
     * A formatter that supports the same patterns with {@link java.text.SimpleDateFormat}.
     */
    SIMPLE {
      @Override
      UnixTimeFormatter newFormatter(ZoneId zone) {
        return new UnixTimeSimpleDateFormatter(zone);
      }
    },
    /**
     * A formatter that supports the same patterns with {@link java.time.format.DateTimeFormatter}.
     */
    DATETIME {
      @Override
      UnixTimeFormatter newFormatter(ZoneId zone) {
        return new UnixTimeDateTimeFormatter(zone);
      }
    };
    /**
     * Creates a new formatter with the specified zone id.
     * @param zone - the zone id 
     * @return a new formatter with the specified zone id.
     */
    abstract UnixTimeFormatter newFormatter(ZoneId zone);
  }

  /**
   * Creates a formatter using the specified configuration.
   * 
   * @param conf the configuration to use, not null
   * @return the formatter based on the provided configuration, not null.
   */
  static UnixTimeFormatter ofConfiguration(Configuration conf) {
    ZoneId zoneId = TimestampTZUtil.parseTimeZone(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_LOCAL_TIME_ZONE));
    Type type = Type.valueOf(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_DATETIME_FORMATTER).toUpperCase());
    return type.newFormatter(zoneId);
  }

  /**
   * Parses the input text and converts it to seconds since epoch.
   * @param text the text to parse, not null
   * @return a long number representing the number of seconds since epoch.
   * @throws RuntimeException if unable to parse the requested text using the default behavior.
   */
  long parse(String text) throws RuntimeException;

  /**
   * Parses the input text and converts it to seconds since epoch using the specified pattern.
   * @param text the text to parse, not null
   * @param pattern the pattern to use to parse the text and resolve it to seconds since epoch
   * @return a long number representing the number of seconds since epoch.
   * @throws RuntimeException if unable to parse the requested text using the specified pattern.
   */
  long parse(String text, String pattern) throws RuntimeException;

  /**
   * Formats the specified number of seconds since epoch using the formatters default pattern.
   * <p>
   * This formats the unixtime to a String using the rules of the underlying formatter.
   * </p>
   * @param epochSeconds the number of seconds to format
   * @return the formatted string, not null
   */
  String format(long epochSeconds);

  /**
   * Formats the specified number of seconds since epoch using the specified pattern.
   * <p>
   * This formats the unixtime to a String using specified pattern and the rules of the underlying formatter.
   * </p>
   * @param epochSeconds the number of seconds to format
   * @param pattern the pattern to use for formatting
   * @return the formatted string, not null
   */
  String format(long epochSeconds, String pattern);
}
