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

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.ResolverStyle;

/**
 * Formatter for parsing and printing {@link Instant} objects.
 * <p>
 * This interface provides the main entry point for print and parsing and provides factories for the 
 * available implementations of {@code InstantFormatter}.
 * </p>
 * <p>
 * The patterns that are supported and their behavior depend on the underlying implementation of the interface.
 * </p>
 * <p>
 * Implementations of the interface are not meant to be thread safe.
 * </p>
 */
public interface InstantFormatter {

  /**
   * Types for the built-in formatter implementations.
   */
  enum Type {
    /**
     * A formatter that supports the same patterns with {@link java.text.SimpleDateFormat}.
     */
    SIMPLE {
      @Override
      InstantFormatter newFormatter(ZoneId zone, ResolverStyle resolverStyle) {
        return new InstantSimpleDateFormatter(zone);
      }
    },
    /**
     * A formatter that supports the same patterns with {@link java.time.format.DateTimeFormatter}.
     */
    DATETIME {
      @Override
      InstantFormatter newFormatter(ZoneId zone, ResolverStyle resolverStyle) {
        return new InstantDateTimeFormatter(zone, resolverStyle);
      }
    };
    /**
     * Creates a new formatter with the specified zone id.
     * @param zone - the zone id
     * @param resolverStyle - The style is used to control how the input is resolved.
     * @return a new formatter with the specified zone id.
     */
    abstract InstantFormatter newFormatter(ZoneId zone, ResolverStyle resolverStyle);
  }

  /**
   * Creates a formatter using the specified configuration.
   * 
   * @param conf the configuration to use, not null
   * @return the formatter based on the provided configuration, not null.
   */
  static InstantFormatter ofConfiguration(Configuration conf) {
    ZoneId zoneId = TimestampTZUtil.parseTimeZone(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_LOCAL_TIME_ZONE));
    Type type = Type.valueOf(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_DATETIME_FORMATTER).toUpperCase());
    ResolverStyle resolverStyle = ResolverStyle.valueOf(HiveConf.getVar(conf,
        HiveConf.ConfVars.HIVE_DATETIME_RESOLVER_STYLE).toUpperCase());
    return type.newFormatter(zoneId, resolverStyle);
  }

  /**
   * Parses the input text and converts it to an instant.
   * @param text the text to parse, not null
   * @return an Instant representing a specific point in time.
   * @throws RuntimeException if unable to parse the requested text using the default behavior.
   */
  Instant parse(String text) throws RuntimeException;

  /**
   * Parses the input text and converts it to an instant using the specified pattern.
   * @param text the text to parse, not null
   * @param pattern the pattern to use to parse the text and resolve it to an instant
   * @return an Instant representing a specific point in time.
   * @throws RuntimeException if unable to parse the requested text using the specified pattern.
   */
  Instant parse(String text, String pattern) throws RuntimeException;

  /**
   * Formats the specified instant using the formatters default pattern.
   * <p>
   * This formats the instant to a String using the rules of the underlying formatter.
   * </p>
   * @param instant the instant to format
   * @return the formatted string, not null
   */
  String format(Instant instant);

  /**
   * Formats the specified instant using the specified pattern.
   * <p>
   * This formats the instant to a String using specified pattern and the rules of the underlying formatter.
   * </p>
   * @param instant the instant to format
   * @param pattern the pattern to use for formatting
   * @return the formatted string, not null
   */
  String format(Instant instant, String pattern);
}
