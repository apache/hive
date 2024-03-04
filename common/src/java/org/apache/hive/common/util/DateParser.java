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
package org.apache.hive.common.util;

import java.util.Objects;

import org.apache.hadoop.hive.common.type.Date;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * Date parser class for Hive.
 */
public final class DateParser {

  /**
   * Cache 10 years worth of dates before evicting. Note, it is possible to cast
   * timestamps (Date and Time) into Dates, therefore the effectiveness of the
   * cache may be limited if the Time portion of the timestamp has many distinct
   * values.
   */
  private static final LoadingCache<String, Date> DATE_CACHE =
      CacheBuilder.newBuilder().maximumSize(10 * 365).build(new CacheLoader<String, Date>() {
        @Override
        public Date load(final String text) throws Exception {
          return Date.valueOf(text);
        }
      });

  private DateParser() {
  }

  /**
   * Obtains an instance of Date from a text string such as 2021-02-21.
   *
   * @param text the text to parse
   * @return The Date objects generated from parsing the text or null if the
   *         text could not be parsed
   * @throws NullPointerException if {@code text} is null
   */
  public static Date parseDate(final String text) {
    Objects.requireNonNull(text);

    // Date is a mutable class; do not return cached value
    Date result = new Date();
    return (parseDate(text, result)) ? result : null;
  }

  /**
   * Obtains an instance of Date from a text string such as 2021-02-21. The
   * {@code result} is not modified if parsing of the {@code text} fails.
   *
   * @param text the text to parse
   * @param result the {@code Date} to load the results into
   * @return True if parsing was successful; false otherwise.
   * @throws NullPointerException if {@code text} or {@code result} is null
   */
  public static boolean parseDate(final String text, final Date result) {
    Objects.requireNonNull(text);
    Objects.requireNonNull(result);

    try {
      Date date = DATE_CACHE.get(text);
      result.setTimeInMillis(date.toEpochMilli());
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
