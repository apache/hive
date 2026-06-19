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

import java.time.ZoneId;
import java.util.Objects;
import java.util.function.Function;

/**
 * Formatter that supports caching of patterns to avoid compilation overhead.
 * <p>
 * At its current state, the cache is very simplistic and just holds the last used pattern in memory.
 * </p>
 * @param <T> the type of the underlying datetime formatter
 */
abstract class InstantFormatterCache<T> implements InstantFormatter {

  protected final ZoneId zoneId;
  protected final Function<String, T> loader;
  protected String lastPattern;
  protected T formatter;

  protected InstantFormatterCache(ZoneId zoneId, Function<String, T> loader) {
    this.zoneId = zoneId;
    this.loader = loader;
  }

  protected final T getFormatter(String pattern) {
    Objects.requireNonNull(pattern);
    if (!pattern.equals(lastPattern)) {
      lastPattern = pattern;
      formatter = loader.apply(pattern);
    }
    return formatter;
  }
}
