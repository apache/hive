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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.time.ZoneId;
import java.util.concurrent.ExecutionException;

public abstract class UnixTimeFormatter<T> {

  enum Type {
    SIMPLE {
      @Override
      UnixTimeFormatter<?> newFormatter(ZoneId zone) {
        return new UnixTimeSimpleDateFormatter(zone);
      }
    }, DATETIME {
      @Override
      UnixTimeFormatter<?> newFormatter(final ZoneId zone) {
        return new UnixTimeDateTimeFormatter(zone);
      }
    };

    abstract UnixTimeFormatter<?> newFormatter(ZoneId zone);
  }

  public static UnixTimeFormatter<?> from(Configuration conf) {
    ZoneId zoneId = TimestampTZUtil.parseTimeZone(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_LOCAL_TIME_ZONE));
    Type type = Type.valueOf(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_DATETIME_FORMATTER).toUpperCase());
    return type.newFormatter(zoneId);
  }

  protected final ZoneId zoneId;
  private final LoadingCache<String, T> formatters;

  protected UnixTimeFormatter(ZoneId zoneId, CacheLoader<String, T> loader) {
    this.zoneId = zoneId;
    formatters = CacheBuilder.newBuilder().maximumSize(1).concurrencyLevel(1).build(loader);
  }

  public abstract long parse(String value) throws RuntimeException;

  public abstract long parse(String value, String pattern) throws RuntimeException;

  public abstract String format(long epochSeconds);

  public abstract String format(long epochSeconds, String pattern);

  protected final T getFormatter(String pattern) {
    try {
      return this.formatters.get(pattern);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
