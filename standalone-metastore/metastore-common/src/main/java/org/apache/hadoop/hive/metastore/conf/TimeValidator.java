/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.conf;

import java.util.concurrent.TimeUnit;

public class TimeValidator implements Validator {

  private final TimeUnit unit;
  private final Long min;
  private final boolean minInclusive;

  private final Long max;
  private final boolean maxInclusive;

  public TimeValidator(TimeUnit unit) {
    this(unit, null, false, null, false);
  }

  public TimeValidator(TimeUnit unit, Long min, boolean minInclusive, Long max,
                       boolean maxInclusive) {
    this.unit = unit;
    this.min = min;
    this.minInclusive = minInclusive;
    this.max = max;
    this.maxInclusive = maxInclusive;
  }

  @Override
  public void validate(String value) {
    // First just check that this translates
    TimeUnit defaultUnit = unit;
    long time = MetastoreConf.convertTimeStr(value, defaultUnit, defaultUnit);
    if (min != null) {
      if (minInclusive ? time < min : time <= min) {
        throw new IllegalArgumentException(value + " is smaller than minimum " + min +
            MetastoreConf.timeAbbreviationFor(defaultUnit));
      }
    }

    if (max != null) {
      if (maxInclusive ? time > max : time >= max) {
        throw new IllegalArgumentException(value + " is larger than maximum " + max +
            MetastoreConf.timeAbbreviationFor(defaultUnit));
      }
    }
  }

  private String timeString(long time, TimeUnit timeUnit) {
    return time + " " + MetastoreConf.timeAbbreviationFor(timeUnit);
  }
}
