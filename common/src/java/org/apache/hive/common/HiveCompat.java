/**
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

package org.apache.hive.common;

import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveCompat {

  private static Logger LOG = LoggerFactory.getLogger(HiveCompat.class);

  /**
   * Enum to represent a level of backward compatibility support.
   *
   */
  public enum CompatLevel {
    HIVE_0_12("0.12", 0, 12),
    HIVE_0_13("0.13", 0, 13);

    public final String value;
    public final int majorVersion;
    public final int minorVersion;

    CompatLevel(String val, int majorVersion, int minorVersion) {
      this.value = val;
      this.majorVersion = majorVersion;
      this.minorVersion = minorVersion;
    }
  }

  public static final String DEFAULT_COMPAT_LEVEL = CompatLevel.HIVE_0_12.value;
  public static final String LATEST_COMPAT_LEVEL = getLastCompatLevel().value;

  /**
   * Returned the configured compatibility level
   * @param hconf Hive configuration
   * @return
   */
  public static CompatLevel getCompatLevel(HiveConf hconf) {
    return getCompatLevel(HiveConf.getVar(hconf, HiveConf.ConfVars.HIVE_COMPAT));
  }

  public static CompatLevel getCompatLevel(String compatStr) {
    if (compatStr.equalsIgnoreCase("latest")) {
      compatStr = LATEST_COMPAT_LEVEL;
    }

    for (CompatLevel cl : CompatLevel.values()) {
      if (cl.value.equals(compatStr)) {
        return cl;
      }
    }

    LOG.error("Could not find CompatLevel for " + compatStr
        + ", using default of " + DEFAULT_COMPAT_LEVEL);
    return getCompatLevel(DEFAULT_COMPAT_LEVEL);
  }

  private static CompatLevel getLastCompatLevel() {
    CompatLevel[] compatLevels = CompatLevel.values();
    return compatLevels[compatLevels.length - 1];
  }
}
