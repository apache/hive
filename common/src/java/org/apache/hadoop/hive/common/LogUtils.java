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

package org.apache.hadoop.hive.common;

import java.net.URL;

import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;

/**
 * Utilities common to logging operations.
 */
public class LogUtils {

  public static final String HIVE_L4J = "hive-log4j.properties";
  public static final String HIVE_EXEC_L4J = "hive-exec-log4j.properties";

  @SuppressWarnings("serial")
  public static class LogInitializationException extends Exception {
    public LogInitializationException(String msg) {
      super(msg);
    }
  }

  /**
   * Initialize log4j based on hive-log4j.properties.
   *
   * @return an message suitable for display to the user
   * @throws LogInitializationException if log4j fails to initialize correctly
   */
  public static String initHiveLog4j() throws LogInitializationException {
    // allow hive log4j to override any normal initialized one
    URL hive_l4j = LogUtils.class.getClassLoader().getResource(HIVE_L4J);
    if (hive_l4j != null) {
      LogManager.resetConfiguration();
      PropertyConfigurator.configure(hive_l4j);
      return "Logging initialized using configuration in " + hive_l4j;
    } else {
      throw new LogInitializationException("Unable to initialize logging using "
          + LogUtils.HIVE_L4J + ", not found on CLASSPATH!");
    }
  }
}
