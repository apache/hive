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

public class JavaVersionUtils {
  private JavaVersionUtils() {
    throw new IllegalStateException("Utility class");
  }

  /**
   * Returns the detected Java major version (e.g., 8, 11, 17).
   */
  public static int getJavaMajorVersion() {
    return Math.max(8, Integer.parseInt(
        System.getProperty("java.specification.version").split("\\.")[0]));
  }

  /**
   * Returns the full --add-opens string if Java 9, or an empty string if Java 8 or below.
   */
  public static String getAddOpensFlagsIfNeeded() {
    //TODO: Please remove this once the codebase has been migrated to JDK 9 or a higher version.
    int javaVersion = getJavaMajorVersion();
    if (javaVersion >= 9) {
      return String.join(" ",
          "-XX:IgnoreUnrecognizedVMOptions",
          "--add-opens=java.base/java.net=ALL-UNNAMED",
          "--add-opens=java.base/java.util=ALL-UNNAMED",
          "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
          "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
          "--add-opens=java.base/java.lang=ALL-UNNAMED",
          "--add-opens=java.base/java.io=ALL-UNNAMED",
          "--add-opens java.base/java.lang=ALL-UNNAMED",
          "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
          "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
          "--add-opens=java.base/java.math=ALL-UNNAMED",
          "--add-opens=java.base/java.nio=ALL-UNNAMED",
          "--add-opens=java.base/java.text=ALL-UNNAMED",
          "--add-opens=java.base/java.time=ALL-UNNAMED",
          "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
          "--add-opens=java.base/jdk.internal.reflect=ALL-UNNAMED",
          "--add-opens=java.sql/java.sql=ALL-UNNAMED",
          "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
          "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
          "--add-opens=java.base/java.util.regex=ALL-UNNAMED",
          "--add-opens=java.base/java.security=ALL-UNNAMED",
          "--add-opens=java.base/sun.security.provider=ALL-UNNAMED"
      );
    } else {
      return ""; // Java 8 or lower does not need add-opens
    }
  }
}
