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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.common;

import org.apache.hadoop.conf.Configuration;

public class JavaVersionUtils {
  private JavaVersionUtils() {
    throw new IllegalStateException("Utility class");
  }

  /**
   * Returns JVM --add-opens flags for Tez/MR child processes.
   */
  public static String getAddOpensFlags() {
    return " --add-opens=java.base/java.net=ALL-UNNAMED" +
        " --add-opens=java.base/java.util=ALL-UNNAMED" +
        " --add-opens=java.base/java.util.concurrent=ALL-UNNAMED" +
        " --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED" +
        " --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED" +
        " --add-opens=java.base/java.lang=ALL-UNNAMED" +
        " --add-opens=java.base/java.io=ALL-UNNAMED" +
        " --add-opens=java.base/java.lang.invoke=ALL-UNNAMED" +
        " --add-opens=java.base/java.lang.reflect=ALL-UNNAMED" +
        " --add-opens=java.base/java.math=ALL-UNNAMED" +
        " --add-opens=java.base/java.nio=ALL-UNNAMED" +
        " --add-opens=java.base/java.text=ALL-UNNAMED" +
        " --add-opens=java.base/java.time=ALL-UNNAMED" +
        " --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED" +
        " --add-opens=java.base/jdk.internal.reflect=ALL-UNNAMED" +
        " --add-opens=java.sql/java.sql=ALL-UNNAMED" +
        " --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" +
        " --add-opens=java.base/sun.nio.cs=ALL-UNNAMED" +
        " --add-opens=java.base/java.util.regex=ALL-UNNAMED" +
        " --add-opens=java.base/java.security=ALL-UNNAMED" +
        " --add-opens=java.base/sun.security.provider=ALL-UNNAMED";
  }

  /**
   * Appends the --add-opens flags to the AM, map and reduce JVM options of an MR job
   * that is about to be submitted, keeping whatever is already configured. Every code
   * path that submits an MR job on Hive's behalf has to call this before creating the
   * JobClient: ExecDriver, MergeFileTask, ColumnTruncateTask and MRCompactor.
   */
  public static void addOpensFlags(Configuration job) {
    String addOpens = getAddOpensFlags();
    for (String key : new String[] {"mapreduce.map.java.opts",
        "mapreduce.reduce.java.opts", "yarn.app.mapreduce.am.command-opts"}) {
      String current = job.get(key);
      job.set(key, (current == null ? "" : current) + addOpens);
    }
  }
}
