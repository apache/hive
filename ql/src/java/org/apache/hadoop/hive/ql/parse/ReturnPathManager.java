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

package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.hive.conf.HiveConf;

/**
 * This is a temporary class for the time while we are in transition to use return path only.
 * It's purpose is to decide if Hive should use return path.
 */
public final class ReturnPathManager {
  private ReturnPathManager() {
    throw new UnsupportedOperationException("ReturnPathManager should not be instantiated");
  }

  private static ThreadLocal<Boolean> useReturnPath = new ThreadLocal<>();

  public static void init(HiveConf conf, ASTNode root) {
    boolean use = shouldUseReturnPath(conf, root);
    useReturnPath.set(use);
  }

  public static boolean shouldUse() {
    return useReturnPath.get();
  }

  private static boolean shouldUseReturnPath(HiveConf conf, ASTNode root) {
    String confVal = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_CBO_RETPATH_HIVEOP);
    if ("true".equals(confVal)) {
      return true;
    } else if ("false".equals(confVal)) {
      return false;
    } else if ("supported".contentEquals(confVal)) {
      return isReturnPathSupported(root);
    } else {
      throw new IllegalStateException(String.format("Invalid value for %s: %s",
          HiveConf.ConfVars.HIVE_CBO_RETPATH_HIVEOP.varname, confVal));
    }
  }

  private static boolean isReturnPathSupported(ASTNode ast) {
    // Here is where we may add the logic in the future for supported operations
    return false;
  }
}
