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

package org.apache.hadoop.hive.metastore;


public class ProtectMode {
  public static String PARAMETER_NAME = "PROTECT_MODE";

  public static String FLAG_OFFLINE = "OFFLINE";
  public static String FLAG_NO_DROP = "NO_DROP";
  public static String FLAG_READ_ONLY = "READ_ONLY";

  public boolean offline = false;
  public boolean readOnly = false;
  public boolean noDrop = false;

  static public ProtectMode getProtectModeFromString(String sourceString) {
    return new ProtectMode(sourceString);
  }

  private ProtectMode(String sourceString) {
    String[] tokens = sourceString.split(",");
    for (String token: tokens) {
      if (token.equalsIgnoreCase(FLAG_OFFLINE)) {
        offline = true;
      } else if (token.equalsIgnoreCase(FLAG_NO_DROP)) {
        noDrop = true;
      } else if (token.equalsIgnoreCase(FLAG_READ_ONLY)) {
        readOnly = true;
      }
    }
  }

  public ProtectMode() {
  }

  @Override
  public String toString() {
    String retString = null;

    if (offline) {
        retString = FLAG_OFFLINE;
    }

    if (noDrop) {
      if (retString != null) {
        retString = retString + "," + FLAG_NO_DROP;
      }
      else
      {
        retString = FLAG_NO_DROP;
      }
    }

    if (readOnly) {
      if (retString != null) {
        retString = retString + "," + FLAG_READ_ONLY;
      }
      else
      {
        retString = FLAG_READ_ONLY;
      }
    }

    return retString;
  }
}
